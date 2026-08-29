-- Rollback-contained real first use of admission input; no business data survives.
\set ON_ERROR_STOP on
begin;
-- Fixture renderer receipts stand in for object storage only; these helpers
-- call real production transitions and do not replace any financial/database owner.
create function pg_temp.candidate_register_all_review_components(
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_contract jsonb;
  v_receipt jsonb;
  v_response jsonb;
begin
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    v_contract:=private._candidate_component_render_contract_v1(
      p_workflow_id,p_generation,v_component.id,'REVIEW');
    v_receipt:=jsonb_build_object(
      'form_variant',v_contract->>'form_variant',
      'workflow_id',p_workflow_id,
      'workflow_generation',p_generation,
      'component_id',v_component.id,
      'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,
      'review_ordinal',v_component.review_ordinal,
      'scope',v_contract->>'scope',
      'page_count',1,
      'render_input_sha256',v_contract->>'render_input_sha256',
      'candidate_signature_embedded',(v_contract->>'candidate_signature_embedded')::boolean,
      'manager_signature_embedded',false,
      'manager_approval_date_embedded',false
    );
    v_response:=public.candidate_workflow_transition_atomic_v1(
      null,'TEST',p_workflow_id,'REGISTER_REVIEW_COMPONENT',p_generation,
      jsonb_build_object(
        'component_id',v_component.id,
        'storage_key',p_key_prefix||'/review/'||v_component.id::text||'.pdf',
        'content_sha256_hex',encode(extensions.digest(
          convert_to(p_key_prefix||':review:'||v_component.id::text,'UTF8'),'sha256'),'hex'),
        'render_input_sha256_hex',v_contract->>'render_input_sha256',
        'media_type','application/pdf','byte_size',2048,'page_count',1,
        'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
        'renderer_receipt',v_receipt
      ),p_key_prefix||':register-review:'||v_component.id::text,p_now_utc
    );
    if coalesce((v_response->>'review_document_ready')::boolean,false)=false then
      raise exception 'review component did not become ready: %',v_response;
    end if;
  end loop;
  if (select state from public.candidate_submission_workflows where id=p_workflow_id)
     <>'READY_FOR_MANAGER_APPROVAL' then
    raise exception 'workflow did not become review-ready';
  end if;
end;
$function$;

create function pg_temp.candidate_phone_approve_all(
  p_session_id uuid,
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_signature_hash_seed text,
  p_now_utc timestamptz
)
returns jsonb
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_response jsonb;
  v_manifest_hash text;
  v_approval_request_id uuid;
  v_signature_component_id uuid;
begin
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'SELECT_PHONE_APPROVAL',p_generation,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(p_workflow_id::text||':'||p_key_prefix||':phone','sha256'),'hex'),
      'expires_at_utc',p_now_utc+interval '30 minutes','handoff_token_key_version',1,
      'public_broker_binding',jsonb_build_object(
        'contract_version','CANDIDATE_PUBLIC_PHONE_BINDING_V1',
        'public_session_binding_sha256',repeat('ab',32),
        'device_binding_sha256',repeat('cd',32)
      ),'broker_handoff_key_version',1
    ),p_key_prefix||':phone-select',p_now_utc);
  v_manifest_hash:=v_response->>'review_manifest_sha256';
  v_approval_request_id:=(v_response->>'approval_request_id')::uuid;
  if v_response->>'state'<>'AWAITING_MANAGER_APPROVAL' or v_manifest_hash is null then
    raise exception 'phone approval request failed: %',v_response;
  end if;
  -- An interrupted Daily PHONE review must return to approval choices without
  -- changing to a Weekly-only route, losing documents or creating a Timesheet.
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'CANCEL_MANAGER_HANDOFF',p_generation,
    '{}'::jsonb,p_key_prefix||':cancel-interrupted-phone',p_now_utc);
  if v_response->>'state'<>'READY_FOR_MANAGER_APPROVAL'
    or v_response->>'handoff_cancelled'<>'true'
    or (select route from public.candidate_submission_workflows where id=p_workflow_id)<>'PHONE'
    or (select state from public.candidate_approval_requests where id=v_approval_request_id)<>'CANCELLED'
    or (select encode(review_manifest_sha256,'hex') from public.candidate_submission_workflows where id=p_workflow_id)<>v_manifest_hash then
    raise exception 'Daily interrupted review failed to preserve its route/documents';
  end if;
  if (public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'CANCEL_MANAGER_HANDOFF',p_generation,
    '{}'::jsonb,p_key_prefix||':cancel-interrupted-phone',p_now_utc)-'idempotent_replay') is distinct from v_response then
    raise exception 'Daily interrupted review cancellation replay changed';
  end if;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'SELECT_PHONE_APPROVAL',p_generation,
    jsonb_build_object(
      'approval_token_hash_hex',encode(extensions.digest(p_workflow_id::text||':'||p_key_prefix||':phone-retry','sha256'),'hex'),
      'expires_at_utc',p_now_utc+interval '30 minutes','handoff_token_key_version',1,
      'public_broker_binding',jsonb_build_object(
        'contract_version','CANDIDATE_PUBLIC_PHONE_BINDING_V1',
        'public_session_binding_sha256',repeat('ab',32),
        'device_binding_sha256',repeat('cd',32)
      ),'broker_handoff_key_version',1
    ),p_key_prefix||':phone-select-retry',p_now_utc);
  if v_response->>'state'<>'AWAITING_MANAGER_APPROVAL'
    or v_response->>'review_manifest_sha256'<>v_manifest_hash
    or (v_response->>'approval_request_id')::uuid=v_approval_request_id then
    raise exception 'Daily interrupted review did not receive a fresh manager request';
  end if;
  v_approval_request_id:=(v_response->>'approval_request_id')::uuid;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'BEGIN_MANAGER_REVIEW',p_generation,
    '{}'::jsonb,p_key_prefix||':begin-review',p_now_utc);
  if v_response->>'manifest_sha256'<>v_manifest_hash then
    raise exception 'manager manifest changed: %',v_response;
  end if;
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    perform public.candidate_workflow_transition_atomic_v1(
      p_session_id,'TEST',p_workflow_id,'RECORD_REVIEW_PROGRESS',p_generation,
      jsonb_build_object(
        'manifest_sha256_hex',v_manifest_hash,
        'component_id',v_component.id,
        'component_sha256_hex',encode(v_component.review_content_sha256,'hex'),
        'viewed_receipt',jsonb_build_object('viewed',true,'component_id',v_component.id)
      ),p_key_prefix||':reviewed:'||v_component.id::text,p_now_utc);
  end loop;
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'COMPONENT_PREPARE',p_generation,
    jsonb_build_object(
      'component_kind','MANAGER_SIGNATURE','document_role','MANAGER_SIGNATURE',
      'approval_request_id',v_approval_request_id,
      'storage_key',p_key_prefix||'/manager-signature.png',
      'media_type','image/png','byte_size',256
    ),p_key_prefix||':manager-signature-prepare',p_now_utc);
  v_signature_component_id:=(v_response->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'COMPONENT_COMPLETE',p_generation,
    jsonb_build_object(
      'component_id',v_signature_component_id,
      'source_content_sha256_hex',encode(extensions.digest(
        convert_to(p_signature_hash_seed,'UTF8'),'sha256'),'hex'),
      'verified_byte_size',256,'verified_media_type','image/png'
    ),p_key_prefix||':manager-signature-complete',p_now_utc);
  v_response:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,'TEST',p_workflow_id,'PHONE_APPROVE',p_generation,
    jsonb_build_object(
      'manifest_sha256_hex',v_manifest_hash,
      'signature_component_id',v_signature_component_id,
      'manager_name','Runtime Manager','manager_position','Service Manager'
    ),p_key_prefix||':phone-approve',p_now_utc);
  if v_response->>'state'<>'MANAGER_APPROVED_PENDING_FINAL_DOCUMENT' then
    raise exception 'manager approval failed: %',v_response;
  end if;
  return v_response;
end;
$function$;

create function pg_temp.candidate_register_all_final_components(
  p_workflow_id uuid,
  p_generation integer,
  p_key_prefix text,
  p_now_utc timestamptz
)
returns void
language plpgsql
as $function$
declare
  v_component public.candidate_submission_components%rowtype;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_contract jsonb;
  v_receipt jsonb;
  v_response jsonb;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id;
  for v_component in
    select * from public.candidate_submission_components
    where workflow_id=p_workflow_id and workflow_generation=p_generation
      and required=true and state<>'SUPERSEDED'
    order by review_ordinal,id
  loop
    v_contract:=private._candidate_component_render_contract_v1(
      p_workflow_id,p_generation,v_component.id,'FINAL');
    v_receipt:=jsonb_strip_nulls(jsonb_build_object(
      'form_variant',v_contract->>'form_variant',
      'workflow_id',p_workflow_id,
      'workflow_generation',p_generation,
      'component_id',v_component.id,
      'component_kind',v_component.component_kind,
      'document_role',v_component.document_role,
      'review_ordinal',v_component.review_ordinal,
      'scope',v_contract->>'scope','page_count',1,
      'render_input_sha256',v_contract->>'render_input_sha256',
      'candidate_signature_embedded',(v_contract->>'candidate_signature_embedded')::boolean,
      'manager_signature_embedded',true,'manager_approval_date_embedded',true,
      'candidate_signature_sha256',case when v_component.component_kind='HOURS_TIMESHEET'
        then encode(v_workflow.candidate_signature_sha256,'hex') end,
      'manager_signature_sha256',encode(v_workflow.manager_signature_sha256,'hex'),
      'manager_name',v_workflow.manager_name,'manager_position',v_workflow.manager_position,
      'manager_approved_at_utc',v_workflow.manager_approved_at_utc
    ));
    v_response:=public.candidate_workflow_transition_atomic_v1(
      null,'TEST',p_workflow_id,'REGISTER_FINAL_SIGNED_DOCUMENT',p_generation,
      jsonb_build_object(
        'component_id',v_component.id,
        'storage_key',p_key_prefix||'/final/'||v_component.id::text||'.pdf',
        'content_sha256_hex',encode(extensions.digest(
          convert_to(p_key_prefix||':final:'||v_component.id::text,'UTF8'),'sha256'),'hex'),
        'render_input_sha256_hex',v_contract->>'render_input_sha256',
        'media_type','application/pdf','byte_size',2304,'page_count',1,
        'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
        'renderer_receipt',v_receipt
      ),p_key_prefix||':register-final:'||v_component.id::text,p_now_utc);
  end loop;
  if (select state from public.candidate_submission_workflows where id=p_workflow_id)
     <>'READY_TO_FINALISE' then
    raise exception 'workflow did not become finalisation-ready';
  end if;
end;
$function$;

do $proof$
declare
  v_candidate uuid:=gen_random_uuid();
  v_other uuid:=gen_random_uuid();
  v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_now timestamptz:=clock_timestamp();
  v_generation uuid;
  v_days jsonb;
  v_result jsonb;
  v_source jsonb;
  v_bad jsonb;
  v_case integer;
  v_workflow uuid:=gen_random_uuid();
  v_receipt uuid;
  v_replayed uuid;
  v_account uuid:=gen_random_uuid();
  v_session uuid:=gen_random_uuid();
  v_created_workflow uuid:=gen_random_uuid();
  v_create_payload jsonb;
  v_create_response jsonb;
  v_create_key text:=gen_random_uuid()::text;
  v_before bigint;
  v_page_one jsonb;
  v_page_two jsonb;
  v_resubmit_workflow uuid:=gen_random_uuid();
  v_signature_id uuid;
  v_submit jsonb;
  v_begin_receipt jsonb;
  v_materialisation jsonb;
  v_final_key text:=gen_random_uuid()::text;
  v_current_id uuid;
  v_expected_count bigint;
  v_resolution_client uuid:=gen_random_uuid();
  v_resolution_context jsonb;
  v_resolution_snapshot jsonb;
  v_resolution_outbox uuid;
  v_documents_before jsonb;
  v_documents_after jsonb;
  v_office_actor uuid;
  v_office_preview jsonb;
  v_office_payload jsonb;
  v_office_replacement uuid;
  v_delete_signature text;
  v_delete_preview jsonb;
  v_delete_result jsonb;
  v_candidate_context jsonb;
  v_context jsonb:=jsonb_build_object('policy','SIGNED_SYSTEM_SYNC','environment','TEST',
    'system_auth_verified',true,'nonce_consumed',true,'environment_trusted',true,
    'stable_operation_identity',true,'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true);
begin
  select count(*) into v_before from public.timesheets;
  insert into public.candidates(id,email,display_name,active,key_norm)
    values(v_candidate,'daily-booked-proof@example.invalid','Daily booked proof',true,
      'cid1-962abcdefghjkmnpqrs');
  select jsonb_agg(jsonb_build_object('date',(v_today-1+n)::text,
    'booked',n<2,'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0'))
    ||case when n<2 then jsonb_build_object(
      'booking_id','daily-booked-proof-'||n::text,
      'shift_starts_at',((v_today-1+n)::text||case when n=0 then ' 21:00:00 Europe/London'
        else ' 00:00:00 Europe/London' end)::timestamptz,
      'shift_ends_at',((v_today+n)::text||' 07:00:00 Europe/London')::timestamptz,
      'hospital','Unresolved hospital','ward','','job_title','Unresolved role',
      'shift_type','NIGHT','timesheet_eligible',false)
      else '{}'::jsonb end order by n) into v_days from generate_series(0,13)n;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_context,
    gen_random_uuid(),'daily-booked-proof-'||v_candidate::text,jsonb_build_array(
      jsonb_build_object('candidate_global_key','CID1-962ABCDEFGHJKMNPQRS',
        'candidate_source_hmac',repeat('e',64),'source_hmac_key_version',1,
        'source_event_id','daily-booked-proof-'||v_candidate::text,
        'source_revision','daily-booked-proof-v1','source_hash',repeat('c',64),
        'window_start',(v_today-1)::text,'days',v_days,'source_event_time',v_now::text,
        'item_key','daily-booked-proof-'||v_candidate::text)),
      '01K2ABCDEFGHJKMNPQRSTVWXYZ');
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: fixture publication failed: %',v_result;
  end if;
  v_generation:=(v_result#>>'{outcomes,0,generation_id}')::uuid;
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST',v_candidate,true,'Daily booked input proof',repeat('d',64))
    on conflict(environment,candidate_id) do update set enabled=true;
  update public.settings_defaults set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||jsonb_build_object('candidate_daily_enabled',true) where id=1;
  insert into private.candidate_daily_sync_state(environment,candidate_id,target,state)
    values('TEST',v_candidate,'MASTER_AVAILABILITY_SHEET','READY')
    on conflict(environment,candidate_id,target) do nothing;
  update private.candidate_daily_authority_scopes set authority_mode='SUPABASE_PRIMARY'
    where environment='TEST' and candidate_id=v_candidate;
  v_candidate_context:=jsonb_build_object('policy','CANDIDATE_SURFACE','environment','TEST','candidate_id',v_candidate);
  v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,v_today,14);
  if v_result#>>'{tiles,0,action_target,target_kind}' is distinct from 'BOOKED_DAILY_SHIFT'
     or v_result#>>'{tiles,1,action_target,target_kind}' is distinct from 'BOOKED_DAILY_SHIFT'
     or v_result#>>'{tiles,0,action_target,source,work_date}' is distinct from (v_today-1)::text
     or v_result#>>'{tiles,1,action_target,source,booking_id}' is distinct from 'daily-booked-proof-1'
     or v_result#>'{tiles,2,action_target}' is not null then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: read-only booked action target is wrong';
  end if;

  -- MyTMS ignores the legacy time-window flag but still denies future shifts.
  -- The previous date is over four hours past its finish even just after midnight.
  begin
    update public.candidate_daily_rota_days set timesheet_eligible=false,
      shift_starts_at=((v_today-1)::text||' 07:00 Europe/London')::timestamptz,
      shift_ends_at=((v_today-1)::text||' 18:00 Europe/London')::timestamptz
      where generation_id=v_generation and rota_date=v_today-1;
    v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,v_today,14);
    if v_result#>>'{tiles,0,action_target,target_kind}' is distinct from 'BOOKED_DAILY_SHIFT'
       or v_result#>>'{tiles,0,timesheet_eligible}' is distinct from 'true' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: legacy four-hour flag hid a current started shift';
    end if;
    v_bad:=jsonb_build_object('generation_id',v_generation,'work_date',v_today-1,
      'source_row_hash',lpad(to_hex(1),64,'0'),'booking_id','daily-booked-proof-0');
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_bad,v_now);
    v_result:=public.candidate_daily_tiles_get_v1(
      v_context||jsonb_build_object('policy','LEGACY_COMPAT','candidate_source_hmac',repeat('e',64)),
      v_today-1,14);
    if v_result#>>'{tiles,0,timesheet_eligible}' is distinct from 'false' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: MyTMS entry changed legacy eligibility';
    end if;
    update public.candidate_daily_rota_days set
      shift_starts_at=v_now+interval '1 hour',shift_ends_at=v_now+interval '9 hours'
      where generation_id=v_generation and rota_date=v_today;
    v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,v_today,14);
    if v_result#>'{tiles,1,action_target}' is not null
       or v_result#>>'{tiles,1,timesheet_eligible}' is distinct from 'false' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: future shift exposed first submission';
    end if;
    v_bad:=jsonb_build_object('generation_id',v_generation,'work_date',v_today,
      'source_row_hash',lpad(to_hex(2),64,'0'),'booking_id','daily-booked-proof-1');
    begin
      perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_bad,v_now);
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: future shift admitted';
    exception when sqlstate '55000' then null;
    end;
    raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS started shift beyond four hours; legacy unchanged; future denied';
    raise exception using errcode='Z9624',message='rollback entry-window fixture';
  exception when sqlstate 'Z9624' then null;
  end;

  for v_case in 0..1 loop
    v_source:=jsonb_build_object('generation_id',v_generation,'work_date',v_today-1+v_case,
      'source_row_hash',lpad(to_hex(v_case+1),64,'0'),'booking_id','daily-booked-proof-'||v_case::text);
    v_result:=private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    if v_result->>'work_date' is distinct from (v_today-1+v_case)::text
       or (v_result->>'week_ending_date')::date is distinct from
         (v_today-1+v_case)+(7-extract(isodow from (v_today-1+v_case))::integer)
       or v_result->>'hospital'<>'Unresolved hospital' or v_result->>'ward'<>''
       or v_result ?| array['contract_id','contract_week_id','client_id','rate','pay','charge'] then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: wrong date/context or invented financial mapping';
    end if;
  end loop;
  -- These are identity checks, not a way to create records while browsing.
  if (select count(*) from public.timesheets)<>v_before
     or exists(select 1 from public.timesheets_financials where candidate_id=v_candidate)
     or exists(select 1 from public.contracts where candidate_id=v_candidate) then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: reading source wrote work or financial data';
  end if;

  begin
    perform private._candidate_daily_first_receipt_v1('TEST',v_candidate,v_source,v_workflow,false,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: first row created without submit intent';
  exception when sqlstate '22023' then null;
  end;
  v_receipt:=private._candidate_daily_first_receipt_v1('TEST',v_candidate,v_source,v_workflow,true,v_now);
  v_replayed:=private._candidate_daily_first_receipt_v1('TEST',v_candidate,v_source,v_workflow,true,v_now);
  if v_replayed is distinct from v_receipt
     or (select count(*) from public.timesheets where booking_id=v_source->>'booking_id')<>1
     or not exists(select 1 from public.timesheets where timesheet_id=v_receipt
       and is_current and version=1 and contract_id is null and sheet_scope='DAILY'
       and submission_mode='MANUAL' and candidate_submission_route_intent='ELECTRONIC'
       and authorised_at_server is null and worked_start_iso is null)
     or exists(select 1 from public.contract_weeks where timesheet_id=v_receipt)
     or exists(select 1 from public.timesheets_financials where timesheet_id=v_receipt) then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: first receipt duplicated or manufactured financial state';
  end if;
  v_result:=private._candidate_daily_receipt_context_v1('TEST',v_candidate,v_receipt,true,v_now);
  if v_result->>'client_id' is not null
     or v_result#>>'{policy,allow_daily_manager_authorise_on_phone}'<>'true'
     or v_result#>>'{policy,allow_daily_manager_authorise_by_email}'<>'false'
     or v_result#>>'{policy,candidate_electronic_auto_authorise}'<>'false'
     or v_result->>'financial_assignment_present'<>'false' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: unresolved policy broadened or financial context invented';
  end if;
  v_result:=public.candidate_daily_tiles_get_v1(v_candidate_context,v_today,14);
  if v_result#>>'{tiles,1,action_target,target_kind}' is distinct from 'TIMESHEET_DETAIL'
     or v_result#>>'{tiles,1,action_target,timesheet_id}' is distinct from v_receipt::text
     or v_result#>>'{tiles,1,action_target,row_signature}' !~ '^[a-f0-9]{32}$' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: submitted booked action must point at the one current receipt';
  end if;

  -- Real authenticated CREATE first use (the other date is still unsubmitted).
  -- Fixture flags, accounts and receipt all disappear in the outer rollback.
  update public.settings_defaults set candidate_app_feature_flags_json=
    candidate_app_feature_flags_json||jsonb_build_object('candidate_app_writes',true,'candidate_app_reads',true,
      'candidate_manager_approval',true,'candidate_daily_finalisation',true,
      'candidate_notifications',true) where id=1;
  insert into public.candidate_app_accounts(id,environment,email_normalized,status,
    password_scheme,password_scheme_version,password_salt,password_digest,password_changed_at_utc)
    values(v_account,'TEST','daily-booked-proof@example.invalid','ACTIVE',
      'PBKDF2-HMAC-SHA256',1,decode(repeat('41',16),'hex'),decode(repeat('42',32),'hex'),v_now);
  insert into public.candidate_app_sessions(id,account_id,environment,selected_candidate_id,
    status,refresh_token_hash,expires_at_utc,absolute_expires_at_utc)
    values(v_session,v_account,'TEST',v_candidate,'ACTIVE',
      extensions.digest(v_session::text,'sha256'),v_now+interval '1 hour',v_now+interval '2 hours');
  v_create_payload:=jsonb_build_object('workflow_kind','DAILY','scope','DAILY','route','PHONE',
    'submission_requested',true,'daily_source',jsonb_build_object('generation_id',v_generation,
      'work_date',v_today-1,'source_row_hash',lpad(to_hex(1),64,'0'),'booking_id','daily-booked-proof-0'));
  v_create_response:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',
    v_created_workflow,'CREATE',1,v_create_payload,v_create_key,v_now);
  v_replayed:=(select target_timesheet_id from public.candidate_submission_workflows where id=v_created_workflow);
  if v_create_response->>'state'<>'WORKER_DRAFT' or v_replayed is null
     or exists(select 1 from public.contract_weeks where timesheet_id=v_replayed)
     or exists(select 1 from public.timesheets_financials where timesheet_id=v_replayed)
     or not exists(select 1 from public.candidate_submission_workflows
       where id=v_created_workflow and work_date=v_today-1 and contract_id is null
         and contract_week_id is null and week_ending_date is null) then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: connected first admission failed';
  end if;
  v_result:=public.candidate_app_timesheet_detail_v2(v_session,'TEST',v_replayed,null,null,v_now);
  if v_result->'contract_week' is distinct from 'null'::jsonb
     or v_result->'weekly_entry' is distinct from 'null'::jsonb
     or v_result#>>'{daily_shift,work_date}' is distinct from (v_today-1)::text
     or v_result#>>'{daily_shift,booking_id}' is distinct from 'daily-booked-proof-0'
     or v_result#>>'{break_entry,mode}' is distinct from 'START_END_TIMES'
     or v_result#>>'{capabilities,can_edit_expenses}' is distinct from 'false'
     or jsonb_array_length(v_result->'workflows')<>1 then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: Daily detail invented a weekly record or lost its exact workflow';
  end if;
  v_result:=public.candidate_break_entry_context_get_v1(v_session,'TEST',v_created_workflow,v_now);
  if v_result->>'context_token' is distinct from
    (private._candidate_daily_break_entry_v1('daily-booked-proof-0',v_today-1)->>'context_token') then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: first-entry and submitted break contexts differ';
  end if;
  v_page_one:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,1,v_now);
  v_page_two:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',v_page_one->>'next_cursor',1,v_now);
  if jsonb_array_length(v_page_one->'items')<>1 or v_page_one->>'next_cursor' is null
     or jsonb_array_length(v_page_two->'items')<>1 or v_page_two->>'next_cursor' is not null
     or v_page_one#>>'{items,0,timesheet_id}'=v_page_two#>>'{items,0,timesheet_id}'
     or v_page_one#>>'{items,0,work_date}'=v_page_two#>>'{items,0,work_date}'
     or v_page_one#>'{items,0,contract_week_id}' is distinct from 'null'::jsonb
     or v_page_one#>'{items,0,contract_id}' is distinct from 'null'::jsonb then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: Daily pagination collapsed dates or fabricated a Contract Week: %, %',v_page_one,v_page_two;
  end if;
  -- A Candidate with both work types must page across the Weekly/Daily seam
  -- without losing or duplicating either Daily date. Roll this extra fixture
  -- back independently so all later no-Contract assertions stay meaningful.
  declare
    v_mixed_client uuid:=gen_random_uuid();
    v_mixed_contract uuid:=gen_random_uuid();
    v_mixed_week uuid:=gen_random_uuid();
    v_mixed_ending date:=v_today+(7-extract(isodow from v_today)::integer);
    v_mixed_cursor text;
    v_mixed_seen text[]:='{}'::text[];
    v_mixed_card jsonb;
    v_mixed_key text;
    v_mixed_page jsonb;
  begin
    insert into public.clients(id,name) values(v_mixed_client,'Daily proof weekly client');
    insert into public.client_settings(client_id,effective_from,default_submission_mode,week_ending_weekday)
      values(v_mixed_client,v_today-30,'ELECTRONIC',0);
    insert into public.contracts(id,candidate_id,client_id,start_date,end_date,
      week_ending_weekday_snapshot,default_submission_mode,pay_method_snapshot)
      values(v_mixed_contract,v_candidate,v_mixed_client,v_today-30,v_today+30,0,'ELECTRONIC','PAYE');
    insert into public.contract_weeks(id,contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot)
      values(v_mixed_week,v_mixed_contract,v_mixed_ending,0,'OPEN','ELECTRONIC');
    for v_case in 1..4 loop
      v_mixed_page:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',v_mixed_cursor,1,v_now);
      if jsonb_array_length(v_mixed_page->'items')<>1 then
        raise exception 'DAILY_BOOKED_SOURCE_PROOF: mixed page unexpectedly empty';
      end if;
      v_mixed_card:=v_mixed_page#>'{items,0}';
      v_mixed_key:=coalesce(v_mixed_card->>'contract_week_id',v_mixed_card->>'timesheet_id');
      if v_mixed_key is null or v_mixed_key=any(v_mixed_seen) then
        raise exception 'DAILY_BOOKED_SOURCE_PROOF: mixed page repeated or lost its identity';
      end if;
      v_mixed_seen:=array_append(v_mixed_seen,v_mixed_key);
      v_mixed_cursor:=v_mixed_page->>'next_cursor';
      exit when v_mixed_cursor is null;
    end loop;
    if cardinality(v_mixed_seen)<>3 or v_mixed_cursor is not null
       or not (v_mixed_week::text=any(v_mixed_seen)
         and v_receipt::text=any(v_mixed_seen) and v_replayed::text=any(v_mixed_seen)) then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: mixed pagination omitted a Weekly/Daily row';
    end if;
    raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS mixed Weekly/Daily single-row pagination';
    raise exception using errcode='Z9623',message='rollback mixed pagination fixture';
  exception when sqlstate 'Z9623' then null;
  end;
  -- Exact retry survives a source update; changed payload under the same key
  -- is a conflict. Neither may create another Timesheet.
  update public.candidate_daily_rota_days set source_row_hash=repeat('a',64)
    where generation_id=v_generation and rota_date=v_today-1;
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',gen_random_uuid(),
    'CREATE',1,v_create_payload,v_create_key,v_now);
  if v_result->>'idempotent_replay'<>'true' or v_result->>'workflow_id'<>v_created_workflow::text
     or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0')<>1 then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: uncertain-response retry duplicated or lost receipt';
  end if;
  begin
    perform public.candidate_workflow_transition_atomic_v1(v_session,'TEST',gen_random_uuid(),
      'CREATE',1,jsonb_set(v_create_payload,'{daily_source,source_row_hash}',to_jsonb(repeat('a',64))),
      v_create_key,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: changed request reused creation key';
  exception when sqlstate '40001' then null;
  end;
  update public.candidate_daily_rota_days set source_row_hash=lpad(to_hex(1),64,'0')
    where generation_id=v_generation and rota_date=v_today-1;
  begin
    perform private._candidate_daily_first_receipt_v1('TEST',v_candidate,v_source,gen_random_uuid(),true,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: second workflow duplicated current booking';
  exception when sqlstate '40001' then null;
  end;

  -- Connected submission uses real transition/manager/finalisation owners.
  -- Only the verified image/PDF bytes are represented by fixture receipts.
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_created_workflow,
    'COMPONENT_PREPARE',1,jsonb_build_object('component_kind','CANDIDATE_SIGNATURE',
      'document_role','CANDIDATE_SIGNATURE','storage_key','daily-proof/candidate.png',
      'media_type','image/png','byte_size',256),'daily-proof-signature-prepare',v_now);
  v_signature_id:=(v_result->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_created_workflow,
    'COMPONENT_COMPLETE',1,jsonb_build_object('component_id',v_signature_id,
      'source_content_sha256_hex',repeat('a1',32),'verified_byte_size',256,
      'verified_media_type','image/png'),'daily-proof-signature-complete',v_now);
  v_submit:=jsonb_build_object('contract_version','CANDIDATE_DAILY_FACTUAL_SUBMISSION_V1',
    'timesheet_patch_json',jsonb_build_object(
      'worked_start_iso',((v_today-1)::text||' 21:00 Europe/London')::timestamptz,
      'worked_end_iso',(v_today::text||' 07:00 Europe/London')::timestamptz,
      'break_start_iso',null,'break_end_iso',null,'break_minutes',0,
      'actual_schedule_json',jsonb_build_array(jsonb_build_object(
        'date',(v_today-1)::text,'start','21:00','end','07:00','break_minutes',0))));
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_created_workflow,
    'WORKER_SUBMIT',1,jsonb_build_object('immutable_submission',v_submit,
      'approval_route','PHONE','candidate_signature_component_id',v_signature_id,
      'candidate_signed_at_utc',v_now),'daily-proof-submit',v_now);
  if v_result->>'state'<>'WORKER_SUBMITTED_PENDING_REVIEW_DOCUMENT' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: connected submit failed: %',v_result;
  end if;
  perform pg_temp.candidate_register_all_review_components(v_created_workflow,2,'daily-proof',v_now);

  -- A submitted Daily receipt which never reached manager approval is a real
  -- current Timesheet, but Office must still be able to remove it permanently
  -- while it is financially clean. Exercise the exact Preview/APPLY boundary
  -- inside a subtransaction, then roll that proof back so the same fixture can
  -- continue through manager approval and canonical completion below.
  begin
    select candidate_app_system_actor_user_id into strict v_office_actor
    from public.settings_defaults where id=1;
    select nullif(btrim(coalesce(s.value->>'backend_row_signature',
      s.value->>'row_signature',s.value->>'signature','')),'')
    into v_delete_signature
    from public.timesheet_lifecycle_guard_signature_v1(v_replayed,null,false) s(value);
    if v_delete_signature is null then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: unfinished Daily delete signature missing';
    end if;
    v_delete_preview:=public.timesheet_daily_abandoned_receipt_delete_preview_v1(
      v_replayed,v_office_actor,v_replayed,v_delete_signature);
    if v_delete_preview->>'applicable'<>'true'
       or v_delete_preview->>'decision'<>'PERMANENT_DELETE'
       or v_delete_preview->>'eligible'<>'true'
       or v_delete_preview->>'workflow_id' is distinct from v_created_workflow::text
       or v_delete_preview->>'current_timesheet_id' is distinct from v_replayed::text
       or jsonb_array_length(coalesce(v_delete_preview->'contract_week_ids','[]'::jsonb))<>0 then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: unfinished Daily delete preview failed: %',v_delete_preview;
    end if;
    v_delete_result:=public.timesheet_daily_abandoned_receipt_delete_apply_v1(
      v_replayed,v_office_actor,v_replayed,v_delete_signature);
    if v_delete_result->>'apply_performed'<>'true'
       or v_delete_result->>'deleted'<>'true'
       or v_delete_result->>'database_commit_confirmed'<>'true'
       or v_delete_result->>'deleted_workflow_id' is distinct from v_created_workflow::text
       or exists(select 1 from public.timesheets where timesheet_id=v_replayed)
       or exists(select 1 from public.candidate_submission_workflows where id=v_created_workflow)
       or exists(select 1 from public.candidate_submission_components where workflow_id=v_created_workflow)
       or exists(select 1 from public.candidate_approval_requests where workflow_id=v_created_workflow)
       or exists(select 1 from public.timesheets_financials where timesheet_id=v_replayed)
       or not exists(select 1 from public.audit_events where object_id_text=v_created_workflow::text
         and action='CANDIDATE_DAILY_ABANDONED_RECEIPT_DELETE_APPLIED') then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: unfinished Daily delete apply failed: %',v_delete_result;
    end if;
    raise exception using errcode='Z9624',message='DAILY_BOOKED_SOURCE_DELETE_PROOF_ROLLBACK';
  exception when sqlstate 'Z9624' then null;
  end;

  if not exists(select 1 from public.timesheets where timesheet_id=v_replayed)
     or not exists(select 1 from public.candidate_submission_workflows where id=v_created_workflow)
     or not exists(select 1 from public.candidate_submission_components where workflow_id=v_created_workflow) then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: delete proof did not roll back cleanly';
  end if;
  perform pg_temp.candidate_phone_approve_all(v_session,v_created_workflow,2,'daily-proof','manager-signature',v_now);
  perform pg_temp.candidate_register_all_final_components(v_created_workflow,2,'daily-proof',v_now);
  v_begin_receipt:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_created_workflow,
    'BEGIN_CANONICAL_DAILY_SAVE',2,'{}'::jsonb,'daily-proof-begin-receipt',v_now);
  if v_begin_receipt->>'receipt_mode'<>'DAILY_FACTUAL'
     or v_begin_receipt ? 'canonical_financial_sha256' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: receipt requested financial processing';
  end if;
  v_materialisation:=jsonb_build_object('contract_version','CANDIDATE_DAILY_FACTUAL_RECEIPT_V1',
    'workflow_id',v_created_workflow,'workflow_generation',2,'timesheet_id',v_replayed,
    'canonical_save_input_sha256_hex',v_begin_receipt->>'canonical_save_input_sha256_hex');
  v_result:=public.candidate_submission_finalize_atomic_v1(null,'TEST',v_created_workflow,
    2,null,v_final_key,v_now,v_materialisation);
  if v_result->>'ok'<>'true' or v_result->>'state'<>'RECEIVED'
     or v_result->>'auto_authorised'<>'false'
     or v_result->>'canonical_financial_sha256_hex' is not null
     or exists(select 1 from public.timesheets_financials where timesheet_id=v_replayed)
     or not exists(select 1 from public.timesheets where timesheet_id=v_replayed
       and submission_mode='ELECTRONIC' and authorised_at_server is null
       and worked_minutes=600 and r2_nurse_key is not null and r2_auth_key is not null)
     or not exists(select 1 from public.timesheet_evidence where timesheet_id=v_replayed
       and kind='TIMESHEET' and processing_state<>'SUPERSEDED') then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: factual completion or signed evidence failed: %',v_result;
  end if;
  v_result:=public.candidate_submission_finalize_atomic_v1(null,'TEST',v_created_workflow,
    2,null,v_final_key,v_now,v_materialisation);
  if v_result->>'idempotent_replay'<>'true'
     or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0')<>1 then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: completion retry lost/duplicated receipt';
  end if;

  v_result:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,100,v_now);
  select item into v_bad from jsonb_array_elements(v_result->'items') item where item->>'timesheet_id'=v_replayed::text;
  if v_bad is null or v_bad->>'candidate_status_code' is distinct from 'RECEIVED'
    or v_bad->>'manager_approval_state' is distinct from 'MANAGER_APPROVED'
    or (v_bad->>'total_hours')::numeric is distinct from 10::numeric
    or v_bad->>'authorised' is distinct from 'false' then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: manager-approved unresolved receipt not displayed as ordinary received: %',v_bad;
  end if;

  -- The real background writer can produce a genuine UNASSIGNED snapshot
  -- before Office resolution. Its zero hours must not erase submitted hours
  -- in the app. Nothing in either read may change that financial snapshot.
  begin
    perform public.enqueue_ts_financials_priority(array[v_replayed],'CONTEXT_CHANGED');
    select id into strict v_resolution_outbox from public.ts_financials_outbox
      where timesheet_id=v_replayed;
    v_resolution_snapshot:=jsonb_build_object('timesheet_version',1,
      'basis','SELF_REPORTED','candidate_id',v_candidate,
      'candidate_assignment','ASSIGNED','role','UNRESOLVED ROLE',
      'total_hours',0,'total_pay_ex_vat',0,'total_charge_ex_vat',0,
      'processing_status','UNASSIGNED','has_rate_issue',true,
      'has_pay_channel_issue',false,'policy_snapshot_json','{}'::jsonb,
      'rate_source_refs_json','{}'::jsonb);
    select to_jsonb(r) into v_result from public.tsfin_write_snapshots_and_complete(
      jsonb_build_array(jsonb_build_object('timesheet_id',v_replayed,
        'outbox_id',v_resolution_outbox,'snapshot',v_resolution_snapshot))) r;
    if v_result->>'ok_count' is distinct from '1'
      or v_result->>'fail_count' is distinct from '0' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: pending snapshot fixture failed: %',v_result;
    end if;
    select to_jsonb(f) into strict v_documents_before from public.timesheets_financials f
      where timesheet_id=v_replayed and is_current;
    v_result:=public.candidate_app_timesheet_detail_v2(v_session,'TEST',v_replayed,null,null,v_now);
    if (v_result#>>'{hours,total_hours}')::numeric is distinct from 10::numeric
      or v_result#>>'{manager_review,manager_approval_state}' is distinct from 'APPROVED' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: pending snapshot hid factual hours or actual approval';
    end if;
    v_result:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,100,v_now);
    select item into v_bad from jsonb_array_elements(v_result->'items') item
      where item->>'timesheet_id'=v_replayed::text;
    if v_bad is null or (v_bad->>'total_hours')::numeric is distinct from 10::numeric
      or v_bad->>'manager_approval_state' is distinct from 'MANAGER_APPROVED' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: list lost pending receipt hours or approval';
    end if;
    select to_jsonb(f) into strict v_documents_after from public.timesheets_financials f
      where timesheet_id=v_replayed and is_current;
    if v_documents_after is distinct from v_documents_before then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: display mutated financial state';
    end if;
    raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS pending zero snapshot preserves factual hours and approval in list/detail without financial writes';
    raise exception 'DAILY_PENDING_SNAPSHOT_ROLLBACK' using errcode='Z9622';
  exception when sqlstate 'Z9622' then null;
  end;

  -- Exercise the existing Office resolution/write/authorisation owners against
  -- this exact manager-approved receipt. Only disposable fixture mappings and
  -- a canonical snapshot fixture are supplied; no financial owner is replaced.
  -- Roll this branch back so the independent no-TSFIN withdrawal proof follows.
  begin
    select jsonb_build_object('signature_keys',jsonb_build_array(t.r2_nurse_key,t.r2_auth_key),
      'signature_hashes',jsonb_build_array(t.img_sha256_nurse,t.img_sha256_auth),
      'manager',jsonb_build_array(t.auth_name,t.auth_job_title,t.candidate_manager_approved_at_utc),
      'evidence',(select jsonb_agg(to_jsonb(e) order by e.id)
        from public.timesheet_evidence e where e.timesheet_id=t.timesheet_id))
      into v_documents_before from public.timesheets t where t.timesheet_id=v_replayed;
    insert into public.clients(id,name) values(v_resolution_client,'Disposable Daily resolution proof');
    insert into public.client_hospitals(client_id,hospital_name_norm)
      select v_resolution_client,jsonb_build_array(t.hospital_norm)
      from public.timesheets t where t.timesheet_id=v_replayed;
    select to_jsonb(c) into v_resolution_context
      from public.tsfin_load_context_batch(array[v_replayed]) c;
    if v_resolution_context->>'effective_timesheet_id'<>v_replayed::text
       or v_resolution_context->>'out_client_id'<>v_resolution_client::text
       or v_resolution_context#>>'{out_candidate,id}'<>v_candidate::text then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: existing Office resolver lost receipt identity';
    end if;
    perform public.enqueue_ts_financials_priority(array[v_replayed],'CONTEXT_CHANGED');
    select id into strict v_resolution_outbox from public.ts_financials_outbox
      where timesheet_id=v_replayed order by created_at desc,id limit 1;
    v_resolution_snapshot:=jsonb_build_object(
      'timesheet_version',1,'basis','SELF_REPORTED','candidate_id',v_candidate,
      'client_id',v_resolution_client,'candidate_assignment','ASSIGNED',
      'role','UNRESOLVED ROLE','band',null,'pay_method','UMBRELLA',
      'occupant_key_norm',v_resolution_context#>>'{out_timesheet,occupant_key_norm}',
      'worked_start_iso',v_resolution_context#>>'{out_timesheet,worked_start_iso}',
      'worked_end_iso',v_resolution_context#>>'{out_timesheet,worked_end_iso}',
      'break_minutes',0,'hours_night',10,'total_hours',10,
      'pay_night',10,'charge_night',20,'total_pay_ex_vat',100,'total_charge_ex_vat',200,
      'processing_status','PENDING_AUTH','has_rate_issue',false,'has_pay_channel_issue',false,
      'policy_snapshot_json','{}'::jsonb,'rate_source_refs_json','{}'::jsonb);
    select to_jsonb(r) into v_result from public.tsfin_write_snapshots_and_complete(
      jsonb_build_array(jsonb_build_object('timesheet_id',v_replayed,
        'outbox_id',v_resolution_outbox,'snapshot',v_resolution_snapshot))) r;
    if v_result->>'ok_count'<>'1' or v_result->>'fail_count'<>'0'
       or not exists(select 1 from public.timesheets_financials
         where timesheet_id=v_replayed and is_current and processing_status='PENDING_AUTH') then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Office recalculation did not advance receipt: %',v_result;
    end if;
    select candidate_app_system_actor_user_id into v_office_actor
      from public.settings_defaults where id=1;
    v_result:=public.timesheet_authorise_generic_atomic(v_replayed,v_replayed,v_office_actor,v_now,null);
    if v_result->>'ok'<>'true'
       or not exists(select 1 from public.timesheets where timesheet_id=v_replayed
         and is_current and authorised_at_server is not null)
       or not exists(select 1 from public.timesheets_financials where timesheet_id=v_replayed
         and is_current and authorised_at_utc is not null) then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: existing Office authorisation rejected resolved receipt: %',v_result;
    end if;
    select jsonb_build_object('signature_keys',jsonb_build_array(t.r2_nurse_key,t.r2_auth_key),
      'signature_hashes',jsonb_build_array(t.img_sha256_nurse,t.img_sha256_auth),
      'manager',jsonb_build_array(t.auth_name,t.auth_job_title,t.candidate_manager_approved_at_utc),
      'evidence',(select jsonb_agg(to_jsonb(e) order by e.id)
        from public.timesheet_evidence e where e.timesheet_id=t.timesheet_id))
      into v_documents_after from public.timesheets t where t.timesheet_id=v_replayed;
    if v_documents_after is distinct from v_documents_before
       or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0')<>1 then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Office resolution changed approved documents or duplicated receipt';
    end if;
    raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS existing Office resolution, canonical snapshot writer and authorisation preserve exact signature/document links';
    raise exception 'DAILY_RESOLUTION_BRANCH_ROLLBACK' using errcode='Z9621';
  exception when sqlstate 'Z9621' then null;
  end;
  begin
    update public.timesheets set authorised_at_server=v_now where timesheet_id=v_replayed;
    perform private._candidate_daily_receipt_reset_v1('TEST',v_candidate,v_replayed,v_replayed,
      'Must not withdraw',v_account,'CANDIDATE_WITHDRAWN',v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: Office-authorised withdrawal accepted';
  exception when sqlstate '55000' then null;
  end;
  -- The published Rota can move on after submission. Existing business history
  -- remains usable, but a cached source-only target is no longer admission.
  update private.candidate_daily_authority_scopes set active_generation_id=null
    where environment='TEST' and candidate_id=v_candidate;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: cached source admitted after window rollout';
  exception when sqlstate '40001' then null;
  end;
  v_result:=public.candidate_app_timesheet_detail_v2(v_session,'TEST',v_replayed,null,null,v_now);
  if v_result#>>'{timesheet,id}'<>v_replayed::text then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: window rollout hid an existing receipt';
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_created_workflow,
    'CANCEL',2,jsonb_build_object('reason_note','Correct my hours'),'daily-proof-cancel',v_now);
  select timesheet_id into v_current_id from public.timesheets
    where booking_id='daily-booked-proof-0' and is_current;
  if v_current_id is null or v_current_id=v_replayed
     or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0' and is_current)<>1
     or exists(select 1 from public.timesheets_financials where timesheet_id=v_current_id)
     or exists(select 1 from public.timesheets where timesheet_id=v_current_id and (
       worked_start_iso is not null or r2_nurse_key is not null or r2_auth_key is not null))
     or exists(select 1 from public.timesheet_evidence where timesheet_id=v_replayed
       and processing_state<>'SUPERSEDED') then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: receipt withdrawal did not preserve one clean current family';
  end if;
  v_result:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,100,v_now);
  if (select count(*) from jsonb_array_elements(v_result->'items') item
      where item->>'booking_id'='daily-booked-proof-0')<>1
     or exists(select 1 from jsonb_array_elements(v_result->'items') item
       where item->>'timesheet_id'=v_replayed::text) then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: withdrawn history leaked into the current list';
  end if;
  -- Resubmission targets the clean current record; it must not INSERT another
  -- family member just because there is no Contract Week/financial snapshot.
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_resubmit_workflow,
    'CREATE',1,jsonb_build_object('workflow_kind','DAILY','scope','DAILY','route','PHONE',
      'target_timesheet_id',v_current_id),'daily-proof-resubmit-create',v_now);
  if (select target_timesheet_id from public.candidate_submission_workflows where id=v_resubmit_workflow)
      is distinct from v_current_id then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: resubmission did not reuse the current record';
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_resubmit_workflow,
    'COMPONENT_PREPARE',1,jsonb_build_object('component_kind','CANDIDATE_SIGNATURE',
      'document_role','CANDIDATE_SIGNATURE','storage_key','daily-proof-resubmit/candidate.png',
      'media_type','image/png','byte_size',256),'daily-proof-resubmit-signature-prepare',v_now);
  v_signature_id:=(v_result->>'component_id')::uuid;
  perform public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_resubmit_workflow,
    'COMPONENT_COMPLETE',1,jsonb_build_object('component_id',v_signature_id,
      'source_content_sha256_hex',repeat('b1',32),'verified_byte_size',256,
      'verified_media_type','image/png'),'daily-proof-resubmit-signature-complete',v_now);
  perform public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_resubmit_workflow,
    'WORKER_SUBMIT',1,jsonb_build_object('immutable_submission',v_submit,
      'approval_route','PHONE','candidate_signature_component_id',v_signature_id,
      'candidate_signed_at_utc',v_now),'daily-proof-resubmit',v_now);
  perform pg_temp.candidate_register_all_review_components(v_resubmit_workflow,2,'daily-proof-resubmit',v_now);
  perform pg_temp.candidate_phone_approve_all(v_session,v_resubmit_workflow,2,'daily-proof-resubmit','new-manager-signature',v_now);
  perform pg_temp.candidate_register_all_final_components(v_resubmit_workflow,2,'daily-proof-resubmit',v_now);
  v_begin_receipt:=public.candidate_workflow_transition_atomic_v1(v_session,'TEST',v_resubmit_workflow,
    'BEGIN_CANONICAL_DAILY_SAVE',2,'{}'::jsonb,'daily-proof-resubmit-begin',v_now);
  v_result:=public.candidate_submission_finalize_atomic_v1(null,'TEST',v_resubmit_workflow,
    2,null,'daily-proof-resubmit-final',v_now,jsonb_build_object(
      'contract_version','CANDIDATE_DAILY_FACTUAL_RECEIPT_V1','workflow_id',v_resubmit_workflow,
      'workflow_generation',2,'timesheet_id',v_current_id,
      'canonical_save_input_sha256_hex',v_begin_receipt->>'canonical_save_input_sha256_hex'));
  if v_result->>'state'<>'RECEIVED'
     or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0')<>2
     or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0' and is_current)<>1
     or exists(select 1 from public.timesheets_financials where timesheet_id=v_current_id)
     or not exists(select 1 from public.timesheet_evidence where timesheet_id=v_current_id
       and kind='TIMESHEET' and processing_state<>'SUPERSEDED') then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: resubmitted current record, evidence or finance boundary failed';
  end if;
  raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS connected submit, PHONE approval, receipt with signed evidence, exact retry, protected withdrawal, resubmission on one current family after window rollout, and revoked evidence history';
  update private.candidate_daily_authority_scopes set active_generation_id=v_generation
    where environment='TEST' and candidate_id=v_candidate;

  -- Office rejection uses the existing authenticated preview/confirm adapter.
  -- Rota-only removal cannot erase access to already submitted business history
  -- or make Office depend on the Candidate still being entitled to new shifts.
  begin
    update private.candidate_daily_entitlements set enabled=false
      where environment='TEST' and candidate_id=v_candidate;
    update public.candidates set key_norm=null where id=v_candidate;
    v_result:=public.candidate_app_timesheet_detail_v2(
      v_session,'TEST',v_current_id,null,null,v_now);
    if v_result#>>'{timesheet,id}'<>v_current_id::text
       or v_result#>>'{capabilities,can_edit_hours}'<>'false' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Rota removal hid submitted history or retained editing';
    end if;
    v_result:=public.candidate_app_timesheet_page_v1(v_session,'TEST','CURRENT',null,100,v_now);
    if (select count(*) from jsonb_array_elements(v_result->'items') item
        where item->>'booking_id'='daily-booked-proof-0')<>1 then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Rota removal lost the current Timesheet';
    end if;
    select candidate_app_system_actor_user_id into v_office_actor
      from public.settings_defaults where id=1;
    v_office_preview:=public.cloudtms_office_candidate_adapter_v1(
      'REJECT_PREVIEW',v_office_actor,'TEST',jsonb_build_object('timesheet_id',v_current_id),v_now);
    if v_office_preview->>'permitted'<>'true' then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: no-finance Office rejection preview failed: %',v_office_preview;
    end if;
    v_office_payload:=jsonb_build_object('timesheet_id',v_current_id,
      'expected_timesheet_id',v_current_id,
      'expected_row_signature',v_office_preview->>'expected_row_signature',
      'context_sha256',v_office_preview->>'context_sha256',
      'reason','Please correct the worked hours','idempotency_key','daily-proof-office-reject');
    v_result:=public.cloudtms_office_candidate_adapter_v1(
      'REJECT_CONFIRM',v_office_actor,'TEST',v_office_payload,v_now);
    v_office_replacement:=(v_result->>'timesheet_id')::uuid;
    if v_result->>'ok'<>'true' or v_office_replacement is null
       or v_office_replacement=v_current_id
       or (select count(*) from public.timesheets where booking_id='daily-booked-proof-0' and is_current)<>1
       or exists(select 1 from public.timesheets_financials where timesheet_id=v_office_replacement)
       or exists(select 1 from public.timesheet_evidence where timesheet_id=v_current_id and processing_state<>'SUPERSEDED')
       or not exists(select 1 from public.candidate_submission_workflows
         where id=v_resubmit_workflow and state='REJECTED' and rejection_reason='Please correct the worked hours') then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Office rejection failed current/history/evidence boundary: %',v_result;
    end if;
    v_result:=public.cloudtms_office_candidate_adapter_v1(
      'REJECT_CONFIRM',v_office_actor,'TEST',v_office_payload,v_now);
    if v_result->>'idempotent_replay'<>'true'
       or v_result->>'timesheet_id'<>v_office_replacement::text then
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: Office rejection replay duplicated the current family';
    end if;
    raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS Office rejection/replay without finance or Rota access; history and evidence retained';
    raise exception 'DAILY_OFFICE_BRANCH_ROLLBACK' using errcode='Z9622';
  exception when sqlstate 'Z9622' then null;
  end;

  for v_case in 1..8 loop
    v_bad:=case v_case
      when 1 then v_source||jsonb_build_object('generation_id',gen_random_uuid())
      when 2 then v_source||jsonb_build_object('source_row_hash',repeat('f',64))
      when 3 then v_source||jsonb_build_object('booking_id','different-booking')
      when 4 then v_source||jsonb_build_object('work_date',v_today+1)
      when 5 then v_source||jsonb_build_object('client_id',gen_random_uuid())
      when 6 then v_source-'work_date'
      when 7 then v_source||jsonb_build_object('booking_id',1234)
      else 'null'::jsonb end;
    begin
      perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_bad,v_now);
      raise exception 'DAILY_BOOKED_SOURCE_PROOF: invalid input accepted, case %',v_case;
    exception when sqlstate '40001' or sqlstate '22023' or sqlstate '55000' then null;
    end;
  end loop;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_other,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: another Candidate accepted';
  exception when sqlstate '55000' then null;
  end;
  begin
    perform private._candidate_daily_booked_source_v1('LIVE',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: another environment accepted';
  exception when sqlstate '40001' then null;
  end;
  update private.candidate_daily_entitlements set enabled=false
    where environment='TEST' and candidate_id=v_candidate;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: withdrawn entitlement accepted';
  exception when sqlstate '40001' then null;
  end;
  update private.candidate_daily_entitlements set enabled=true
    where environment='TEST' and candidate_id=v_candidate;
  update private.candidate_daily_authority_scopes set transition_in_progress=true
    where environment='TEST' and candidate_id=v_candidate;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: transitioning authority accepted';
  exception when sqlstate '40001' then null;
  end;
  update private.candidate_daily_authority_scopes set transition_in_progress=false
    where environment='TEST' and candidate_id=v_candidate;
  update public.candidate_daily_rota_days set timesheet_authorised=true
    where generation_id=v_generation and rota_date=v_today;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: authorised source accepted';
  exception when sqlstate '55000' then null;
  end;
  update public.candidate_daily_rota_days set timesheet_authorised=false
    where generation_id=v_generation and rota_date=v_today;
  delete from public.candidate_daily_rota_days where generation_id=v_generation and rota_date=v_today+12;
  begin
    perform private._candidate_daily_booked_source_v1('TEST',v_candidate,v_source,v_now);
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: incomplete generation accepted';
  exception when sqlstate '55000' then null;
  end;
  if has_function_privilege('anon','private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz)','EXECUTE')
     or has_function_privilege('authenticated','private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz)','EXECUTE')
     or has_function_privilege('service_role','private._candidate_daily_booked_source_v1(text,uuid,jsonb,timestamptz)','EXECUTE') then
    raise exception 'DAILY_BOOKED_SOURCE_PROOF: owner-only helper exposed';
  end if;
  raise notice 'DAILY_BOOKED_SOURCE_PROOF: PASS exact source/date, overnight, no Client/rates/CW, no receipt on read, stale/foreign/disabled/partial negatives, internal ACL';
end;
$proof$;
rollback;
