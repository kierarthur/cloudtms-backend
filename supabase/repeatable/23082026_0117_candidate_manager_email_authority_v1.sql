create or replace function private._candidate_manager_terminal_mail_payload_v1(
  p_mail jsonb,
  p_mail_kind text
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare v_kind text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_mail_kind,'')));
begin
  if v_kind not in ('WITHDRAWAL','CANCELLATION')
     or pg_catalog.jsonb_typeof(p_mail)<>'object'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(p_mail))<>6
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_mail) k where k not in (
       'subject','body_text','body_html','manager_template_version','manager_template_sha256','manager_submission_type'
     ))
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_mail->>'subject',''))) not between 1 and 240
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_mail->>'body_text',''))) not between 1 and 20000
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_mail->>'body_html',''))) not between 1 and 50000
     or (p_mail->>'body_html')~*'(<(a|img|svg|iframe|form|script|style)([[:space:]>])|(^|[^a-z])(href|src|on[a-z]+)[[:space:]]*=|https?://)'
     or coalesce(p_mail->>'manager_template_sha256','')!~'^[0-9a-f]{64}$'
     or coalesce(p_mail->>'manager_template_version','')!~'^[1-9][0-9]*$'
     or p_mail->>'manager_submission_type' not in ('TIMESHEET','EXPENSE_CLAIM')
  then raise exception using errcode='22023',message='CANDIDATE_MANAGER_TERMINAL_MAIL_INVALID'; end if;
  return pg_catalog.jsonb_build_object(
    'subject',p_mail->>'subject','body_text',p_mail->>'body_text','body_html',p_mail->>'body_html',
    'manager_template_version',(p_mail->>'manager_template_version')::bigint,
    'manager_template_sha256',p_mail->>'manager_template_sha256',
    'manager_submission_type',p_mail->>'manager_submission_type'
  );
end;
$function$;

create or replace function public.candidate_manager_email_route_receipt_commit_v1(
  p_environment text,
  p_workflow_id uuid,
  p_approval_request_id uuid,
  p_request_generation integer,
  p_credential_generation integer,
  p_mail_outbox_id uuid,
  p_manager_token_hash_hex text,
  p_manager_route_ticket_id uuid,
  p_route_revision bigint,
  p_registration_receipt_sha256_hex text,
  p_route_semantic_sha256_hex text,
  p_mail_kind text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_expected_environment text;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_approval public.candidate_approval_requests%rowtype;
  v_outbox public.mail_outbox%rowtype;
  v_existing public.candidate_manager_email_route_receipts%rowtype;
  v_receipt public.candidate_manager_email_route_receipts%rowtype;
  v_token_hash bytea;
  v_registration_hash bytea;
  v_semantic_hash bytea;
  v_mail_kind text:=pg_catalog.upper(pg_catalog.btrim(p_mail_kind));
begin
  select pg_catalog.upper(candidate_app_environment) into strict v_expected_environment
  from public.settings_defaults where id=1;
  if v_expected_environment<>pg_catalog.upper(pg_catalog.btrim(p_environment))
     or v_mail_kind not in ('INITIAL','REMINDER','RENEWAL')
     or p_request_generation<1 or p_credential_generation<1 or p_route_revision<1
     or coalesce(p_manager_token_hash_hex,'')!~'^[0-9a-f]{64}$'
     or coalesce(p_registration_receipt_sha256_hex,'')!~'^[0-9a-f]{64}$'
     or coalesce(p_route_semantic_sha256_hex,'')!~'^[0-9a-f]{64}$'
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_idempotency_key,''))) not between 1 and 200
  then raise exception using errcode='22023',message='CANDIDATE_MANAGER_ROUTE_RECEIPT_INVALID'; end if;
  v_token_hash:=pg_catalog.decode(p_manager_token_hash_hex,'hex');
  v_registration_hash:=pg_catalog.decode(p_registration_receipt_sha256_hex,'hex');
  v_semantic_hash:=pg_catalog.decode(p_route_semantic_sha256_hex,'hex');

  select * into strict v_workflow from public.candidate_submission_workflows
    where id=p_workflow_id for update;
  select * into strict v_approval from public.candidate_approval_requests
    where id=p_approval_request_id and workflow_id=p_workflow_id
      and request_generation=p_request_generation and method='EMAIL'
      and state='PENDING' and token_hash=v_token_hash and expires_at_utc>p_now_utc
    for update;
  select * into v_existing from public.candidate_manager_email_route_receipts
    where workflow_id=p_workflow_id and idempotency_key=p_idempotency_key for update;
  if found then
    if v_existing.manager_route_ticket_id<>p_manager_route_ticket_id
       or v_existing.registration_receipt_sha256<>v_registration_hash
       or v_existing.route_semantic_sha256<>v_semantic_hash
       or v_existing.approval_request_id<>p_approval_request_id
       or v_existing.request_generation<>p_request_generation
       or v_existing.credential_generation<>p_credential_generation
       or v_existing.mail_kind<>v_mail_kind
    then raise exception using errcode='40001',message='CANDIDATE_MANAGER_ROUTE_RECEIPT_CONFLICT'; end if;
    return pg_catalog.jsonb_build_object('ok',true,'idempotent_replay',true,
      'route_receipt_id',v_existing.route_receipt_id,'manager_route_ticket_id',v_existing.manager_route_ticket_id,
      'route_revision',v_existing.route_revision,'state',v_existing.state);
  end if;

  select * into strict v_outbox from public.mail_outbox
    where id=p_mail_outbox_id and context_kind='CANDIDATE_WORKFLOW' and context_id=p_workflow_id
      and status='QUEUED' and sent_at is null
      and payment_scope_json->>'candidate_approval_request_id'=p_approval_request_id::text
      and (payment_scope_json->>'candidate_approval_request_generation')::integer=p_request_generation
      and pg_catalog.upper(payment_scope_json->>'candidate_manager_mail_kind')=v_mail_kind
    for update;

  update public.candidate_manager_email_route_receipts set
    state='RETIRED',retired_at_utc=p_now_utc,updated_at_utc=p_now_utc
  where workflow_id=p_workflow_id and state='CURRENT';

  insert into public.candidate_manager_email_route_receipts(
    workflow_id,approval_request_id,request_generation,credential_generation,mail_kind,
    manager_token_hash_snapshot,manager_route_ticket_id,route_revision,
    registration_receipt_sha256,route_semantic_sha256,idempotency_key,state,
    registered_at_utc,current_at_utc,created_at_utc,updated_at_utc
  ) values (
    p_workflow_id,p_approval_request_id,p_request_generation,p_credential_generation,v_mail_kind,
    v_token_hash,p_manager_route_ticket_id,p_route_revision,v_registration_hash,v_semantic_hash,
    p_idempotency_key,'CURRENT',p_now_utc,p_now_utc,p_now_utc,p_now_utc
  ) returning * into v_receipt;

  update public.candidate_approval_requests set
    current_manager_route_receipt_id=v_receipt.route_receipt_id,updated_at_utc=p_now_utc
  where id=p_approval_request_id;

  update public.mail_outbox set payment_scope_json=payment_scope_json||pg_catalog.jsonb_build_object(
    'candidate_manager_route_receipt_id',v_receipt.route_receipt_id,
    'candidate_manager_route_ticket_id',p_manager_route_ticket_id,
    'candidate_manager_route_revision',p_route_revision,
    'candidate_manager_route_registration_sha256',p_registration_receipt_sha256_hex
  ) where id=p_mail_outbox_id;

  return pg_catalog.jsonb_build_object('ok',true,'idempotent_replay',false,
    'route_receipt_id',v_receipt.route_receipt_id,'manager_route_ticket_id',v_receipt.manager_route_ticket_id,
    'route_revision',v_receipt.route_revision,'state',v_receipt.state);
exception when no_data_found then
  raise exception using errcode='40001',message='CANDIDATE_MANAGER_ROUTE_RECEIPT_STALE';
end;
$function$;

create or replace function public.candidate_manager_email_route_receipt_retire_v1(
  p_workflow_id uuid,
  p_approval_request_id uuid,
  p_reason_code text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare v_count integer;
begin
  if coalesce(p_reason_code,'')!~'^[A-Z][A-Z0-9_]{2,99}$' then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_ROUTE_RETIRE_REASON_INVALID';
  end if;
  update public.candidate_manager_email_route_receipts set state='RETIRED',
    failure_code=p_reason_code,retired_at_utc=coalesce(retired_at_utc,p_now_utc),updated_at_utc=p_now_utc
  where workflow_id=p_workflow_id and approval_request_id=p_approval_request_id and state='CURRENT';
  get diagnostics v_count=row_count;
  update public.candidate_approval_requests set current_manager_route_receipt_id=null,updated_at_utc=p_now_utc
  where id=p_approval_request_id and workflow_id=p_workflow_id and current_manager_route_receipt_id is not null;
  return pg_catalog.jsonb_build_object('ok',true,'retired_count',v_count);
end;
$function$;

create or replace function public.candidate_manager_email_settings_get_v1()
returns jsonb
language sql
security definer
stable
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'ok',true,'templates',s.candidate_manager_email_templates_json,
    'version',s.candidate_manager_email_templates_version,
    'sanitizer_policy_version',s.candidate_manager_email_sanitizer_policy_version,
    'semantic_sha256_hex',pg_catalog.encode(s.candidate_manager_email_templates_sha256,'hex'),
    'updated_at_utc',s.candidate_manager_email_templates_updated_at_utc
  ) from public.settings_defaults s where s.id=1
$function$;

create or replace function public.candidate_manager_email_settings_set_v1(
  p_expected_version bigint,
  p_templates jsonb,
  p_sanitizer_policy_version text,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_hash bytea;
  v_actor bytea;
  v_changed integer;
  v_version bigint;
  v_submission_type text;
  v_mail_kind text;
  v_template jsonb;
  v_existing public.candidate_manager_email_template_versions%rowtype;
begin
  if pg_catalog.jsonb_typeof(p_templates)<>'object'
     or p_templates->>'schema_version'<>'CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1'
     or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(p_templates))<>3
     or not (p_templates ? 'TIMESHEET' and p_templates ? 'EXPENSE_CLAIM')
     or p_sanitizer_policy_version<>'MANAGER_EMAIL_SAFE_HTML_V1'
     or coalesce(p_actor_identity_hmac_hex,'')!~'^[0-9a-f]{64}$'
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_idempotency_key,''))) not between 1 and 200
  then raise exception using errcode='22023',message='CANDIDATE_MANAGER_EMAIL_SETTINGS_INVALID'; end if;

  foreach v_submission_type in array array['TIMESHEET','EXPENSE_CLAIM'] loop
    if pg_catalog.jsonb_typeof(p_templates->v_submission_type)<>'object'
       or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(p_templates->v_submission_type))<>5
    then raise exception using errcode='22023',message='CANDIDATE_MANAGER_EMAIL_SETTINGS_INVALID'; end if;
    foreach v_mail_kind in array array['INITIAL','REMINDER','RENEWAL','WITHDRAWAL','CANCELLATION'] loop
      v_template:=p_templates->v_submission_type->v_mail_kind;
      if pg_catalog.jsonb_typeof(v_template)<>'object'
         or (select pg_catalog.count(*) from pg_catalog.jsonb_object_keys(v_template))<>5
         or exists(select 1 from pg_catalog.jsonb_object_keys(v_template) k
                   where k not in ('subject','body_text','body_html','button_text','include_link'))
         or pg_catalog.char_length(pg_catalog.btrim(coalesce(v_template->>'subject',''))) not between 1 and 240
         or pg_catalog.char_length(pg_catalog.btrim(coalesce(v_template->>'body_text',''))) not between 1 and 20000
         or pg_catalog.char_length(pg_catalog.btrim(coalesce(v_template->>'body_html',''))) not between 1 and 50000
         or (v_template->>'subject')~*'(\{\{|\}\}|https?://)'
         or (v_template->>'body_text')~*'(\{\{|\}\}|https?://)'
         or pg_catalog.jsonb_typeof(v_template->'subject')<>'string'
         or pg_catalog.jsonb_typeof(v_template->'body_text')<>'string'
         or pg_catalog.jsonb_typeof(v_template->'body_html')<>'string'
         or (v_template->>'body_html')~*'(<(a|img|svg|iframe|form|script|style)([[:space:]>])|(^|[^a-z])(href|src|on[a-z]+)[[:space:]]*=|https?://)'
         or (case
              when pg_catalog.jsonb_typeof(v_template->'include_link')<>'boolean' then true
              when v_mail_kind in ('INITIAL','REMINDER','RENEWAL') then
                coalesce((v_template->>'include_link')::boolean,false) is not true
                or pg_catalog.jsonb_typeof(v_template->'button_text')<>'string'
                or pg_catalog.char_length(pg_catalog.btrim(coalesce(v_template->>'button_text',''))) not between 1 and 80
              else
                coalesce((v_template->>'include_link')::boolean,true) is not false
                or pg_catalog.jsonb_typeof(v_template->'button_text')<>'null'
            end)
      then raise exception using errcode='22023',message='CANDIDATE_MANAGER_EMAIL_SETTINGS_INVALID'; end if;
    end loop;
  end loop;

  v_hash:=extensions.digest(pg_catalog.convert_to(p_templates::text,'UTF8'),'sha256');
  v_actor:=pg_catalog.decode(p_actor_identity_hmac_hex,'hex');
  select * into v_existing from public.candidate_manager_email_template_versions
    where idempotency_key=p_idempotency_key;
  if found then
    if v_existing.semantic_sha256<>v_hash then
      raise exception using errcode='40001',message='CANDIDATE_MANAGER_EMAIL_SETTINGS_IDEMPOTENCY_CONFLICT';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'templates',v_existing.templates_json,'version',v_existing.version,
      'sanitizer_policy_version',v_existing.sanitizer_policy_version,
      'semantic_sha256_hex',pg_catalog.encode(v_existing.semantic_sha256,'hex'),
      'updated_at_utc',v_existing.recorded_at_utc,'idempotent_replay',true
    );
  end if;
  update public.settings_defaults set
    candidate_manager_email_templates_json=p_templates,
    candidate_manager_email_templates_version=candidate_manager_email_templates_version+1,
    candidate_manager_email_sanitizer_policy_version=p_sanitizer_policy_version,
    candidate_manager_email_templates_sha256=v_hash,
    candidate_manager_email_templates_updated_at_utc=p_now_utc,
    candidate_manager_email_templates_updated_by_hmac=v_actor
  where id=1 and candidate_manager_email_templates_version=p_expected_version
  returning candidate_manager_email_templates_version into v_version;
  get diagnostics v_changed=row_count;
  if v_changed<>1 then raise exception using errcode='40001',message='MYTMS_SETTINGS_VERSION_CONFLICT'; end if;
  insert into public.candidate_manager_email_template_versions(
    version,templates_json,sanitizer_policy_version,semantic_sha256,
    actor_identity_hmac,idempotency_key,reason_code,recorded_at_utc
  ) values (
    v_version,p_templates,p_sanitizer_policy_version,v_hash,
    v_actor,p_idempotency_key,'OFFICE_SAVE',p_now_utc
  );
  return public.candidate_manager_email_settings_get_v1()||pg_catalog.jsonb_build_object('idempotent_replay',false);
end;
$function$;

create or replace function public.candidate_manager_email_settings_reset_v1(
  p_expected_version bigint,
  p_actor_identity_hmac_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
) returns jsonb
language plpgsql
security definer
set search_path = ''
as $function$
declare
  v_defaults public.candidate_manager_email_template_versions%rowtype;
  v_result jsonb;
begin
  select * into strict v_defaults from public.candidate_manager_email_template_versions where version=1;
  v_result:=public.candidate_manager_email_settings_set_v1(
    p_expected_version,v_defaults.templates_json,v_defaults.sanitizer_policy_version,
    p_actor_identity_hmac_hex,p_idempotency_key,p_now_utc
  );
  update public.candidate_manager_email_template_versions set reason_code='OFFICE_RESET'
    where idempotency_key=p_idempotency_key and reason_code='OFFICE_SAVE';
  return v_result;
end;
$function$;

alter function private._candidate_manager_terminal_mail_payload_v1(jsonb,text) owner to postgres;
alter function public.candidate_manager_email_route_receipt_commit_v1(text,uuid,uuid,integer,integer,uuid,text,uuid,bigint,text,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_manager_email_route_receipt_retire_v1(uuid,uuid,text,timestamptz) owner to postgres;
alter function public.candidate_manager_email_settings_get_v1() owner to postgres;
alter function public.candidate_manager_email_settings_set_v1(bigint,jsonb,text,text,text,timestamptz) owner to postgres;
alter function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz) owner to postgres;

revoke all on function public.candidate_manager_email_route_receipt_commit_v1(text,uuid,uuid,integer,integer,uuid,text,uuid,bigint,text,text,text,text,timestamptz) from public,anon,authenticated;
revoke all on function private._candidate_manager_terminal_mail_payload_v1(jsonb,text) from public,anon,authenticated,service_role;
revoke all on function public.candidate_manager_email_route_receipt_retire_v1(uuid,uuid,text,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_manager_email_settings_get_v1() from public,anon,authenticated;
revoke all on function public.candidate_manager_email_settings_set_v1(bigint,jsonb,text,text,text,timestamptz) from public,anon,authenticated;
revoke all on function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz) from public,anon,authenticated;
grant execute on function public.candidate_manager_email_route_receipt_commit_v1(text,uuid,uuid,integer,integer,uuid,text,uuid,bigint,text,text,text,text,timestamptz) to service_role;
grant execute on function public.candidate_manager_email_route_receipt_retire_v1(uuid,uuid,text,timestamptz) to service_role;
grant execute on function public.candidate_manager_email_settings_get_v1() to service_role;
grant execute on function public.candidate_manager_email_settings_set_v1(bigint,jsonb,text,text,text,timestamptz) to service_role;
grant execute on function public.candidate_manager_email_settings_reset_v1(bigint,text,text,timestamptz) to service_role;
