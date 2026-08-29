begin;

create or replace function private._candidate_manager_authoriser_email_array_v2(p_value jsonb)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path=''
as $function$
declare
  v_result jsonb;
  v_count integer;
begin
  if p_value is not null and pg_catalog.jsonb_typeof(p_value)<>'array' then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_EMAILS_INVALID';
  end if;
  select pg_catalog.count(*),coalesce(pg_catalog.jsonb_agg(v.email order by v.email),'[]'::jsonb)
    into v_count,v_result
  from (
    select distinct private._candidate_normalize_email(e.value) as email
    from pg_catalog.jsonb_array_elements_text(coalesce(p_value,'[]'::jsonb)) e
  ) v
  where v.email<>'';
  if v_count>50 or exists (
    select 1
    from pg_catalog.jsonb_array_elements_text(v_result) e
    where pg_catalog.char_length(e.value)>320
       or e.value!~'^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$'
  ) then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_EMAIL_INVALID';
  end if;
  return v_result;
end;
$function$;

create or replace function private._candidate_manager_authoriser_domain_array_v2(p_value jsonb)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path=''
as $function$
declare
  v_result jsonb;
  v_count integer;
begin
  if p_value is not null and pg_catalog.jsonb_typeof(p_value)<>'array' then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_DOMAINS_INVALID';
  end if;
  select pg_catalog.count(*),coalesce(pg_catalog.jsonb_agg(v.domain order by v.domain),'[]'::jsonb)
    into v_count,v_result
  from (
    select distinct private._candidate_normalize_domain_v1(e.value) as domain
    from pg_catalog.jsonb_array_elements_text(coalesce(p_value,'[]'::jsonb)) e
  ) v
  where v.domain<>'';
  if v_count>50 or exists (
    select 1
    from pg_catalog.jsonb_array_elements_text(v_result) e
    where pg_catalog.char_length(e.value)>253
       or e.value!~'^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$'
  ) then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_APPROVED_DOMAIN_INVALID';
  end if;
  return v_result;
end;
$function$;

create or replace function private._candidate_manager_authoriser_policy_normalize_v2(
  p_policy jsonb,
  p_scope text default 'EFFECTIVE'
)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path=''
as $function$
declare
  v_source jsonb:=case when pg_catalog.jsonb_typeof(p_policy)='object' then p_policy else '{}'::jsonb end;
  v_scope text:=pg_catalog.upper(pg_catalog.btrim(coalesce(p_scope,'EFFECTIVE')));
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_source->>'mode','INHERIT')));
  v_emails jsonb;
  v_domains jsonb;
  v_allow_free boolean:=false;
begin
  if v_scope not in ('CLIENT','CONTRACT_WRITE','EFFECTIVE') then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_POLICY_SCOPE_INVALID';
  end if;
  if v_scope='CLIENT' and exists (
    select 1 from pg_catalog.jsonb_object_keys(v_source) k
    where k not in ('approved_emails','approved_domains','allow_free_business_email')
  ) then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_CLIENT_POLICY_FIELD_INVALID';
  end if;
  if v_scope='CONTRACT_WRITE' and exists (
    select 1 from pg_catalog.jsonb_object_keys(v_source) k
    where k not in ('mode','approved_emails','approved_domains')
  ) then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_CONTRACT_POLICY_FIELD_INVALID';
  end if;
  v_emails:=private._candidate_manager_authoriser_email_array_v2(v_source->'approved_emails');
  v_domains:=private._candidate_manager_authoriser_domain_array_v2(v_source->'approved_domains');
  if v_source ? 'allow_free_business_email' then
    if pg_catalog.jsonb_typeof(v_source->'allow_free_business_email')<>'boolean' then
      raise exception using errcode='22023',message='CANDIDATE_MANAGER_FREE_EMAIL_SETTING_INVALID';
    end if;
    v_allow_free:=(v_source->>'allow_free_business_email')::boolean;
  end if;

  if v_scope='CLIENT' then
    return pg_catalog.jsonb_build_object(
      'approved_emails',v_emails,'approved_domains',v_domains,
      'allow_free_business_email',v_allow_free
    );
  elsif v_scope='CONTRACT_WRITE' then
    if v_mode not in ('INHERIT','EXTEND','CONTRACT_ONLY') then
      raise exception using errcode='22023',message='CANDIDATE_MANAGER_CONTRACT_MODE_INVALID';
    end if;
    if v_mode='INHERIT' and (pg_catalog.jsonb_array_length(v_emails)>0 or pg_catalog.jsonb_array_length(v_domains)>0) then
      raise exception using errcode='22023',message='CANDIDATE_MANAGER_INHERIT_HAS_ADDITIONS';
    end if;
    if v_mode='CONTRACT_ONLY' and pg_catalog.jsonb_array_length(v_emails)=0 and pg_catalog.jsonb_array_length(v_domains)=0 then
      raise exception using errcode='22023',message='CANDIDATE_MANAGER_CONTRACT_ONLY_EMPTY';
    end if;
    return pg_catalog.jsonb_build_object(
      'mode',v_mode,'approved_emails',v_emails,'approved_domains',v_domains
    );
  end if;

  return pg_catalog.jsonb_build_object(
    'approved_emails',v_emails,'approved_domains',v_domains,
    'allow_free_business_email',v_allow_free
  );
end;
$function$;

create or replace function private._candidate_normalize_manager_policy_v1(p_policy jsonb)
returns jsonb
language sql
immutable
parallel safe
set search_path=''
as $function$
  select private._candidate_manager_authoriser_policy_normalize_v2(p_policy,'EFFECTIVE')
$function$;

create or replace function private._candidate_manager_authoriser_effective_v2(
  p_client_policy jsonb,
  p_contract_policy jsonb default null
)
returns jsonb
language plpgsql
immutable
parallel safe
set search_path=''
as $function$
declare
  v_client jsonb:=private._candidate_manager_authoriser_policy_normalize_v2(p_client_policy,'CLIENT');
  v_contract_source jsonb:=case when pg_catalog.jsonb_typeof(p_contract_policy)='object' then p_contract_policy else '{"mode":"INHERIT"}'::jsonb end;
  v_mode text:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_contract_source->>'mode','INHERIT')));
  v_contract jsonb;
  v_emails jsonb;
  v_domains jsonb;
  v_allow_free boolean;
  v_source_mode text;
begin
  if p_contract_policy is null or v_mode='INHERIT' then
    return v_client||pg_catalog.jsonb_build_object('source_mode','INHERIT');
  end if;

  if v_mode in ('EXTEND','CONTRACT_ONLY') then
    v_contract:=private._candidate_manager_authoriser_policy_normalize_v2(v_contract_source,'CONTRACT_WRITE');
  else
    -- Compatibility: the former resolver treated every non-INHERIT value as a
    -- complete replacement. Preserve that meaning without rewriting stored rows.
    v_contract:=private._candidate_manager_authoriser_policy_normalize_v2(v_contract_source,'EFFECTIVE');
  end if;

  if v_mode='EXTEND' then
    select coalesce(pg_catalog.jsonb_agg(x.value order by x.value),'[]'::jsonb)
      into v_emails
    from (
      select distinct value
      from pg_catalog.jsonb_array_elements_text((v_client->'approved_emails')||(v_contract->'approved_emails'))
    ) x;
    select coalesce(pg_catalog.jsonb_agg(x.value order by x.value),'[]'::jsonb)
      into v_domains
    from (
      select distinct value
      from pg_catalog.jsonb_array_elements_text((v_client->'approved_domains')||(v_contract->'approved_domains'))
    ) x;
    v_allow_free:=coalesce((v_client->>'allow_free_business_email')::boolean,false);
    v_source_mode:='EXTEND';
  elsif v_mode='CONTRACT_ONLY' then
    v_emails:=v_contract->'approved_emails';
    v_domains:=v_contract->'approved_domains';
    v_allow_free:=false;
    v_source_mode:='CONTRACT_ONLY';
  else
    v_emails:=v_contract->'approved_emails';
    v_domains:=v_contract->'approved_domains';
    v_allow_free:=coalesce((v_contract->>'allow_free_business_email')::boolean,false);
    v_source_mode:='LEGACY_REPLACEMENT';
  end if;
  return pg_catalog.jsonb_build_object(
    'approved_emails',v_emails,'approved_domains',v_domains,
    'allow_free_business_email',v_allow_free,'source_mode',v_source_mode
  );
end;
$function$;

create or replace function private._candidate_manager_email_allowed_v1(
  p_policy jsonb,
  p_email text,
  p_barred_domains jsonb
)
returns jsonb
language plpgsql
immutable
set search_path=''
as $function$
declare
  v_policy jsonb:=private._candidate_manager_authoriser_policy_normalize_v2(p_policy,'EFFECTIVE');
  v_email text:=private._candidate_normalize_email(p_email);
  v_domain text;
  v_full boolean:=false;
  v_domain_allowed boolean:=false;
  v_barred boolean:=false;
  v_barred_policy_ready boolean:=pg_catalog.jsonb_typeof(p_barred_domains)='array'
    and pg_catalog.jsonb_array_length(p_barred_domains)>0;
  v_free boolean:=coalesce((v_policy->>'allow_free_business_email')::boolean,false);
begin
  if v_email!~'^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
    return pg_catalog.jsonb_build_object('allowed',false,'reason_code','MANAGER_EMAIL_INVALID');
  end if;
  v_domain:=pg_catalog.split_part(v_email,'@',2);
  select exists(select 1 from pg_catalog.jsonb_array_elements_text(v_policy->'approved_emails') x where x.value=v_email) into v_full;
  select exists(select 1 from pg_catalog.jsonb_array_elements_text(v_policy->'approved_domains') x where x.value=v_domain) into v_domain_allowed;
  select exists(select 1 from pg_catalog.jsonb_array_elements_text(coalesce(p_barred_domains,'[]'::jsonb)) x
    where private._candidate_normalize_domain_v1(x.value)=v_domain) into v_barred;
  return pg_catalog.jsonb_build_object(
    'allowed',v_full or v_domain_allowed or (v_free and v_barred_policy_ready and not v_barred),
    'reason_code',case
      when v_full then 'APPROVED_EMAIL'
      when v_domain_allowed then 'APPROVED_DOMAIN'
      when v_free and not v_barred_policy_ready then 'BARRED_DOMAIN_POLICY_NOT_CONFIGURED'
      when v_free and not v_barred then 'FREE_BUSINESS_EMAIL'
      when v_barred then 'BARRED_DOMAIN'
      else 'MANAGER_EMAIL_NOT_APPROVED' end,
    'email_normalized',v_email,'domain',v_domain
  );
end;
$function$;

create or replace function private._candidate_policy_resolve_v1(
  p_client_id uuid,
  p_contract_id uuid default null,
  p_evaluation_date date default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_evaluation_date date:=coalesce(p_evaluation_date,(pg_catalog.transaction_timestamp() at time zone 'Europe/London')::date);
  v_global public.settings_defaults%rowtype;
  v_client public.client_settings%rowtype;
  v_contract public.contracts%rowtype;
  v_client_found boolean:=false;
  v_contract_found boolean:=false;
  v_auto boolean;
  v_auto_source text;
  v_separate boolean;
  v_separate_source text;
  v_paper boolean;
  v_paper_source text;
  v_expense_email text;
  v_import_mandatory boolean:=false;
  v_expense_email_ready boolean:=false;
  v_manager_policy jsonb;
  v_result jsonb;
  v_import_authority jsonb;
begin
  if p_client_id is null then raise exception using errcode='22023',message='CANDIDATE_POLICY_CLIENT_REQUIRED'; end if;
  select * into v_global from public.settings_defaults where id=1;
  if not found then raise exception using errcode='55000',message='CANDIDATE_GLOBAL_SETTINGS_MISSING'; end if;
  select * into v_client from public.client_settings cs
   where cs.client_id=p_client_id and (cs.effective_from is null or cs.effective_from<=v_evaluation_date)
   order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1;
  v_client_found:=found;
  if p_contract_id is not null then
    select * into v_contract from public.contracts c where c.id=p_contract_id;
    v_contract_found:=found;
    if not v_contract_found then raise exception using errcode='P0002',message='CANDIDATE_POLICY_CONTRACT_NOT_FOUND'; end if;
    if v_contract.client_id is distinct from p_client_id then raise exception using errcode='22023',message='CANDIDATE_POLICY_CONTRACT_CLIENT_MISMATCH'; end if;
  end if;
  v_import_authority:=private._candidate_import_authoritative_v1(p_client_id,p_contract_id,null,null,v_evaluation_date);
  v_import_mandatory:=coalesce((v_import_authority->>'is_import_authoritative')::boolean,false);
  if v_contract_found and v_contract.candidate_electronic_auto_authorise_override is not null then
    v_auto:=v_contract.candidate_electronic_auto_authorise_override; v_auto_source:='CONTRACT';
  elsif v_client_found and v_client.candidate_electronic_auto_authorise is not null then
    v_auto:=v_client.candidate_electronic_auto_authorise; v_auto_source:='CLIENT';
  else v_auto:=v_global.candidate_electronic_auto_authorise_default; v_auto_source:='GLOBAL'; end if;
  if v_import_mandatory then v_separate:=true; v_separate_source:='IMPORT_MANDATORY';
  elsif v_contract_found and v_contract.candidate_expenses_require_separate_timesheet_override is not null then
    v_separate:=v_contract.candidate_expenses_require_separate_timesheet_override; v_separate_source:='CONTRACT';
  elsif v_client_found then v_separate:=v_client.candidate_expenses_require_separate_timesheet; v_separate_source:='CLIENT';
  else v_separate:=false; v_separate_source:='SAFE_DEFAULT'; end if;
  if v_contract_found and v_contract.candidate_paper_submission_enabled_override is not null then
    v_paper:=v_contract.candidate_paper_submission_enabled_override; v_paper_source:='CONTRACT';
  elsif v_client_found then v_paper:=v_client.candidate_paper_submission_enabled; v_paper_source:='CLIENT';
  else v_paper:=false; v_paper_source:='SAFE_DEFAULT'; end if;
  v_expense_email:=nullif(pg_catalog.btrim(case
    when v_contract_found and nullif(pg_catalog.btrim(v_contract.candidate_expense_invoice_email_override),'') is not null
      then v_contract.candidate_expense_invoice_email_override
    when v_client_found then v_client.candidate_expense_invoice_email else null end),'');
  v_expense_email_ready:=v_expense_email is not null and pg_catalog.char_length(v_expense_email)<=320
    and v_expense_email~'^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$';
  v_manager_policy:=private._candidate_manager_authoriser_effective_v2(
    case when v_client_found then v_client.candidate_manager_approval_policy_json else '{}'::jsonb end,
    case when v_contract_found then v_contract.candidate_manager_approval_policy_json else null end
  );
  v_result:=pg_catalog.jsonb_build_object(
    'client_id',p_client_id,'contract_id',p_contract_id,'evaluation_date',v_evaluation_date,
    'candidate_electronic_auto_authorise',v_auto,'candidate_electronic_auto_authorise_source',v_auto_source,
    'expenses_require_separate_timesheet',v_separate,'expenses_require_separate_timesheet_source',v_separate_source,
    'import_expense_separation_mandatory',v_import_mandatory,'import_source_family',v_import_authority->>'source_family',
    'paper_submission_enabled',v_paper,'paper_submission_enabled_source',v_paper_source,
    'expense_invoice_email',v_expense_email,'expense_invoice_email_ready',v_expense_email_ready,
    'manager_approval_policy',v_manager_policy,
    'allow_daily_manager_authorise_on_phone',coalesce(v_client.allow_daily_manager_authorise_on_phone,true),
    'allow_daily_manager_authorise_by_email',coalesce(v_client.allow_daily_manager_authorise_by_email,false),
    'hours_deviation_pct',v_global.candidate_hours_deviation_pct,
    'barred_manager_email_domains',private._candidate_normalize_domain_array_v1(v_global.candidate_barred_manager_email_domains),
    'client_setting_found',v_client_found,'client_settings_id',case when v_client_found then v_client.id else null end,
    'contract_found',v_contract_found,'global_settings_updated_at',v_global.updated_at
  );
  return v_result||pg_catalog.jsonb_build_object(
    'policy_fingerprint',pg_catalog.encode(extensions.digest(pg_catalog.convert_to(v_result::text,'UTF8'),'sha256'),'hex')
  );
end;
$function$;

create or replace function public.client_manager_authoriser_policy_get_v1(p_client_id uuid)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_client public.clients%rowtype;
  v_settings public.client_settings%rowtype;
  v_policy jsonb;
begin
  select * into v_client from public.clients c where c.id=p_client_id;
  if not found then raise exception using errcode='P0002',message='CLIENT_NOT_FOUND'; end if;
  select * into v_settings from public.client_settings cs where cs.client_id=p_client_id
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1;
  if not found then raise exception using errcode='55000',message='CLIENT_SETTINGS_NOT_FOUND'; end if;
  v_policy:=private._candidate_manager_authoriser_policy_normalize_v2(v_settings.candidate_manager_approval_policy_json,'CLIENT');
  return pg_catalog.jsonb_build_object(
    'ok',true,'client_name',v_client.name,'client_rev',v_client.rev,
    'settings_updated_at',v_settings.updated_at,'policy',v_policy,
    'approved_email_count',pg_catalog.jsonb_array_length(v_policy->'approved_emails'),
    'approved_domain_count',pg_catalog.jsonb_array_length(v_policy->'approved_domains'),
    'usable',coalesce((v_policy->>'allow_free_business_email')::boolean,false)
      or pg_catalog.jsonb_array_length(v_policy->'approved_emails')>0
      or pg_catalog.jsonb_array_length(v_policy->'approved_domains')>0
  );
end;
$function$;

create or replace function public.contract_manager_authoriser_policy_get_v1(p_contract_id uuid)
returns jsonb
language plpgsql
stable
security definer
set search_path=''
as $function$
declare
  v_contract public.contracts%rowtype;
  v_client public.clients%rowtype;
  v_client_settings public.client_settings%rowtype;
  v_contract_policy jsonb;
  v_client_policy jsonb;
  v_effective jsonb;
begin
  select * into v_contract from public.contracts c where c.id=p_contract_id;
  if not found then raise exception using errcode='P0002',message='CONTRACT_NOT_FOUND'; end if;
  select * into v_client from public.clients c where c.id=v_contract.client_id;
  select * into v_client_settings from public.client_settings cs where cs.client_id=v_contract.client_id
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1;
  if not found then raise exception using errcode='55000',message='CLIENT_SETTINGS_NOT_FOUND'; end if;
  v_client_policy:=private._candidate_manager_authoriser_policy_normalize_v2(v_client_settings.candidate_manager_approval_policy_json,'CLIENT');
  if pg_catalog.upper(coalesce(v_contract.candidate_manager_approval_policy_json->>'mode','INHERIT')) in ('INHERIT','EXTEND','CONTRACT_ONLY') then
    v_contract_policy:=private._candidate_manager_authoriser_policy_normalize_v2(v_contract.candidate_manager_approval_policy_json,'CONTRACT_WRITE');
  else
    v_contract_policy:=v_contract.candidate_manager_approval_policy_json;
  end if;
  v_effective:=private._candidate_manager_authoriser_effective_v2(v_client_settings.candidate_manager_approval_policy_json,v_contract.candidate_manager_approval_policy_json);
  return pg_catalog.jsonb_build_object(
    'ok',true,'contract_updated_at',v_contract.updated_at,'client_name',v_client.name,
    'contract_policy',v_contract_policy,'client_policy',v_client_policy,'effective_policy',v_effective,
    'client_approved_count',pg_catalog.jsonb_array_length(v_client_policy->'approved_emails')+pg_catalog.jsonb_array_length(v_client_policy->'approved_domains'),
    'contract_approved_count',pg_catalog.jsonb_array_length(v_contract_policy->'approved_emails')+pg_catalog.jsonb_array_length(v_contract_policy->'approved_domains'),
    'effective_approved_count',pg_catalog.jsonb_array_length(v_effective->'approved_emails')+pg_catalog.jsonb_array_length(v_effective->'approved_domains')
  );
end;
$function$;

create or replace function public.client_manager_authoriser_policy_update_v1(
  p_client_id uuid,
  p_expected_settings_updated_at timestamptz,
  p_policy jsonb,
  p_actor_user_id uuid,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_settings public.client_settings%rowtype;
  v_policy jsonb:=private._candidate_manager_authoriser_policy_normalize_v2(p_policy,'CLIENT');
  v_hash bytea;
  v_existing public.candidate_manager_authoriser_policy_receipts%rowtype;
  v_response jsonb;
begin
  if p_client_id is null or p_expected_settings_updated_at is null or p_actor_user_id is null
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_idempotency_key,''))) not between 16 and 200 then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_AUTHORISER_REQUEST_INVALID';
  end if;
  if not coalesce((v_policy->>'allow_free_business_email')::boolean,false)
     and pg_catalog.jsonb_array_length(v_policy->'approved_emails')=0
     and pg_catalog.jsonb_array_length(v_policy->'approved_domains')=0 then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_RESTRICTED_POLICY_EMPTY';
  end if;
  v_hash:=extensions.digest(pg_catalog.convert_to(v_policy::text,'UTF8'),'sha256');
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('CLIENT:'||p_client_id::text||':'||p_idempotency_key,0));
  select * into v_existing from public.candidate_manager_authoriser_policy_receipts r
    where r.entity_kind='CLIENT' and r.entity_id=p_client_id and r.idempotency_key=p_idempotency_key;
  if found then
    if v_existing.semantic_sha256<>v_hash then raise exception using errcode='40001',message='CANDIDATE_MANAGER_AUTHORISER_IDEMPOTENCY_CONFLICT'; end if;
    return v_existing.response_json||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;
  select * into v_settings from public.client_settings cs where cs.client_id=p_client_id
    order by cs.effective_from desc nulls last,cs.updated_at desc,cs.id desc limit 1 for update;
  if not found then raise exception using errcode='P0002',message='CLIENT_SETTINGS_NOT_FOUND'; end if;
  if v_settings.updated_at is distinct from p_expected_settings_updated_at then
    raise exception using errcode='40001',message='CANDIDATE_MANAGER_AUTHORISER_VERSION_CONFLICT';
  end if;
  update public.client_settings set candidate_manager_approval_policy_json=v_policy,updated_at=p_now_utc where id=v_settings.id;
  v_response:=public.client_manager_authoriser_policy_get_v1(p_client_id)||pg_catalog.jsonb_build_object('idempotent_replay',false);
  insert into public.candidate_manager_authoriser_policy_receipts(entity_kind,entity_id,idempotency_key,semantic_sha256,response_json,actor_user_id,recorded_at_utc)
  values ('CLIENT',p_client_id,p_idempotency_key,v_hash,v_response,p_actor_user_id,p_now_utc);
  return v_response;
end;
$function$;

create or replace function public.contract_manager_authoriser_policy_update_v1(
  p_contract_id uuid,
  p_expected_contract_updated_at timestamptz,
  p_policy jsonb,
  p_actor_user_id uuid,
  p_idempotency_key text,
  p_now_utc timestamptz default pg_catalog.transaction_timestamp()
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare
  v_contract public.contracts%rowtype;
  v_policy jsonb:=private._candidate_manager_authoriser_policy_normalize_v2(p_policy,'CONTRACT_WRITE');
  v_hash bytea;
  v_existing public.candidate_manager_authoriser_policy_receipts%rowtype;
  v_response jsonb;
begin
  if p_contract_id is null or p_expected_contract_updated_at is null or p_actor_user_id is null
     or pg_catalog.char_length(pg_catalog.btrim(coalesce(p_idempotency_key,''))) not between 16 and 200 then
    raise exception using errcode='22023',message='CANDIDATE_MANAGER_AUTHORISER_REQUEST_INVALID';
  end if;
  v_hash:=extensions.digest(pg_catalog.convert_to(v_policy::text,'UTF8'),'sha256');
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('CONTRACT:'||p_contract_id::text||':'||p_idempotency_key,0));
  select * into v_existing from public.candidate_manager_authoriser_policy_receipts r
    where r.entity_kind='CONTRACT' and r.entity_id=p_contract_id and r.idempotency_key=p_idempotency_key;
  if found then
    if v_existing.semantic_sha256<>v_hash then raise exception using errcode='40001',message='CANDIDATE_MANAGER_AUTHORISER_IDEMPOTENCY_CONFLICT'; end if;
    return v_existing.response_json||pg_catalog.jsonb_build_object('idempotent_replay',true);
  end if;
  select * into v_contract from public.contracts c where c.id=p_contract_id for update;
  if not found then raise exception using errcode='P0002',message='CONTRACT_NOT_FOUND'; end if;
  if v_contract.updated_at is distinct from p_expected_contract_updated_at then
    raise exception using errcode='40001',message='CANDIDATE_MANAGER_AUTHORISER_VERSION_CONFLICT';
  end if;
  update public.contracts set candidate_manager_approval_policy_json=v_policy,updated_at=p_now_utc where id=p_contract_id;
  v_response:=public.contract_manager_authoriser_policy_get_v1(p_contract_id)||pg_catalog.jsonb_build_object('idempotent_replay',false);
  insert into public.candidate_manager_authoriser_policy_receipts(entity_kind,entity_id,idempotency_key,semantic_sha256,response_json,actor_user_id,recorded_at_utc)
  values ('CONTRACT',p_contract_id,p_idempotency_key,v_hash,v_response,p_actor_user_id,p_now_utc);
  return v_response;
end;
$function$;

alter function private._candidate_manager_authoriser_email_array_v2(jsonb) owner to postgres;
alter function private._candidate_manager_authoriser_domain_array_v2(jsonb) owner to postgres;
alter function private._candidate_manager_authoriser_policy_normalize_v2(jsonb,text) owner to postgres;
alter function private._candidate_manager_authoriser_effective_v2(jsonb,jsonb) owner to postgres;
alter function private._candidate_normalize_manager_policy_v1(jsonb) owner to postgres;
alter function private._candidate_manager_email_allowed_v1(jsonb,text,jsonb) owner to postgres;
alter function private._candidate_policy_resolve_v1(uuid,uuid,date) owner to postgres;
alter function public.client_manager_authoriser_policy_get_v1(uuid) owner to postgres;
alter function public.contract_manager_authoriser_policy_get_v1(uuid) owner to postgres;
alter function public.client_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) owner to postgres;
alter function public.contract_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) owner to postgres;

revoke all on function private._candidate_manager_authoriser_email_array_v2(jsonb) from public,anon,authenticated;
revoke all on function private._candidate_manager_authoriser_domain_array_v2(jsonb) from public,anon,authenticated;
revoke all on function private._candidate_manager_authoriser_policy_normalize_v2(jsonb,text) from public,anon,authenticated;
revoke all on function private._candidate_manager_authoriser_effective_v2(jsonb,jsonb) from public,anon,authenticated;
revoke all on function private._candidate_normalize_manager_policy_v1(jsonb) from public,anon,authenticated;
revoke all on function private._candidate_manager_email_allowed_v1(jsonb,text,jsonb) from public,anon,authenticated;
revoke all on function private._candidate_policy_resolve_v1(uuid,uuid,date) from public,anon,authenticated;
revoke all on function public.client_manager_authoriser_policy_get_v1(uuid) from public,anon,authenticated;
revoke all on function public.contract_manager_authoriser_policy_get_v1(uuid) from public,anon,authenticated;
revoke all on function public.client_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) from public,anon,authenticated;
revoke all on function public.contract_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) from public,anon,authenticated;
grant execute on function public.client_manager_authoriser_policy_get_v1(uuid) to service_role;
grant execute on function public.contract_manager_authoriser_policy_get_v1(uuid) to service_role;
grant execute on function public.client_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) to service_role;
grant execute on function public.contract_manager_authoriser_policy_update_v1(uuid,timestamptz,jsonb,uuid,text,timestamptz) to service_role;
grant execute on function private._candidate_policy_resolve_v1(uuid,uuid,date) to service_role;

-- New Office RPCs must become callable through the already-running Miget
-- PostgREST service as part of the same protected release.  The notification is
-- delivered only after this transaction commits, so the cache cannot advertise
-- a definition that was subsequently rolled back.
notify pgrst, 'reload schema';

commit;
