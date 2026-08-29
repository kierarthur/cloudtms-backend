-- Immutable CloudTMS TEST function snapshot, page 05.
-- Generated from pg_get_functiondef; definitions only, with function body checks deferred for forward references.
-- Do not edit an applied baseline page. Add or replace routine authority in supabase/repeatable.

\set ON_ERROR_STOP on
set check_function_bodies = off;
set search_path = pg_catalog, public, extensions;

-- client_picker_search(text,integer,integer,boolean,boolean)
CREATE OR REPLACE FUNCTION public.client_picker_search(p_query text, p_limit integer DEFAULT 25, p_offset integer DEFAULT 0, p_nhsp_only boolean DEFAULT false, p_hr_auto_only boolean DEFAULT false)
 RETURNS TABLE(id uuid, name text, cli_ref text, primary_invoice_email text, is_nhsp boolean, autoprocess_hr boolean, rev bigint, updated_at timestamp with time zone, match_rank integer)
 LANGUAGE plpgsql
 STABLE
AS $function$
declare
  v_query_raw text := coalesce(p_query, '');
  v_query text := btrim(v_query_raw);
  v_query_lc text := lower(btrim(v_query_raw));
  v_limit integer := greatest(1, least(coalesce(p_limit, 25), 100));
  v_offset integer := greatest(coalesce(p_offset, 0), 0);
begin
  if v_query = '' or char_length(v_query) < 2 then
    return;
  end if;

  return query
  with latest_settings as (
    select
      cs.client_id,
      cs.is_nhsp,
      cs.autoprocess_hr,
      row_number() over (
        partition by cs.client_id
        order by
          cs.effective_from desc nulls last,
          cs.updated_at desc nulls last,
          cs.created_at desc nulls last,
          cs.id desc
      ) as rn
    from public.client_settings as cs
  ),
  base as (
    select
      c.id,
      c.name,
      c.cli_ref,
      c.primary_invoice_email,
      coalesce(ls.is_nhsp, false) as is_nhsp,
      coalesce(ls.autoprocess_hr, false) as autoprocess_hr,
      c.rev,
      c.updated_at
    from public.clients as c
    left join latest_settings as ls
      on ls.client_id = c.id
     and ls.rn = 1
    where
      (p_nhsp_only is false or coalesce(ls.is_nhsp, false) is true)
      and (p_hr_auto_only is false or coalesce(ls.autoprocess_hr, false) is true)
      and (
        lower(coalesce(c.cli_ref, '')) = v_query_lc
        or lower(coalesce(c.primary_invoice_email, '')) = v_query_lc
        or lower(coalesce(c.name, '')) = v_query_lc
        or lower(coalesce(c.ap_phone, '')) = v_query_lc
        or lower(coalesce(c.contact_email, '')) = v_query_lc
        or lower(coalesce(c.cli_ref, '')) like v_query_lc || '%'
        or lower(coalesce(c.primary_invoice_email, '')) like v_query_lc || '%'
        or lower(coalesce(c.name, '')) like v_query_lc || '%'
        or lower(coalesce(c.ap_phone, '')) like v_query_lc || '%'
        or lower(coalesce(c.contact_email, '')) like v_query_lc || '%'
        or position(v_query_lc in lower(coalesce(c.cli_ref, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.primary_invoice_email, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.name, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.ap_phone, ''))) > 0
        or position(v_query_lc in lower(coalesce(c.contact_email, ''))) > 0
      )
  ),
  ranked as (
    select
      b.id,
      b.name,
      b.cli_ref,
      b.primary_invoice_email,
      b.is_nhsp,
      b.autoprocess_hr,
      b.rev,
      b.updated_at,
      case
        when lower(coalesce(b.cli_ref, '')) = v_query_lc then 10
        when lower(coalesce(b.primary_invoice_email, '')) = v_query_lc then 20
        when lower(coalesce(b.name, '')) = v_query_lc then 30
        when lower(coalesce(b.cli_ref, '')) like v_query_lc || '%' then 40
        when lower(coalesce(b.primary_invoice_email, '')) like v_query_lc || '%' then 50
        when lower(coalesce(b.name, '')) like v_query_lc || '%' then 60
        when position(v_query_lc in lower(coalesce(b.cli_ref, ''))) > 0 then 70
        when position(v_query_lc in lower(coalesce(b.primary_invoice_email, ''))) > 0 then 80
        when position(v_query_lc in lower(coalesce(b.name, ''))) > 0 then 90
        else 999
      end as match_rank
    from base as b
  )
  select
    r.id,
    r.name,
    r.cli_ref,
    r.primary_invoice_email,
    r.is_nhsp,
    r.autoprocess_hr,
    r.rev,
    r.updated_at,
    r.match_rank
  from ranked as r
  order by
    r.match_rank asc,
    lower(coalesce(r.name, '')) asc,
    r.id asc
  offset v_offset
  limit v_limit;
end;
$function$;

-- client_update_with_settings_v1(uuid,bigint,timestamp with time zone,jsonb,jsonb,uuid,text)
CREATE OR REPLACE FUNCTION public.client_update_with_settings_v1(p_client_id uuid, p_expected_client_rev bigint, p_expected_settings_updated_at timestamp with time zone, p_client_patch jsonb DEFAULT '{}'::jsonb, p_financial_policy_patch jsonb DEFAULT '{}'::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_request_key text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_patch jsonb:=coalesce(p_client_patch,'{}'); v_policy jsonb:=coalesce(p_financial_policy_patch,'{}');
  v_client public.clients%rowtype; v_in public.clients%rowtype; v_settings public.client_settings%rowtype;
  v_settings_in public.client_settings%rowtype; v_global public.settings_defaults%rowtype;
  v_reversal public.correction_financials_date_basis_enum; v_replacement public.correction_financials_date_basis_enum;
  v_eligible boolean; v_unknown text[]; v_key text:=btrim(coalesce(p_request_key,'')); v_old jsonb;
  v_candidate_settings_enabled boolean := private._candidate_feature_enabled_current_v1('candidate_settings');
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_client_id is null or length(v_key) not between 16 and 256 or jsonb_typeof(v_patch)<>'object' or jsonb_typeof(v_policy)<>'object'
    or pg_column_size(v_patch)>65536
    or pg_column_size(v_policy)>(case when v_candidate_settings_enabled then 65536 else 4096 end)
  then raise exception 'CLIENT_UPDATE_INPUT_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(key_name order by key_name),array[]::text[]) into v_unknown
  from jsonb_object_keys(v_patch) as supplied_key(key_name)
  where key_name<>all(array['name','invoice_address','primary_invoice_email','ap_phone','vat_chargeable','payment_terms_days','mileage_charge_rate',
    'ts_queries_email','client_address','contact_title','contact_known_as','contact_forename','contact_surname','contact_job_title','contact_tel',
    'contact_mobile','contact_email','website','notes']::text[]);
  if cardinality(v_unknown)>0 then raise exception 'CLIENT_UPDATE_UNKNOWN_FIELDS' using errcode='22023',detail=to_jsonb(v_unknown)::text; end if;
  select coalesce(array_agg(key_name order by key_name),array[]::text[]) into v_unknown
  from jsonb_object_keys(v_policy) as supplied_key(key_name)
  where key_name<>all(
    array['reversal_complete_financials_date','reversal_replacement_financials_date']::text[]
    || case when v_candidate_settings_enabled then array[
      'candidate_electronic_auto_authorise',
      'candidate_expenses_require_separate_timesheet',
      'candidate_paper_submission_enabled',
      'candidate_expense_invoice_email',
      'candidate_manager_approval_policy_json',
      'allow_daily_manager_authorise_on_phone',
      'allow_daily_manager_authorise_by_email'
    ]::text[] else array[]::text[] end
  );
  if cardinality(v_unknown)>0 then raise exception 'CLIENT_POLICY_UPDATE_UNKNOWN_FIELDS' using errcode='22023',detail=to_jsonb(v_unknown)::text; end if;
  if exists(select 1 from jsonb_each(v_policy)e
    where e.key in('reversal_complete_financials_date','reversal_replacement_financials_date')
      and e.value<>'null'::jsonb
      and upper(trim(both '"' from e.value::text)) not in ('PAID_DATE','NOW')) then
    raise exception 'CLIENT_POLICY_VALUE_INVALID' using errcode='22023'; end if;
  select * into v_client from public.clients where id=p_client_id for update; if not found then raise exception 'CLIENT_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_settings from public.client_settings where client_id=p_client_id order by effective_from desc nulls last,updated_at desc,id desc limit 1 for update;
  if not found then raise exception 'CLIENT_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_global from public.settings_defaults where id=1 for share; if not found then raise exception 'GLOBAL_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;
  if v_client.rev is distinct from p_expected_client_rev or v_settings.updated_at is distinct from p_expected_settings_updated_at then
    raise exception 'CLIENT_UPDATE_VERSION_CONFLICT' using errcode='40001',detail=jsonb_build_object('client_rev',v_client.rev,'settings_updated_at',v_settings.updated_at)::text; end if;
  select * into v_in from jsonb_populate_record(null::public.clients,v_patch);
  if v_candidate_settings_enabled then
    select * into v_settings_in from jsonb_populate_record(null::public.client_settings,v_policy);
    if v_policy?'candidate_manager_approval_policy_json' then
      v_settings_in.candidate_manager_approval_policy_json :=
        private._candidate_normalize_manager_policy_v1(
          v_settings_in.candidate_manager_approval_policy_json
        );
    end if;
  end if;
  if v_candidate_settings_enabled and exists(select 1 from jsonb_each(v_policy)e
      where e.key in('candidate_expenses_require_separate_timesheet',
        'candidate_paper_submission_enabled','candidate_manager_approval_policy_json',
        'allow_daily_manager_authorise_on_phone','allow_daily_manager_authorise_by_email')
        and e.value='null'::jsonb) then
    raise exception 'CLIENT_CANDIDATE_SETTINGS_REQUIRED_FIELD_NULL' using errcode='22023';
  end if;
  if v_candidate_settings_enabled and v_policy?'candidate_manager_approval_policy_json'
      and jsonb_typeof(v_settings_in.candidate_manager_approval_policy_json)<>'object' then
    raise exception 'CLIENT_CANDIDATE_MANAGER_POLICY_INVALID' using errcode='22023';
  end if;
  if v_candidate_settings_enabled
     and coalesce((case
       when v_policy?'candidate_manager_approval_policy_json'
         then v_settings_in.candidate_manager_approval_policy_json
       else v_settings.candidate_manager_approval_policy_json
     end->>'allow_free_business_email')::boolean,false)
     and (
       jsonb_typeof(v_global.candidate_barred_manager_email_domains)<>'array'
       or jsonb_array_length(v_global.candidate_barred_manager_email_domains)=0
     ) then
    raise exception 'CANDIDATE_BARRED_MANAGER_DOMAIN_POLICY_REQUIRED' using errcode='22023';
  end if;
  if v_candidate_settings_enabled and coalesce(case when v_policy?'allow_daily_manager_authorise_on_phone'
        then v_settings_in.allow_daily_manager_authorise_on_phone
        else v_settings.allow_daily_manager_authorise_on_phone end,false)=false
     and coalesce(case when v_policy?'allow_daily_manager_authorise_by_email'
        then v_settings_in.allow_daily_manager_authorise_by_email
        else v_settings.allow_daily_manager_authorise_by_email end,false)=false then
    raise exception 'CLIENT_DAILY_MANAGER_METHOD_REQUIRED' using errcode='22023';
  end if;
  v_reversal:=case when v_policy?'reversal_complete_financials_date' then nullif(upper(v_policy->>'reversal_complete_financials_date'),'')::public.correction_financials_date_basis_enum else v_settings.reversal_complete_financials_date end;
  v_replacement:=case when v_policy?'reversal_replacement_financials_date' then nullif(upper(v_policy->>'reversal_replacement_financials_date'),'')::public.correction_financials_date_basis_enum else v_settings.reversal_replacement_financials_date end;
  v_eligible:=coalesce(v_settings.is_nhsp,false) or (coalesce(v_settings.requires_hr,false) and coalesce(v_settings.no_timesheet_required,false));
  if v_candidate_settings_enabled and v_eligible then
    if not coalesce(case
        when v_policy?'candidate_expenses_require_separate_timesheet'
          then v_settings_in.candidate_expenses_require_separate_timesheet
        else v_settings.candidate_expenses_require_separate_timesheet end,false) then
      raise exception 'CANDIDATE_IMPORT_EXPENSE_SEPARATION_REQUIRED' using errcode='22023';
    end if;
    if nullif(btrim(case
        when v_policy?'candidate_expense_invoice_email'
          then v_settings_in.candidate_expense_invoice_email
        else v_settings.candidate_expense_invoice_email end),'') is null
       or char_length(btrim(case
        when v_policy?'candidate_expense_invoice_email'
          then v_settings_in.candidate_expense_invoice_email
        else v_settings.candidate_expense_invoice_email end))>320
       or btrim(case
        when v_policy?'candidate_expense_invoice_email'
          then v_settings_in.candidate_expense_invoice_email
        else v_settings.candidate_expense_invoice_email end)
          !~ '^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$' then
      raise exception 'CANDIDATE_IMPORT_EXPENSE_EMAIL_REQUIRED' using errcode='22023';
    end if;
  end if;
  if ((v_policy?'reversal_complete_financials_date' and v_reversal is not null)
      or (v_policy?'reversal_replacement_financials_date' and v_replacement is not null)) and not v_eligible then
    raise exception 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT' using errcode='22023'; end if;
  perform public._ctms_assert_import_correction_settings_write_v1(v_settings.is_nhsp,v_settings.requires_hr,v_settings.no_timesheet_required,
    case when v_policy?'reversal_complete_financials_date' then v_reversal end,case when v_policy?'reversal_replacement_financials_date' then v_replacement end);
  v_old:=case when v_candidate_settings_enabled then
    jsonb_build_object('client',to_jsonb(v_client),'settings',jsonb_build_object(
      'reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
      'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date,
      'candidate_electronic_auto_authorise',v_settings.candidate_electronic_auto_authorise,
      'candidate_expenses_require_separate_timesheet',v_settings.candidate_expenses_require_separate_timesheet,
      'candidate_paper_submission_enabled',v_settings.candidate_paper_submission_enabled,
      'candidate_expense_invoice_email',v_settings.candidate_expense_invoice_email,
      'candidate_manager_approval_policy_json',v_settings.candidate_manager_approval_policy_json,
      'allow_daily_manager_authorise_on_phone',v_settings.allow_daily_manager_authorise_on_phone,
      'allow_daily_manager_authorise_by_email',v_settings.allow_daily_manager_authorise_by_email))
    else jsonb_build_object('client',to_jsonb(v_client),
      'reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
      'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date)
  end;
  update public.clients set
    name=case when v_patch?'name' then v_in.name else v_client.name end,
    invoice_address=case when v_patch?'invoice_address' then v_in.invoice_address else v_client.invoice_address end,
    primary_invoice_email=case when v_patch?'primary_invoice_email' then v_in.primary_invoice_email else v_client.primary_invoice_email end,
    ap_phone=case when v_patch?'ap_phone' then v_in.ap_phone else v_client.ap_phone end,
    vat_chargeable=case when v_patch?'vat_chargeable' then v_in.vat_chargeable else v_client.vat_chargeable end,
    payment_terms_days=case when v_patch?'payment_terms_days' then v_in.payment_terms_days else v_client.payment_terms_days end,
    mileage_charge_rate=case when v_patch?'mileage_charge_rate' then v_in.mileage_charge_rate else v_client.mileage_charge_rate end,
    ts_queries_email=case when v_patch?'ts_queries_email' then v_in.ts_queries_email else v_client.ts_queries_email end,
    client_address=case when v_patch?'client_address' then v_in.client_address else v_client.client_address end,
    contact_title=case when v_patch?'contact_title' then v_in.contact_title else v_client.contact_title end,
    contact_known_as=case when v_patch?'contact_known_as' then v_in.contact_known_as else v_client.contact_known_as end,
    contact_forename=case when v_patch?'contact_forename' then v_in.contact_forename else v_client.contact_forename end,
    contact_surname=case when v_patch?'contact_surname' then v_in.contact_surname else v_client.contact_surname end,
    contact_job_title=case when v_patch?'contact_job_title' then v_in.contact_job_title else v_client.contact_job_title end,
    contact_tel=case when v_patch?'contact_tel' then v_in.contact_tel else v_client.contact_tel end,
    contact_mobile=case when v_patch?'contact_mobile' then v_in.contact_mobile else v_client.contact_mobile end,
    contact_email=case when v_patch?'contact_email' then v_in.contact_email else v_client.contact_email end,
    website=case when v_patch?'website' then v_in.website else v_client.website end,
    notes=case when v_patch?'notes' then v_in.notes else v_client.notes end,updated_at=now()
  where id=p_client_id returning * into v_client;
  update public.client_settings set
    reversal_complete_financials_date=v_reversal,
    reversal_replacement_financials_date=v_replacement,
    candidate_electronic_auto_authorise=case when v_candidate_settings_enabled and v_policy?'candidate_electronic_auto_authorise'
      then v_settings_in.candidate_electronic_auto_authorise else v_settings.candidate_electronic_auto_authorise end,
    candidate_expenses_require_separate_timesheet=case when v_candidate_settings_enabled and v_policy?'candidate_expenses_require_separate_timesheet'
      then v_settings_in.candidate_expenses_require_separate_timesheet else v_settings.candidate_expenses_require_separate_timesheet end,
    candidate_paper_submission_enabled=case when v_candidate_settings_enabled and v_policy?'candidate_paper_submission_enabled'
      then v_settings_in.candidate_paper_submission_enabled else v_settings.candidate_paper_submission_enabled end,
    candidate_expense_invoice_email=case when v_candidate_settings_enabled and v_policy?'candidate_expense_invoice_email'
      then nullif(btrim(v_settings_in.candidate_expense_invoice_email),'') else v_settings.candidate_expense_invoice_email end,
    candidate_manager_approval_policy_json=case when v_candidate_settings_enabled and v_policy?'candidate_manager_approval_policy_json'
      then v_settings_in.candidate_manager_approval_policy_json else v_settings.candidate_manager_approval_policy_json end,
    allow_daily_manager_authorise_on_phone=case when v_candidate_settings_enabled and v_policy?'allow_daily_manager_authorise_on_phone'
      then v_settings_in.allow_daily_manager_authorise_on_phone else v_settings.allow_daily_manager_authorise_on_phone end,
    allow_daily_manager_authorise_by_email=case when v_candidate_settings_enabled and v_policy?'allow_daily_manager_authorise_by_email'
      then v_settings_in.allow_daily_manager_authorise_by_email else v_settings.allow_daily_manager_authorise_by_email end,
    updated_at=now()
  where id=v_settings.id returning * into v_settings;
  perform public._inv_write_audit(p_actor_user_id,'CLIENT_UPDATED_WITH_IMPORT_SETTINGS',jsonb_build_object('client_id',p_client_id,
    'request_key_hash',public._import_review_hash_v1(v_key),'old',v_old,'new',case when v_candidate_settings_enabled then
      jsonb_build_object('client',to_jsonb(v_client),'settings',to_jsonb(v_settings))
    else jsonb_build_object('client',to_jsonb(v_client),
      'reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
      'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date)
    end),
    'client',p_client_id::text,null::jsonb,'Atomic client/import policy update',null::text,null::text,'client-update-with-settings:'||v_key);
  return jsonb_build_object('ok',true,'client',to_jsonb(v_client),'client_settings_id',v_settings.id,'eligible',v_eligible,
    'stored',jsonb_build_object('reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
      'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date)
      || case when v_candidate_settings_enabled then jsonb_build_object(
      'candidate_electronic_auto_authorise',v_settings.candidate_electronic_auto_authorise,
      'candidate_expenses_require_separate_timesheet',v_settings.candidate_expenses_require_separate_timesheet,
      'candidate_paper_submission_enabled',v_settings.candidate_paper_submission_enabled,
      'candidate_expense_invoice_email',v_settings.candidate_expense_invoice_email,
      'candidate_manager_approval_policy_json',v_settings.candidate_manager_approval_policy_json,
      'allow_daily_manager_authorise_on_phone',v_settings.allow_daily_manager_authorise_on_phone,
      'allow_daily_manager_authorise_by_email',v_settings.allow_daily_manager_authorise_by_email)
      else '{}'::jsonb end,
    'effective',jsonb_build_object('reversal_complete_financials_date',coalesce(v_settings.reversal_complete_financials_date,v_global.reversal_complete_financials_date),
      'reversal_replacement_financials_date',coalesce(v_settings.reversal_replacement_financials_date,v_global.reversal_replacement_financials_date)),
    'source',jsonb_build_object('reversal_complete_financials_date',case when v_settings.reversal_complete_financials_date is null then 'GLOBAL' else 'CLIENT' end,
      'reversal_replacement_financials_date',case when v_settings.reversal_replacement_financials_date is null then 'GLOBAL' else 'CLIENT' end),
    'client_rev',v_client.rev,'settings_updated_at',v_settings.updated_at);
end $function$;

-- cloudtms_data_api_mfa_gate()
CREATE OR REPLACE FUNCTION public.cloudtms_data_api_mfa_gate()
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_jwt_raw text;
  v_jwt jsonb := '{}'::jsonb;
  v_role text := '';
  v_aal text := 'aal1';
  v_sub text := '';
begin
  v_jwt_raw := coalesce(
    nullif(current_setting('request.jwt', true), ''),
    nullif(current_setting('request.jwt.claims', true), ''),
    '{}'
  );

  begin
    v_jwt := v_jwt_raw::jsonb;
  exception when others then
    v_jwt := '{}'::jsonb;
  end;

  v_role := coalesce(nullif(v_jwt->>'role', ''), current_user, '');
  v_aal := coalesce(nullif(v_jwt->>'aal', ''), 'aal1');
  v_sub := coalesce(nullif(v_jwt->>'sub', ''), '');

  -- Backend/system path must not be blocked.
  if v_role = 'service_role' or current_user = 'service_role' then
    return;
  end if;

  -- Normal MFA-complete Supabase user.
  if v_role = 'authenticated' and v_aal = 'aal2' then
    return;
  end if;

  -- Known TMS/Codex/system user exemption only.
  if v_role = 'authenticated'
     and v_sub = '42f1f62c-7d11-437e-85b0-7b135be865e3' then
    return;
  end if;

  if v_role = 'anon' or v_role = '' or v_jwt = '{}'::jsonb then
    raise sqlstate 'PGRST'
      using
        message = json_build_object(
          'code', 'AUTH_REQUIRED',
          'message', 'Authentication required',
          'details', 'CloudTMS Data API/RPC access requires a logged-in user.',
          'hint', 'Login before calling the CloudTMS Data API/RPC.'
        )::text,
        detail = json_build_object(
          'status', 401,
          'status_text', 'Unauthorized'
        )::text;
  end if;

  raise sqlstate 'PGRST'
    using
      message = json_build_object(
        'code', 'MFA_REQUIRED',
        'message', 'MFA required',
        'details', 'CloudTMS Data API/RPC access requires an aal2 MFA-complete session.',
        'hint', 'Complete MFA before calling the CloudTMS Data API/RPC.'
      )::text,
      detail = json_build_object(
        'status', 403,
        'status_text', 'Forbidden'
      )::text;
end;
$function$;

-- cloudtms_format_gbp(numeric)
CREATE OR REPLACE FUNCTION public.cloudtms_format_gbp(p_amount numeric)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_amount numeric;
  v_abs_amount numeric;
  v_formatted text;
BEGIN
  IF p_amount IS NULL THEN
    RETURN '—';
  END IF;

  v_amount := round(p_amount, 2);
  v_abs_amount := abs(v_amount);
  v_formatted := to_char(v_abs_amount, 'FM999,999,999,999,999,999,999,990.00');

  IF v_amount < 0 THEN
    RETURN '-£' || v_formatted;
  END IF;

  RETURN '£' || v_formatted;
END;
$function$;

-- cloudtms_format_london_date(date)
CREATE OR REPLACE FUNCTION public.cloudtms_format_london_date(p_date date)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
BEGIN
  IF p_date IS NULL THEN
    RETURN 'Not recorded';
  END IF;

  RETURN TO_CHAR(p_date, 'FMDD FMMonth YYYY');
END;
$function$;

-- cloudtms_format_london_datetime(timestamp with time zone)
CREATE OR REPLACE FUNCTION public.cloudtms_format_london_datetime(p_ts timestamp with time zone)
 RETURNS text
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_local_ts timestamp without time zone;
BEGIN
  IF p_ts IS NULL THEN
    RETURN 'Not recorded';
  END IF;

  v_local_ts := p_ts AT TIME ZONE 'Europe/London';

  RETURN TO_CHAR(v_local_ts, 'FMDD FMMonth YYYY "at" HH24:MI "hrs"') || ' (UK time)';
END;
$function$;

-- cloudtms_jsonb_storage_keys_v1(jsonb,integer)
CREATE OR REPLACE FUNCTION public.cloudtms_jsonb_storage_keys_v1(p_document jsonb, p_max_depth integer DEFAULT 8)
 RETURNS text[]
 LANGUAGE sql
 IMMUTABLE PARALLEL SAFE
 SET search_path TO 'public', 'pg_temp'
AS $function$
WITH RECURSIVE walk(value, edge_key, depth) AS (
  SELECT COALESCE(p_document, 'null'::jsonb), NULL::text, 0
  UNION ALL
  SELECT child.value, child.edge_key, walk.depth + 1
  FROM walk
  CROSS JOIN LATERAL (
    SELECT object_entry.key AS edge_key, object_entry.value
    FROM jsonb_each(
      CASE WHEN jsonb_typeof(walk.value) = 'object' THEN walk.value ELSE '{}'::jsonb END
    ) AS object_entry(key, value)
    UNION ALL
    SELECT NULL::text AS edge_key, array_entry.value
    FROM jsonb_array_elements(
      CASE WHEN jsonb_typeof(walk.value) = 'array' THEN walk.value ELSE '[]'::jsonb END
    ) AS array_entry(value)
  ) AS child
  WHERE walk.depth < GREATEST(1, LEAST(COALESCE(p_max_depth, 8), 12))
), storage_values AS (
  SELECT DISTINCT NULLIF(BTRIM(walk.value #>> '{}'), '') AS storage_key
  FROM walk
  WHERE LOWER(COALESCE(walk.edge_key, '')) IN (
    'r2_key', 'storage_key', 'file_key', 'canonical_key', 'object_key'
  )
    AND jsonb_typeof(walk.value) = 'string'
)
SELECT COALESCE(array_agg(storage_values.storage_key ORDER BY storage_values.storage_key), ARRAY[]::text[])
FROM storage_values
WHERE storage_values.storage_key IS NOT NULL;
$function$;

-- cloudtms_office_candidate_adapter_v1(text,uuid,text,jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.cloudtms_office_candidate_adapter_v1(p_action text, p_actor_user_id uuid, p_environment text, p_payload jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'extensions', 'pg_temp'
AS $function$
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

-- codex_debug_activity_snapshot(integer,numeric)
CREATE OR REPLACE FUNCTION public.codex_debug_activity_snapshot(p_query_prefix_len integer DEFAULT 500, p_min_age_seconds numeric DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_len integer := greatest(100, least(coalesce(p_query_prefix_len, 500), 4000));
  v_min_age numeric := greatest(0, coalesce(p_min_age_seconds, 0));
  v_summary jsonb := '[]'::jsonb;
  v_active jsonb := '[]'::jsonb;
  v_blocking jsonb := '[]'::jsonb;
  v_locks jsonb := '[]'::jsonb;
  v_connections jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_summary
  FROM (
    SELECT
      a.state,
      a.wait_event_type,
      a.wait_event,
      count(*)::int AS n
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
    GROUP BY a.state, a.wait_event_type, a.wait_event
    ORDER BY n DESC
    LIMIT 50
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_active
  FROM (
    SELECT
      a.pid,
      a.usename,
      a.application_name,
      a.client_addr::text AS client_addr,
      a.state,
      a.wait_event_type,
      a.wait_event,
      round(extract(epoch from (now() - a.query_start))::numeric, 1) AS age_s,
      left(regexp_replace(coalesce(a.query, ''), '\s+', ' ', 'g'), v_len) AS query_prefix
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
      AND a.state <> 'idle'
      AND (
        a.query_start IS NULL
        OR extract(epoch from (now() - a.query_start)) >= v_min_age
      )
    ORDER BY a.query_start NULLS LAST
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_blocking
  FROM (
    SELECT
      a.pid,
      a.usename,
      a.application_name,
      a.client_addr::text AS client_addr,
      a.state,
      a.wait_event_type,
      a.wait_event,
      pg_blocking_pids(a.pid) AS blocking_pids,
      round(extract(epoch from (now() - a.query_start))::numeric, 1) AS age_s,
      left(regexp_replace(coalesce(a.query, ''), '\s+', ' ', 'g'), v_len) AS query_prefix
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
      AND (
        cardinality(pg_blocking_pids(a.pid)) > 0
        OR a.wait_event_type IS NOT NULL
        OR a.state = 'active'
      )
    ORDER BY age_s DESC NULLS LAST
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_locks
  FROM (
    SELECT
      l.locktype,
      l.mode,
      l.granted,
      count(*)::int AS n
    FROM pg_locks AS l
    GROUP BY l.locktype, l.mode, l.granted
    ORDER BY n DESC
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_connections
  FROM (
    SELECT
      a.usename,
      a.application_name,
      a.state,
      count(*)::int AS n
    FROM pg_stat_activity AS a
    WHERE a.datname = current_database()
    GROUP BY a.usename, a.application_name, a.state
    ORDER BY n DESC
    LIMIT 100
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at', now(),
    'database', current_database(),
    'activity_summary', v_summary,
    'active_queries', v_active,
    'blocking_activity', v_blocking,
    'lock_summary', v_locks,
    'connection_summary', v_connections
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- codex_debug_bulk_process_snapshot(uuid[],uuid[])
CREATE OR REPLACE FUNCTION public.codex_debug_bulk_process_snapshot(p_contract_week_ids uuid[] DEFAULT ARRAY[]::uuid[], p_timesheet_ids uuid[] DEFAULT ARRAY[]::uuid[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_contract_weeks jsonb := '[]'::jsonb;
  v_timesheets jsonb := '[]'::jsonb;
  v_evidence_counts jsonb := '[]'::jsonb;
  v_queue_counts jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_contract_weeks
  FROM (
    SELECT
      cw.id,
      cw.status,
      cw.timesheet_id,
      cw.created_at,
      cw.updated_at
    FROM public.contract_weeks AS cw
    WHERE array_length(p_contract_week_ids, 1) IS NOT NULL
      AND cw.id = ANY(p_contract_week_ids)
    ORDER BY cw.updated_at DESC NULLS LAST, cw.id
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_timesheets
  FROM (
    SELECT
      t.timesheet_id,
      t.status,
      t.is_current,
      t.manual_pdf_r2_key,
      t.created_at,
      t.updated_at
    FROM public.timesheets AS t
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND t.timesheet_id = ANY(p_timesheet_ids)
    ORDER BY t.updated_at DESC NULLS LAST, t.timesheet_id
    LIMIT 100
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_evidence_counts
  FROM (
    SELECT
      te.timesheet_id,
      coalesce(te.kind::text, 'UNKNOWN') AS kind,
      count(*)::int AS n,
      max(te.created_at) AS latest_created_at
    FROM public.timesheet_evidence AS te
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND te.timesheet_id = ANY(p_timesheet_ids)
    GROUP BY te.timesheet_id, coalesce(te.kind::text, 'UNKNOWN')
    ORDER BY te.timesheet_id, kind
    LIMIT 500
  ) AS x;

  SELECT coalesce(jsonb_agg(to_jsonb(x)), '[]'::jsonb)
  INTO v_queue_counts
  FROM (
    SELECT
      mq.timesheet_id,
      mq.status,
      count(*)::int AS n
    FROM public.manual_timesheet_queue AS mq
    WHERE array_length(p_timesheet_ids, 1) IS NOT NULL
      AND mq.timesheet_id = ANY(p_timesheet_ids)
    GROUP BY mq.timesheet_id, mq.status
    ORDER BY mq.timesheet_id, mq.status
    LIMIT 500
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'generated_at', now(),
    'contract_week_ids', to_jsonb(p_contract_week_ids),
    'timesheet_ids', to_jsonb(p_timesheet_ids),
    'contract_weeks', v_contract_weeks,
    'timesheets', v_timesheets,
    'evidence_counts', v_evidence_counts,
    'manual_queue_counts', v_queue_counts
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- codex_debug_exec_sql(text,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_exec_sql(p_sql text, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_row_count integer := NULL;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  EXECUTE v_sql;
  GET DIAGNOSTICS v_row_count = ROW_COUNT;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'exec',
    'row_count', v_row_count,
    'elapsed_ms', v_elapsed_ms
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'exec',
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- codex_debug_explain_sql(text,boolean,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_explain_sql(p_sql text, p_analyze boolean DEFAULT false, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_plan json;
  v_options text;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  v_sql := regexp_replace(v_sql, ';\s*$', '');

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  v_options := CASE
    WHEN p_analyze THEN 'FORMAT JSON, ANALYZE, VERBOSE, BUFFERS'
    ELSE 'FORMAT JSON, VERBOSE'
  END;

  EXECUTE 'EXPLAIN (' || v_options || ') ' || v_sql INTO v_plan;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'explain',
    'analyze', p_analyze,
    'elapsed_ms', v_elapsed_ms,
    'plan', v_plan::jsonb
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'explain',
    'analyze', p_analyze,
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- codex_debug_function_fingerprints(text[],text)
CREATE OR REPLACE FUNCTION public.codex_debug_function_fingerprints(p_function_names text[] DEFAULT NULL::text[], p_schema text DEFAULT 'public'::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_schema text := coalesce(nullif(btrim(p_schema), ''), 'public');
  v_rows jsonb := '[]'::jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.schema_name, x.function_name, x.identity_arguments), '[]'::jsonb)
  INTO v_rows
  FROM (
    SELECT
      n.nspname AS schema_name,
      p.proname AS function_name,
      p.oid::regprocedure::text AS regprocedure,
      pg_get_function_identity_arguments(p.oid) AS identity_arguments,
      l.lanname AS language,
      p.prosecdef AS security_definer,
      p.provolatile AS volatility,
      length(d.def) AS definition_chars,
      md5(d.def) AS definition_md5,
      (position('FOR UPDATE' in upper(d.def)) > 0) AS has_for_update,
      (position('LOCK_TIMEOUT' in upper(d.def)) > 0) AS has_lock_timeout,
      (position('STATEMENT_TIMEOUT' in upper(d.def)) > 0) AS has_statement_timeout,
      (position('QUERY_CANCELED' in upper(d.def)) > 0 OR position('57014' in upper(d.def)) > 0) AS has_query_canceled_handler,
      (position('LOCK_NOT_AVAILABLE' in upper(d.def)) > 0 OR position('55P03' in upper(d.def)) > 0) AS has_lock_not_available_handler,
      (position('EXCEPTION WHEN OTHERS' in upper(d.def)) > 0) AS has_exception_when_others
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    JOIN pg_language AS l ON l.oid = p.prolang
    CROSS JOIN LATERAL (
      SELECT pg_get_functiondef(p.oid) AS def
    ) AS d
    WHERE n.nspname = v_schema
      AND (
        p_function_names IS NULL
        OR array_length(p_function_names, 1) IS NULL
        OR p.proname = ANY(p_function_names)
      )
  ) AS x;

  RETURN jsonb_build_object(
    'ok', true,
    'schema', v_schema,
    'function_names', to_jsonb(p_function_names),
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- codex_debug_lock_activity_snapshot()
CREATE OR REPLACE FUNCTION public.codex_debug_lock_activity_snapshot()
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
BEGIN
  RETURN public.codex_debug_activity_snapshot(500, 0);
END;
$function$;

-- codex_debug_pg_stat_statements_snapshot(text[],integer)
CREATE OR REPLACE FUNCTION public.codex_debug_pg_stat_statements_snapshot(p_terms text[] DEFAULT ARRAY['timesheet'::text, 'contract_week'::text, 'evidence'::text, 'manual'::text, 'tsfin'::text, 'bulk_process'::text, 'manual_timesheet_queue'::text, 'rpc_changes_ping'::text], p_limit integer DEFAULT 50)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_schema name;
  v_view text;
  v_limit integer := greatest(1, least(coalesce(p_limit, 50), 500));
  v_rows jsonb := '[]'::jsonb;
BEGIN
  SELECT n.nspname
  INTO v_schema
  FROM pg_extension AS e
  JOIN pg_namespace AS n ON n.oid = e.extnamespace
  WHERE e.extname = 'pg_stat_statements'
  LIMIT 1;

  IF v_schema IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'PG_STAT_STATEMENTS_NOT_INSTALLED'
    );
  END IF;

  v_view := format('%I.pg_stat_statements', v_schema);

  IF to_regclass(v_view) IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'PG_STAT_STATEMENTS_VIEW_NOT_FOUND',
      'extension_schema', v_schema
    );
  END IF;

  EXECUTE format(
    'select coalesce(jsonb_agg(to_jsonb(s)), ''[]''::jsonb)
       from (
         select
           userid::text as userid,
           dbid::text as dbid,
           queryid::text as queryid,
           calls,
           round(total_exec_time::numeric, 1) as total_exec_time_ms,
           round(mean_exec_time::numeric, 1) as mean_exec_time_ms,
           round(max_exec_time::numeric, 1) as max_exec_time_ms,
           rows,
           left(regexp_replace(query, ''\s+'', '' '', ''g''), 700) as query_prefix
         from %s as p
         where (
           $1 is null
           or array_length($1, 1) is null
           or exists (
             select 1
             from unnest($1) as term(value)
             where p.query ilike ''%%'' || term.value || ''%%''
           )
         )
         order by total_exec_time desc
         limit %s
       ) as s',
    v_view,
    v_limit
  )
  INTO v_rows
  USING p_terms;

  RETURN jsonb_build_object(
    'ok', true,
    'extension_schema', v_schema,
    'limit', v_limit,
    'terms', to_jsonb(p_terms),
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  RETURN jsonb_build_object(
    'ok', false,
    'sqlstate', SQLSTATE,
    'message', SQLERRM
  );
END;
$function$;

-- codex_debug_query_sql(text,integer,integer,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_query_sql(p_sql text, p_limit integer DEFAULT 500, p_statement_timeout_ms integer DEFAULT 0, p_lock_timeout_ms integer DEFAULT 0)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
DECLARE
  v_sql text := btrim(coalesce(p_sql, ''));
  v_limit integer := greatest(1, least(coalesce(p_limit, 500), 10000));
  v_rows jsonb := '[]'::jsonb;
  v_row_count integer := 0;
  v_started_at timestamptz := clock_timestamp();
  v_elapsed_ms numeric;
BEGIN
  IF v_sql = '' THEN
    RETURN jsonb_build_object(
      'ok', false,
      'error', 'EMPTY_SQL',
      'message', 'p_sql is empty'
    );
  END IF;

  -- Strip one trailing semicolon for easier wrapping.
  v_sql := regexp_replace(v_sql, ';\s*$', '');

  IF coalesce(p_statement_timeout_ms, 0) > 0 THEN
    PERFORM set_config('statement_timeout', p_statement_timeout_ms::text, true);
  END IF;

  IF coalesce(p_lock_timeout_ms, 0) > 0 THEN
    PERFORM set_config('lock_timeout', p_lock_timeout_ms::text, true);
  END IF;

  EXECUTE format(
    'select coalesce(jsonb_agg(to_jsonb(_codex_limited)), ''[]''::jsonb), count(*)::int
       from (
         select *
         from (%s) as _codex_inner
         limit %s
       ) as _codex_limited',
    v_sql,
    v_limit
  )
  INTO v_rows, v_row_count;

  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', true,
    'mode', 'query',
    'row_count', coalesce(v_row_count, 0),
    'limit', v_limit,
    'elapsed_ms', v_elapsed_ms,
    'rows', coalesce(v_rows, '[]'::jsonb)
  );

EXCEPTION WHEN OTHERS THEN
  v_elapsed_ms := round((extract(epoch from (clock_timestamp() - v_started_at)) * 1000)::numeric, 1);

  RETURN jsonb_build_object(
    'ok', false,
    'mode', 'query',
    'sqlstate', SQLSTATE,
    'message', SQLERRM,
    'elapsed_ms', v_elapsed_ms
  );
END;
$function$;

-- codex_debug_select_sql(text,integer)
CREATE OR REPLACE FUNCTION public.codex_debug_select_sql(p_sql text, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public'
AS $function$
DECLARE
  v_sql text;
  v_norm text;
  v_limit integer;
  v_rows jsonb := '[]'::jsonb;
  v_row_count integer := 0;
  v_truncated boolean := false;
BEGIN
  v_sql := btrim(coalesce(p_sql, ''));

  IF v_sql = '' THEN
    RAISE EXCEPTION 'codex_debug_select_sql: p_sql is required'
      USING ERRCODE = '22023';
  END IF;

  IF length(v_sql) > 20000 THEN
    RAISE EXCEPTION 'codex_debug_select_sql: SQL text is too long'
      USING ERRCODE = '22023';
  END IF;

  -- Single statement only. This intentionally also blocks semicolons inside strings.
  IF position(';' in v_sql) > 0 THEN
    RAISE EXCEPTION 'codex_debug_select_sql: semicolons / multiple statements are not allowed'
      USING ERRCODE = '22023';
  END IF;

  -- Keep the guard simple and conservative: no comments, because comments can hide/recombine tokens.
  IF position('--' in v_sql) > 0
     OR position('/*' in v_sql) > 0
     OR position('*/' in v_sql) > 0 THEN
    RAISE EXCEPTION 'codex_debug_select_sql: SQL comments are not allowed'
      USING ERRCODE = '22023';
  END IF;

  v_norm := lower(regexp_replace(v_sql, '\s+', ' ', 'g'));

  -- Only SELECT / WITH diagnostics.
  IF NOT (v_norm ~ '^\s*(select|with)\M') THEN
    RAISE EXCEPTION 'codex_debug_select_sql: only SELECT/WITH queries are allowed'
      USING ERRCODE = '22023';
  END IF;

  -- Block obvious write/DDL/transaction/control statements even if embedded in CTEs.
  IF v_norm ~ '\m(insert|update|delete|drop|alter|create|truncate|grant|revoke|call|do|copy|execute|merge|vacuum|analyze|reindex|cluster|refresh|listen|notify|lock|begin|commit|rollback|savepoint|set|reset|into)\M' THEN
    RAISE EXCEPTION 'codex_debug_select_sql: blocked keyword in diagnostic SQL'
      USING ERRCODE = '22023';
  END IF;

  -- Block explicit row locks.
  IF v_norm ~ '\mfor\s+(update|no\s+key\s+update|share|key\s+share)\M' THEN
    RAISE EXCEPTION 'codex_debug_select_sql: row-locking clauses are not allowed'
      USING ERRCODE = '22023';
  END IF;

  -- Block known side-effect / disruption / external-call helpers.
  IF v_norm ~ '\m(pg_sleep|pg_terminate_backend|pg_cancel_backend|pg_reload_conf|pg_rotate_logfile|pg_start_backup|pg_stop_backup|pg_advisory_lock|pg_try_advisory_lock|nextval|setval|lo_import|lo_export|dblink|http_get|http_post|http_put|http_delete|net\.)\M' THEN
    RAISE EXCEPTION 'codex_debug_select_sql: blocked function or extension call in diagnostic SQL'
      USING ERRCODE = '22023';
  END IF;

  v_limit := greatest(1, least(coalesce(p_limit, 100), 500));

  -- Keep diagnostics bounded.
  PERFORM set_config('statement_timeout', '5000ms', true);
  PERFORM set_config('lock_timeout', '1000ms', true);
  PERFORM set_config('idle_in_transaction_session_timeout', '5000ms', true);

  EXECUTE format(
    $fmt$
      SELECT
        coalesce(jsonb_agg(to_jsonb(_codex_row) ORDER BY _codex_ord), '[]'::jsonb),
        count(*)::integer
      FROM (
        SELECT _codex_inner.*, row_number() OVER () AS _codex_ord
        FROM (
          %s
        ) AS _codex_inner
        LIMIT %s
      ) AS _codex_row
    $fmt$,
    v_sql,
    v_limit + 1
  )
  INTO v_rows, v_row_count;

  IF v_row_count > v_limit THEN
    v_truncated := true;

    SELECT coalesce(jsonb_agg(value ORDER BY ord), '[]'::jsonb)
    INTO v_rows
    FROM jsonb_array_elements(v_rows) WITH ORDINALITY AS e(value, ord)
    WHERE ord <= v_limit;

    v_row_count := v_limit;
  END IF;

  -- Remove the internal ordinal from returned rows.
  SELECT coalesce(jsonb_agg(value - '_codex_ord'), '[]'::jsonb)
  INTO v_rows
  FROM jsonb_array_elements(v_rows) AS e(value);

  RETURN jsonb_build_object(
    'ok', true,
    'limit', v_limit,
    'row_count', v_row_count,
    'truncated', v_truncated,
    'rows', v_rows
  );

EXCEPTION
  WHEN OTHERS THEN
    RETURN jsonb_build_object(
      'ok', false,
      'sqlstate', SQLSTATE,
      'message', SQLERRM,
      'rows', '[]'::jsonb
    );
END;
$function$;

-- comms_by_recipient(text,uuid,integer,integer)
CREATE OR REPLACE FUNCTION public.comms_by_recipient(p_recipient_kind text, p_recipient_id uuid, p_limit integer, p_offset integer)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_kind text := nullif(lower(btrim(coalesce(p_recipient_kind,''))), '');
  v_limit int := coalesce(p_limit, 50);
  v_offset int := coalesce(p_offset, 0);
  v_now timestamptz := now();

  v_total bigint := 0;
  v_items jsonb := '[]'::jsonb;
begin
  if v_kind is null then
    raise exception 'recipient_kind required';
  end if;

  if p_recipient_id is null then
    raise exception 'recipient_id required';
  end if;

  if v_limit < 1 then v_limit := 1; end if;
  if v_limit > 500 then v_limit := 500; end if;
  if v_offset < 0 then v_offset := 0; end if;

  with base_rows as (
    select
      u.channel,
      u.outbox_id,
      u.outbox_type,
      u.status,
      u.delivery_status,
      u.created_at_utc,
      u.sent_at,
      u.delivered_at,
      u.read_at,
      u.failed_at,
      u.to_address,
      u.subject,
      u.body_text,
      u.reference,
      u.provider_message_id,
      u.last_error,
      u.created_by,
      u.recipient_kind,
      u.recipient_id,
      u.context_kind,
      u.context_id,
      u.mailshot_run_id,
      u.document_template_id,
      u.scheduled_for_utc,
      u.next_attempt_at_utc,
      coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) as effective_ready_at_utc,
      case
        when u.read_at is not null then 'READ'
        when u.delivered_at is not null then 'DELIVERED'
        when u.sent_at is not null then 'SENT'
        when upper(coalesce(u.status,'')) = 'FAILED' or u.failed_at is not null then 'FAILED'
        when upper(coalesce(u.status,'')) = 'QUEUED'
             and coalesce(u.next_attempt_at_utc, u.scheduled_for_utc, u.created_at_utc) > v_now then 'SCHEDULED'
        when upper(coalesce(u.status,'')) = 'QUEUED' then 'QUEUED'
        else upper(coalesce(u.status,''))
      end as queue_state,
      (u.scheduled_for_utc is not null) as is_scheduled
    from public.v_outbox_unified as u
    where lower(coalesce(u.recipient_kind,'')) = v_kind
      and u.recipient_id = p_recipient_id
  ),
  counted as (
    select count(*)::bigint as total_count
    from base_rows as b
  ),
  paged as (
    select
      b.channel,
      b.outbox_id,
      b.outbox_type,
      b.status,
      b.delivery_status,
      b.created_at_utc,
      b.sent_at,
      b.delivered_at,
      b.read_at,
      b.failed_at,
      b.to_address,
      b.subject,
      b.body_text,
      b.reference,
      b.provider_message_id,
      b.last_error,
      b.created_by,
      b.recipient_kind,
      b.recipient_id,
      b.context_kind,
      b.context_id,
      b.mailshot_run_id,
      b.document_template_id,
      b.scheduled_for_utc,
      b.next_attempt_at_utc,
      b.effective_ready_at_utc,
      b.queue_state,
      b.is_scheduled
    from base_rows as b
    order by b.created_at_utc desc, b.outbox_id::text desc
    limit v_limit offset v_offset
  )
  select
    (select c.total_count from counted as c),
    coalesce(
      jsonb_agg(
        jsonb_build_object(
          'channel', p.channel,
          'outbox_id', p.outbox_id::text,
          'outbox_type', p.outbox_type,
          'status', p.status,
          'delivery_status', p.delivery_status,
          'created_at_utc', p.created_at_utc::text,
          'sent_at', case when p.sent_at is null then null else p.sent_at::text end,
          'delivered_at', case when p.delivered_at is null then null else p.delivered_at::text end,
          'read_at', case when p.read_at is null then null else p.read_at::text end,
          'failed_at', case when p.failed_at is null then null else p.failed_at::text end,
          'to_address', p.to_address,
          'subject', p.subject,
          'body_preview', case
            when p.body_text is null then null
            when char_length(p.body_text) <= 200 then p.body_text
            else left(p.body_text, 200) || '…'
          end,
          'reference', p.reference,
          'provider_message_id', p.provider_message_id,
          'last_error', p.last_error,
          'created_by', case when p.created_by is null then null else p.created_by::text end,
          'recipient_kind', p.recipient_kind,
          'recipient_id', case when p.recipient_id is null then null else p.recipient_id::text end,
          'context_kind', p.context_kind,
          'context_id', case when p.context_id is null then null else p.context_id::text end,
          'mailshot_run_id', case when p.mailshot_run_id is null then null else p.mailshot_run_id::text end,
          'document_template_id', case when p.document_template_id is null then null else p.document_template_id::text end,
          'scheduled_for_utc', case when p.scheduled_for_utc is null then null else p.scheduled_for_utc::text end,
          'next_attempt_at_utc', case when p.next_attempt_at_utc is null then null else p.next_attempt_at_utc::text end,
          'effective_ready_at_utc', case when p.effective_ready_at_utc is null then null else p.effective_ready_at_utc::text end,
          'queue_state', p.queue_state,
          'is_scheduled', p.is_scheduled
        )
        order by p.created_at_utc desc, p.outbox_id::text desc
      ),
      '[]'::jsonb
    )
  into v_total, v_items
  from paged as p;

  return jsonb_build_object(
    'ok', true,
    'recipient_kind', v_kind,
    'recipient_id', p_recipient_id::text,
    'limit', v_limit,
    'offset', v_offset,
    'total_count', v_total,
    'items', v_items
  );
end;
$function$;

-- comms_outbox_claim_ready_batch(text,integer,text,integer)
CREATE OR REPLACE FUNCTION public.comms_outbox_claim_ready_batch(p_channel text, p_limit integer, p_attempt_lease_token text, p_lease_minutes integer DEFAULT 5)
 RETURNS SETOF comms_outbox
 LANGUAGE plpgsql
AS $function$
declare
  v_now timestamptz := now();
  v_channel text := nullif(upper(btrim(coalesce(p_channel, ''))), '');
  v_effective_limit integer := greatest(coalesce(p_limit, 0), 0);
  v_effective_lease_minutes integer := greatest(coalesce(p_lease_minutes, 5), 1);
begin
  if v_channel is null then
    raise exception 'channel is required';
  end if;

  if v_channel not in ('WHATSAPP', 'SMS', 'VOICE') then
    raise exception 'unsupported channel %', v_channel;
  end if;

  if coalesce(btrim(p_attempt_lease_token), '') = '' then
    raise exception 'attempt_lease_token is required';
  end if;

  if v_effective_limit = 0 then
    return;
  end if;

  return query
  with picked as (
    select co.id
    from public.comms_outbox as co
    where upper(coalesce(co.channel, '')) = v_channel
      and upper(coalesce(co.status, '')) = 'QUEUED'
      and co.sent_at is null
      and co.delivered_at is null
      and co.read_at is null
      and coalesce(
            co.next_attempt_at_utc,
            co.scheduled_for_utc,
            co.created_at_utc
          ) <= v_now
      and (
            co.attempt_lease_token is null
         or co.attempt_lease_expires_at_utc is null
         or co.attempt_lease_expires_at_utc <= v_now
      )
    order by
      coalesce(
        co.next_attempt_at_utc,
        co.scheduled_for_utc,
        co.created_at_utc
      ) asc,
      co.created_at_utc asc,
      co.id asc
    for update skip locked
    limit v_effective_limit
  ),
  updated as (
    update public.comms_outbox as co
    set attempt_lease_token = p_attempt_lease_token,
        attempt_leased_at_utc = v_now,
        attempt_lease_expires_at_utc = v_now + make_interval(mins => v_effective_lease_minutes)
    from picked
    where co.id = picked.id
    returning co.*
  )
  select u.*
  from updated as u
  order by
    coalesce(
      u.next_attempt_at_utc,
      u.scheduled_for_utc,
      u.created_at_utc
    ) asc,
    u.created_at_utc asc,
    u.id asc;

  return;
end;
$function$;

-- contract_list_count(jsonb)
CREATE OR REPLACE FUNCTION public.contract_list_count(p_filters jsonb)
 RETURNS TABLE(count_all bigint)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_candidate_id uuid := null;
  v_client_id uuid := null;

  v_pay_method text := null;
  v_role_like text := null;
  v_band text := null;

  v_candidate_name_like text := null;
  v_client_name_like text := null;

  v_active_on date := null;

  v_auto_invoice text := null;
  v_q text := null;

  v_default_submission_mode text := null;
  v_week_ending_weekday_snapshot int := null;

  v_require_ref_pay text := null;
  v_require_ref_inv text := null;

  v_has_custom_labels text := null;

  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;

  v_start_from date := null;
  v_start_to date := null;
  v_end_from date := null;
  v_end_to date := null;

  v_mileage_pay_rate numeric := null;
  v_mileage_charge_rate numeric := null;

  v_status text := null; -- all|active|completed|unassigned

  v_ids jsonb := null;
  v_ids_arr uuid[] := null;

  INCOMPLETE_STATUSES text[] := array['OPEN','PLANNED','SUBMITTED','AUTHORISED'];
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;

  v_pay_method := nullif(btrim(coalesce(p_filters->>'pay_method_snapshot','')), '');
  if v_pay_method is not null then v_pay_method := upper(v_pay_method); end if;

  if nullif(btrim(coalesce(p_filters->>'role','')), '') is not null then
    v_role_like := '%' || (p_filters->>'role') || '%';
  end if;

  v_band := nullif(btrim(coalesce(p_filters->>'band','')), '');

  if nullif(btrim(coalesce(p_filters->>'candidate_name','')), '') is not null then
    v_candidate_name_like := '%' || (p_filters->>'candidate_name') || '%';
  end if;
  if nullif(btrim(coalesce(p_filters->>'client_name','')), '') is not null then
    v_client_name_like := '%' || (p_filters->>'client_name') || '%';
  end if;

  begin if nullif(btrim(coalesce(p_filters->>'active_on','')), '') is not null then v_active_on := (p_filters->>'active_on')::date; end if; exception when others then v_active_on := null; end;

  v_auto_invoice := nullif(btrim(coalesce(p_filters->>'auto_invoice','')), '');

  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'default_submission_mode','')), '');
  if v_default_submission_mode is null then
    v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'submission_mode','')), '');
  end if;
  if v_default_submission_mode is not null then v_default_submission_mode := upper(v_default_submission_mode); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_weekday_snapshot','')), '') is not null then v_week_ending_weekday_snapshot := (p_filters->>'week_ending_weekday_snapshot')::int; end if; exception when others then v_week_ending_weekday_snapshot := null; end;

  v_require_ref_pay := nullif(btrim(coalesce(p_filters->>'require_reference_to_pay','')), '');
  v_require_ref_inv := nullif(btrim(coalesce(p_filters->>'require_reference_to_invoice','')), '');

  v_has_custom_labels := nullif(btrim(coalesce(p_filters->>'has_custom_labels','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_from','')), '') is not null then v_updated_from := (p_filters->>'updated_from')::timestamptz; end if; exception when others then v_updated_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_to','')), '') is not null then v_updated_to := (p_filters->>'updated_to')::timestamptz; end if; exception when others then v_updated_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'start_date_from','')), '') is not null then v_start_from := (p_filters->>'start_date_from')::date; end if; exception when others then v_start_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'start_date_to','')), '') is not null then v_start_to := (p_filters->>'start_date_to')::date; end if; exception when others then v_start_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'end_date_from','')), '') is not null then v_end_from := (p_filters->>'end_date_from')::date; end if; exception when others then v_end_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_to','')), '') is not null then v_end_to := (p_filters->>'end_date_to')::date; end if; exception when others then v_end_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'mileage_pay_rate','')), '') is not null then v_mileage_pay_rate := (p_filters->>'mileage_pay_rate')::numeric; end if; exception when others then v_mileage_pay_rate := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate','')), '') is not null then v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric; end if; exception when others then v_mileage_charge_rate := null; end;

  v_status := lower(nullif(btrim(coalesce(p_filters->>'status','')), ''));
  if v_status is null then v_status := 'all'; end if;

  -- ids filter (focus selections): p_filters.ids can be array of uuids
  if p_filters ? 'ids' then
    v_ids := p_filters->'ids';
    if v_ids is not null and jsonb_typeof(v_ids)='array' then
      select array_agg((x)::uuid) into v_ids_arr
      from jsonb_array_elements_text(v_ids) x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    end if;
  end if;

  return query
  with base as (
    select
      c.*,
      cand.display_name as cand_display,
      cand.first_name as cand_first,
      cand.last_name as cand_last,
      cli.name as cli_name
    from public.contracts c
    left join public.candidates cand on cand.id = c.candidate_id
    left join public.clients cli on cli.id = c.client_id
    where (v_ids_arr is null or c.id = any(v_ids_arr))
      and (v_candidate_id is null or c.candidate_id = v_candidate_id)
      and (v_client_id is null or c.client_id = v_client_id)
      and (v_pay_method is null or upper(coalesce(c.pay_method_snapshot::text,'')) = v_pay_method)
      and (v_band is null or c.band = v_band)
      and (v_role_like is null or c.role ilike v_role_like)
      and (v_active_on is null or (c.start_date <= v_active_on and c.end_date >= v_active_on))
      and (
        v_auto_invoice is null
        or (lower(v_auto_invoice) = 'true' and c.auto_invoice = true)
        or (lower(v_auto_invoice) = 'false' and c.auto_invoice = false)
      )
      and (v_default_submission_mode is null or upper(coalesce(c.default_submission_mode::text,'')) = v_default_submission_mode)
      and (v_week_ending_weekday_snapshot is null or c.week_ending_weekday_snapshot = v_week_ending_weekday_snapshot)
      and (
        v_require_ref_pay is null
        or (lower(v_require_ref_pay) = 'true' and c.require_reference_to_pay = true)
        or (lower(v_require_ref_pay) = 'false' and c.require_reference_to_pay = false)
      )
      and (
        v_require_ref_inv is null
        or (lower(v_require_ref_inv) = 'true' and c.require_reference_to_invoice = true)
        or (lower(v_require_ref_inv) = 'false' and c.require_reference_to_invoice = false)
      )
      and (
        v_has_custom_labels is null
        or (lower(v_has_custom_labels) = 'true' and c.bucket_labels_json is not null)
        or (lower(v_has_custom_labels) = 'false' and c.bucket_labels_json is null)
      )
      and (v_created_from is null or c.created_at >= v_created_from)
      and (v_created_to is null or c.created_at <= v_created_to)
      and (v_updated_from is null or c.updated_at >= v_updated_from)
      and (v_updated_to is null or c.updated_at <= v_updated_to)
      and (v_start_from is null or c.start_date >= v_start_from)
      and (v_start_to is null or c.start_date <= v_start_to)
      and (v_end_from is null or c.end_date >= v_end_from)
      and (v_end_to is null or c.end_date <= v_end_to)
      and (v_mileage_pay_rate is null or c.mileage_pay_rate = v_mileage_pay_rate)
      and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)

      and (
        v_candidate_name_like is null
        or (coalesce(cand.display_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.first_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.last_name,'') ilike v_candidate_name_like)
      )
      and (
        v_client_name_like is null
        or (coalesce(cli.name,'') ilike v_client_name_like)
      )
      and (
        v_q is null
        or (
          coalesce(cli.name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.display_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.first_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.last_name,'') ilike ('%'||v_q||'%')
          or coalesce(c.role,'') ilike ('%'||v_q||'%')
        )
      )
  ),
  filtered as (
    select b.*
    from base b
    where
      -- status semantics:
      (v_status = 'all')
      or (v_status = 'unassigned' and b.candidate_id is null)
      or (
        v_status = 'active'
        and exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
      or (
        v_status = 'completed'
        and b.candidate_id is not null
        and not exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(INCOMPLETE_STATUSES)
        )
      )
  )
  select count(*)::bigint as count_all
  from filtered;
end;
$function$;

-- contract_list_ids(jsonb)
CREATE OR REPLACE FUNCTION public.contract_list_ids(p_filters jsonb)
 RETURNS TABLE(id uuid)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_candidate_id uuid := null;
  v_client_id uuid := null;
  v_pay_method text := null;
  v_role_like text := null;
  v_band text := null;
  v_candidate_name_like text := null;
  v_client_name_like text := null;
  v_active_on date := null;
  v_auto_invoice text := null;
  v_q text := null;
  v_default_submission_mode text := null;
  v_week_ending_weekday_snapshot int := null;
  v_require_ref_pay text := null;
  v_require_ref_inv text := null;
  v_has_custom_labels text := null;
  v_created_from timestamptz := null;
  v_created_to timestamptz := null;
  v_updated_from timestamptz := null;
  v_updated_to timestamptz := null;
  v_start_from date := null;
  v_start_to date := null;
  v_end_from date := null;
  v_end_to date := null;
  v_mileage_pay_rate numeric := null;
  v_mileage_charge_rate numeric := null;
  v_status text := null;
  v_ids jsonb := null;
  v_ids_arr uuid[] := null;
  incomplete_statuses text[] := array['OPEN','PLANNED','SUBMITTED','AUTHORISED'];
begin
  if p_filters is null then p_filters := '{}'::jsonb; end if;

  begin if nullif(btrim(coalesce(p_filters->>'candidate_id','')), '') is not null then v_candidate_id := (p_filters->>'candidate_id')::uuid; end if; exception when others then v_candidate_id := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'client_id','')), '') is not null then v_client_id := (p_filters->>'client_id')::uuid; end if; exception when others then v_client_id := null; end;

  v_pay_method := nullif(btrim(coalesce(p_filters->>'pay_method_snapshot','')), '');
  if v_pay_method is not null then v_pay_method := upper(v_pay_method); end if;

  if nullif(btrim(coalesce(p_filters->>'role','')), '') is not null then
    v_role_like := '%' || (p_filters->>'role') || '%';
  end if;

  v_band := nullif(btrim(coalesce(p_filters->>'band','')), '');

  if nullif(btrim(coalesce(p_filters->>'candidate_name','')), '') is not null then
    v_candidate_name_like := '%' || (p_filters->>'candidate_name') || '%';
  end if;
  if nullif(btrim(coalesce(p_filters->>'client_name','')), '') is not null then
    v_client_name_like := '%' || (p_filters->>'client_name') || '%';
  end if;

  begin if nullif(btrim(coalesce(p_filters->>'active_on','')), '') is not null then v_active_on := (p_filters->>'active_on')::date; end if; exception when others then v_active_on := null; end;

  v_auto_invoice := nullif(btrim(coalesce(p_filters->>'auto_invoice','')), '');
  v_q := nullif(btrim(coalesce(p_filters->>'q','')), '');

  v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'default_submission_mode','')), '');
  if v_default_submission_mode is null then
    v_default_submission_mode := nullif(btrim(coalesce(p_filters->>'submission_mode','')), '');
  end if;
  if v_default_submission_mode is not null then v_default_submission_mode := upper(v_default_submission_mode); end if;

  begin if nullif(btrim(coalesce(p_filters->>'week_ending_weekday_snapshot','')), '') is not null then v_week_ending_weekday_snapshot := (p_filters->>'week_ending_weekday_snapshot')::int; end if; exception when others then v_week_ending_weekday_snapshot := null; end;

  v_require_ref_pay := nullif(btrim(coalesce(p_filters->>'require_reference_to_pay','')), '');
  v_require_ref_inv := nullif(btrim(coalesce(p_filters->>'require_reference_to_invoice','')), '');
  v_has_custom_labels := nullif(btrim(coalesce(p_filters->>'has_custom_labels','')), '');

  begin if nullif(btrim(coalesce(p_filters->>'created_from','')), '') is not null then v_created_from := (p_filters->>'created_from')::timestamptz; end if; exception when others then v_created_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'created_to','')), '') is not null then v_created_to := (p_filters->>'created_to')::timestamptz; end if; exception when others then v_created_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_from','')), '') is not null then v_updated_from := (p_filters->>'updated_from')::timestamptz; end if; exception when others then v_updated_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'updated_to','')), '') is not null then v_updated_to := (p_filters->>'updated_to')::timestamptz; end if; exception when others then v_updated_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'start_date_from','')), '') is not null then v_start_from := (p_filters->>'start_date_from')::date; end if; exception when others then v_start_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'start_date_to','')), '') is not null then v_start_to := (p_filters->>'start_date_to')::date; end if; exception when others then v_start_to := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_from','')), '') is not null then v_end_from := (p_filters->>'end_date_from')::date; end if; exception when others then v_end_from := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'end_date_to','')), '') is not null then v_end_to := (p_filters->>'end_date_to')::date; end if; exception when others then v_end_to := null; end;

  begin if nullif(btrim(coalesce(p_filters->>'mileage_pay_rate','')), '') is not null then v_mileage_pay_rate := (p_filters->>'mileage_pay_rate')::numeric; end if; exception when others then v_mileage_pay_rate := null; end;
  begin if nullif(btrim(coalesce(p_filters->>'mileage_charge_rate','')), '') is not null then v_mileage_charge_rate := (p_filters->>'mileage_charge_rate')::numeric; end if; exception when others then v_mileage_charge_rate := null; end;

  v_status := lower(nullif(btrim(coalesce(p_filters->>'status','')), ''));
  if v_status is null then v_status := 'all'; end if;

  if p_filters ? 'ids' then
    v_ids := p_filters->'ids';
    if v_ids is not null and jsonb_typeof(v_ids) = 'array' then
      select array_agg((x)::uuid) into v_ids_arr
      from jsonb_array_elements_text(v_ids) x
      where nullif(btrim(coalesce(x,'')), '') is not null;
    elsif v_ids is not null and jsonb_typeof(v_ids) <> 'array' and nullif(btrim(coalesce(p_filters->>'ids','')), '') is not null then
      select array_agg(val::uuid)
      into v_ids_arr
      from (
        select distinct nullif(btrim(x), '') as val
        from unnest(regexp_split_to_array(p_filters->>'ids', '\s*,\s*')) as u(x)
      ) s
      where s.val is not null;
    end if;
  end if;

  return query
  with base as (
    select
      c.id,
      c.candidate_id,
      c.client_id,
      c.role,
      c.band,
      c.start_date,
      c.end_date,
      c.auto_invoice,
      c.pay_method_snapshot,
      c.default_submission_mode,
      c.week_ending_weekday_snapshot,
      c.require_reference_to_pay,
      c.require_reference_to_invoice,
      c.created_at,
      c.updated_at,
      c.bucket_labels_json,
      c.mileage_pay_rate,
      c.mileage_charge_rate,
      cand.display_name as cand_display,
      cand.first_name as cand_first,
      cand.last_name as cand_last,
      cli.name as cli_name
    from public.contracts c
    left join public.candidates cand on cand.id = c.candidate_id
    left join public.clients cli on cli.id = c.client_id
    where (v_ids_arr is null or c.id = any(v_ids_arr))
      and (v_candidate_id is null or c.candidate_id = v_candidate_id)
      and (v_client_id is null or c.client_id = v_client_id)
      and (v_pay_method is null or upper(coalesce(c.pay_method_snapshot::text,'')) = v_pay_method)
      and (v_band is null or c.band = v_band)
      and (v_role_like is null or c.role ilike v_role_like)
      and (v_active_on is null or (c.start_date <= v_active_on and c.end_date >= v_active_on))
      and (
        v_auto_invoice is null
        or (lower(v_auto_invoice) = 'true' and c.auto_invoice = true)
        or (lower(v_auto_invoice) = 'false' and c.auto_invoice = false)
      )
      and (v_default_submission_mode is null or upper(coalesce(c.default_submission_mode::text,'')) = v_default_submission_mode)
      and (v_week_ending_weekday_snapshot is null or c.week_ending_weekday_snapshot = v_week_ending_weekday_snapshot)
      and (
        v_require_ref_pay is null
        or (lower(v_require_ref_pay) = 'true' and c.require_reference_to_pay = true)
        or (lower(v_require_ref_pay) = 'false' and c.require_reference_to_pay = false)
      )
      and (
        v_require_ref_inv is null
        or (lower(v_require_ref_inv) = 'true' and c.require_reference_to_invoice = true)
        or (lower(v_require_ref_inv) = 'false' and c.require_reference_to_invoice = false)
      )
      and (
        v_has_custom_labels is null
        or (lower(v_has_custom_labels) = 'true' and c.bucket_labels_json is not null)
        or (lower(v_has_custom_labels) = 'false' and c.bucket_labels_json is null)
      )
      and (v_created_from is null or c.created_at >= v_created_from)
      and (v_created_to is null or c.created_at <= v_created_to)
      and (v_updated_from is null or c.updated_at >= v_updated_from)
      and (v_updated_to is null or c.updated_at <= v_updated_to)
      and (v_start_from is null or c.start_date >= v_start_from)
      and (v_start_to is null or c.start_date <= v_start_to)
      and (v_end_from is null or c.end_date >= v_end_from)
      and (v_end_to is null or c.end_date <= v_end_to)
      and (v_mileage_pay_rate is null or c.mileage_pay_rate = v_mileage_pay_rate)
      and (v_mileage_charge_rate is null or c.mileage_charge_rate = v_mileage_charge_rate)
      and (
        v_candidate_name_like is null
        or (coalesce(cand.display_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.first_name,'') ilike v_candidate_name_like)
        or (coalesce(cand.last_name,'') ilike v_candidate_name_like)
      )
      and (
        v_client_name_like is null
        or (coalesce(cli.name,'') ilike v_client_name_like)
      )
      and (
        v_q is null
        or (
          coalesce(cli.name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.display_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.first_name,'') ilike ('%'||v_q||'%')
          or coalesce(cand.last_name,'') ilike ('%'||v_q||'%')
          or coalesce(c.role,'') ilike ('%'||v_q||'%')
        )
      )
  ),
  filtered as (
    select b.id
    from base b
    where
      (v_status = 'all')
      or (v_status = 'unassigned' and b.candidate_id is null)
      or (
        v_status = 'active'
        and exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(incomplete_statuses)
        )
      )
      or (
        v_status = 'completed'
        and b.candidate_id is not null
        and not exists (
          select 1
          from public.contract_weeks cw
          where cw.contract_id = b.id
            and upper(cw.status::text) = any(incomplete_statuses)
        )
      )
  )
  select filtered.id
  from filtered
  order by filtered.id;
end;
$function$;

-- contract_week_delete_planned(uuid,uuid)
CREATE OR REPLACE FUNCTION public.contract_week_delete_planned(p_contract_week_id uuid, p_actor_user_id uuid)
 RETURNS TABLE(deleted boolean, contract_week_id uuid)
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_cw public.contract_weeks%rowtype;
begin
  if p_contract_week_id is null then
    raise exception 'contract_week_id is required';
  end if;

  select *
  into v_cw
  from public.contract_weeks
  where id = p_contract_week_id
  for update;

  if not found then
    raise exception 'contract_week not found';
  end if;

  if v_cw.timesheet_id is not null then
    raise exception 'Not a planned-only week: timesheet_id is present';
  end if;

  delete from public.contract_weeks
  where id = p_contract_week_id;

  insert into public.audit_events(
    object_type, object_id_text, action, before_json, after_json, reason, actor_user_id, ts_utc
  )
  values(
    'contract_week',
    p_contract_week_id::text,
    'CONTRACT_WEEK_DELETED_PLANNED',
    jsonb_build_object('contract_id', v_cw.contract_id, 'week_ending_date', v_cw.week_ending_date, 'additional_seq', v_cw.additional_seq),
    null,
    null,
    p_actor_user_id,
    v_now
  );

  deleted := true;
  contract_week_id := p_contract_week_id;
  return next;
end;
$function$;

-- contract_week_manual_draft_upsert_atomic_v1(uuid,text,jsonb,jsonb,boolean,boolean,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.contract_week_manual_draft_upsert_atomic_v1(p_week_id uuid, p_expected_row_signature text, p_totals_json jsonb, p_planned_schedule_json jsonb DEFAULT NULL::jsonb, p_replace_planned_schedule boolean DEFAULT false, p_force_adjustment boolean DEFAULT false, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_catalog'
AS $function$
DECLARE
  v_week public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb;
  v_after_signature_json jsonb;
  v_current_row_signature text;
  v_after_row_signature text;
  v_expected_row_signature text := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');
  v_status text;
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_candidate_guard jsonb := '{}'::jsonb;
BEGIN
  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF v_expected_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF p_totals_json IS NULL OR jsonb_typeof(p_totals_json) <> 'object' THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_totals_json', 'reason', 'object_required')::text;
  END IF;

  IF COALESCE(p_replace_planned_schedule, false)
     AND (p_planned_schedule_json IS NULL OR jsonb_typeof(p_planned_schedule_json) <> 'array') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object('field', 'p_planned_schedule_json', 'reason', 'array_required')::text;
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_NOT_FOUND',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'current_timesheet_id', v_week.timesheet_id
      )::text;
  END IF;

  v_status := UPPER(BTRIM(COALESCE(v_week.status::text, '')));
  IF v_status NOT IN ('PLANNED', 'OPEN') THEN
    RAISE EXCEPTION USING
      MESSAGE = 'CONTRACT_WEEK_DRAFT_NOT_EDITABLE',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'status', v_status
      )::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_current_row_signature := NULLIF(BTRIM(COALESCE(
    v_before_signature_json ->> 'backend_row_signature',
    v_before_signature_json ->> 'row_signature',
    v_before_signature_json ->> 'signature',
    ''
  )), '');

  IF v_current_row_signature IS NULL
     OR v_current_row_signature IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'contract_week_id', v_week.id,
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature
      )::text;
  END IF;

  v_candidate_guard := private._candidate_draft_totals_guard_v1(v_week.id,p_totals_json);

  UPDATE public.contract_weeks AS cw
  SET totals_json = p_totals_json,
      planned_schedule_json = CASE
        WHEN COALESCE(p_replace_planned_schedule, false) THEN p_planned_schedule_json
        ELSE cw.planned_schedule_json
      END,
      is_adjustment = CASE
        WHEN COALESCE(p_force_adjustment, false) THEN true
        ELSE cw.is_adjustment
      END,
      updated_at = v_now
  WHERE cw.id = v_week.id
    AND cw.timesheet_id IS NULL
  RETURNING cw.* INTO v_week;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'TIMESHEET_MOVED',
      DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(
    NULL::uuid,
    v_week.id,
    false
  );
  v_after_row_signature := NULLIF(BTRIM(COALESCE(
    v_after_signature_json ->> 'backend_row_signature',
    v_after_signature_json ->> 'row_signature',
    v_after_signature_json ->> 'signature',
    ''
  )), '');

  IF v_after_row_signature IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'PLANNED_CONTRACT_WEEK_SIGNATURE_MISSING',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  RETURN to_jsonb(v_week) || jsonb_build_object(
    'ok', true,
    'updated', true,
    'contract_week_id', v_week.id,
    'current_timesheet_id', NULL,
    'backend_row_signature', v_after_row_signature,
    'mutation_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'planned_contract_week_authority_complete', true,
    'planned_contract_week_authority_contract_week_id', v_week.id,
    'refresh_required', false,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'row_key', 'contract_week:' || v_week.id::text,
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'backend_row_signature', v_after_row_signature,
      'row_signature', v_after_row_signature,
      'planned_contract_week_authority_complete', true
    ))
  ) || CASE
    WHEN private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') THEN
      jsonb_build_object(
        'candidate_record_role',v_candidate_guard->>'record_role',
        'candidate_final_state_guard',v_candidate_guard)
    ELSE '{}'::jsonb END;
END;
$function$;

-- contract_week_manual_unprocess_atomic(uuid,uuid,uuid,timestamp with time zone,text)
CREATE OR REPLACE FUNCTION public.contract_week_manual_unprocess_atomic(p_week_id uuid, p_expected_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_existing_queue public.manual_timesheet_queue%ROWTYPE;
  v_contract public.contracts%ROWTYPE;

  v_booking_id text := NULL;
  v_all_timesheet_ids uuid[] := ARRAY[]::uuid[];
  v_deleted_timesheet_count integer := 0;
  v_deleted_tsfin_count integer := 0;
  v_deleted_evidence_count integer := 0;
  v_deleted_validation_count integer := 0;
  v_deleted_ts_pdf_outbox_count integer := 0;
  v_deleted_tsfin_outbox_count integer := 0;
  v_cleared_snooze_ids uuid[] := ARRAY[]::uuid[];

  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;

  v_paid_timesheet_id uuid := NULL;
  v_invoice_locked_timesheet_id uuid := NULL;
  v_invoice_segment_locked_timesheet_id uuid := NULL;

  v_seen_storage_keys text[] := ARRAY[]::text[];
  v_stage_items jsonb := '[]'::jsonb;
  v_stage_item jsonb := NULL;
  v_stage_item_kind text := NULL;
  v_stage_item_storage_key text := NULL;
  v_stage_item_display_name text := NULL;
  v_stage_item_created_at timestamp with time zone := NULL;
  v_stage_item_created_by uuid := NULL;
  v_stage_item_rotation integer := 0;
  v_stage_item_timesheet_id uuid := NULL;
  v_stage_item_evidence_id uuid := NULL;
  v_timesheet_stage_keys text[] := ARRAY[]::text[];
  v_active_timesheet_keys text[] := ARRAY[]::text[];
  v_active_timesheet_missing_key_id uuid := NULL;
  v_existing_active_key text := NULL;
  v_repaired_same_key_duplicate_count integer := 0;
  v_dematerialised_primary_timesheet_storage_key text := NULL;
  v_staged_count integer := 0;

  v_evidence_record record;
  v_queue_record record;
  v_queue_storage_key text := NULL;
  v_queue_kind text := NULL;
  v_duplicate_queue_ids uuid[] := ARRAY[]::uuid[];
  v_clean_meta jsonb := '{}'::jsonb;
  v_merged_meta jsonb := '{}'::jsonb;

  v_reopen_snapshot text := 'MANUAL';
  v_reopened_totals_json jsonb := '{}'::jsonb;
  v_reopened_day_entries_json jsonb := '{}'::jsonb;
  v_reopened_planned_schedule_json jsonb := NULL;
  v_additional_units_week_json jsonb := '{}'::jsonb;
  v_additional_units_per_day_json jsonb := '{}'::jsonb;
  v_existing_totals_json jsonb := '{}'::jsonb;
  v_expenses_draft_json jsonb := '{}'::jsonb;

  v_signature_after_text text := NULL;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_error_constraint text := NULL;
  v_history jsonb := '{}'::jsonb;
BEGIN

  if p_expected_timesheet_id is not null
     and coalesce((public._ctms_import_correction_classify_v1(p_expected_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    declare v_transition jsonb;
    begin
      v_transition := public.timesheet_correction_pair_transition_v1(
        p_expected_timesheet_id, 'UNPROCESS', p_actor_user_id,
        null::uuid, null::text, true, 100
      );
      if coalesce((v_transition ->> 'action_ready')::boolean, false) is not true
         or coalesce((v_transition ->> 'expected_member_count')::integer, 0) > 1 then
        return jsonb_build_object(
          'ok', false,
          'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_ATOMIC_PROCESS_ORCHESTRATION',
          'transition', v_transition
        );
      end if;
    end;
  end if;
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;

  IF p_expected_timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
  END IF;

  IF p_actor_user_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id')::text;
  END IF;

  IF NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_ROW_SIGNATURE_REQUIRED',
      DETAIL = jsonb_build_object(
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'message', 'The current lifecycle signature is required. Refresh the timesheet and try again.'
      )::text;
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;

  v_previous_contract_week_status := v_week.status::text;

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF v_week.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_UNPROCESSED', DETAIL = jsonb_build_object('contract_week_id', v_week.id)::text;
  END IF;

  SELECT ts.*
    INTO v_pointer_ts
  FROM public.timesheets AS ts
  WHERE ts.timesheet_id = v_week.timesheet_id
  FOR UPDATE;

  IF v_pointer_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
  END IF;

  v_booking_id := v_pointer_ts.booking_id;

  SELECT ts.*
    INTO v_current_ts
  FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id
    AND ts.is_current = true
  ORDER BY ts.version DESC, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_ts.timesheet_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'current_timesheet_not_found')::text;
  END IF;

  IF v_current_ts.timesheet_id IS DISTINCT FROM p_expected_timesheet_id THEN
    RAISE EXCEPTION USING
      MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  IF v_week.status = 'INVOICED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'contract_week_status')::text;
  END IF;

  IF v_current_ts.authorised_at_server IS NOT NULL OR v_week.status = 'AUTHORISED'::public.contract_week_status_enum THEN
    RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT locked_ids.timesheet_ids
    INTO v_all_timesheet_ids
  FROM (
    SELECT COALESCE(array_agg(locked_ts.timesheet_id ORDER BY locked_ts.version ASC, locked_ts.created_at ASC, locked_ts.timesheet_id ASC), ARRAY[]::uuid[]) AS timesheet_ids
    FROM (
      SELECT ts.timesheet_id, ts.version, ts.created_at
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_booking_id
      FOR UPDATE
    ) AS locked_ts
  ) AS locked_ids;

  IF COALESCE(array_length(v_all_timesheet_ids, 1), 0) = 0 THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('booking_id', v_booking_id, 'reason', 'timesheet_series_not_found')::text;
  END IF;

  IF EXISTS (
    SELECT 1 FROM public.timesheets AS archived_guard
    WHERE archived_guard.timesheet_id = ANY(v_all_timesheet_ids)
      AND archived_guard.archived_at_utc IS NOT NULL
  ) THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_ids', to_jsonb(v_all_timesheet_ids), 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1
  FOR UPDATE;

  IF v_current_tsfin.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'reason', 'current_timesheet_financials_not_found')::text;
  END IF;

  v_previous_processing_status := v_current_tsfin.processing_status::text;

  SELECT invoice_guard.timesheet_id
    INTO v_invoice_locked_timesheet_id
  FROM public.timesheets_financials AS invoice_guard
  WHERE invoice_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND invoice_guard.is_current = true
    AND invoice_guard.locked_by_invoice_id IS NOT NULL
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_locked_timesheet_id, 'contract_week_id', v_week.id)::text;
  END IF;

  SELECT segment_guard.timesheet_id
    INTO v_invoice_segment_locked_timesheet_id
  FROM public.timesheets_financials AS segment_guard
  WHERE segment_guard.timesheet_id = ANY(v_all_timesheet_ids)
    AND segment_guard.is_current = true
    AND EXISTS (
      SELECT 1
      FROM jsonb_array_elements(
        CASE
          WHEN segment_guard.invoice_breakdown_json IS NULL THEN '[]'::jsonb
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'array' THEN segment_guard.invoice_breakdown_json
          WHEN jsonb_typeof(segment_guard.invoice_breakdown_json) = 'object'
           AND jsonb_typeof(segment_guard.invoice_breakdown_json -> 'segments') = 'array' THEN segment_guard.invoice_breakdown_json -> 'segments'
          ELSE '[]'::jsonb
        END
      ) AS invoice_segment(segment_json)
      WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
    )
  LIMIT 1
  FOR UPDATE;

  IF v_invoice_segment_locked_timesheet_id IS NOT NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_invoice_segment_locked_timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'segment')::text;
  END IF;

  IF v_current_tsfin.authorised_at_utc IS NOT NULL OR v_current_tsfin.authorised_by_user_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ALREADY_AUTHORISED',
      DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id, 'lock_scope', 'timesheet_financials')::text;
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_signature_v1(v_current_ts.timesheet_id, v_week.id, false);
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '');

  IF COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object(
        'expected_row_signature', v_expected_row_signature,
        'current_row_signature', v_current_row_signature,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id
      )::text;
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  LIMIT 1;

  FOR v_evidence_record IN
    SELECT
      ev.id AS evidence_id,
      ev.timesheet_id,
      ev.kind,
      ev.display_name,
      ev.storage_key,
      ev.created_at,
      ev.created_by,
      ts.manual_pdf_r2_key,
      ts.manual_pdf_rotation_degrees
    FROM public.timesheet_evidence AS ev
    JOIN public.timesheets AS ts ON ts.timesheet_id = ev.timesheet_id
    WHERE ev.timesheet_id = ANY(v_all_timesheet_ids)
    ORDER BY ev.created_at ASC NULLS LAST, ev.id ASC
    FOR UPDATE OF ev
  LOOP
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.storage_key, '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL OR v_stage_item_storage_key = ANY(v_seen_storage_keys) THEN
      CONTINUE;
    END IF;
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);

    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_evidence_record.kind), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;

    v_stage_item_rotation := 0;
    IF v_stage_item_kind = 'TIMESHEET'
       AND NULLIF(regexp_replace(BTRIM(COALESCE(v_evidence_record.manual_pdf_r2_key, '')), '^/+', ''), '') = v_stage_item_storage_key THEN
      v_stage_item_rotation := COALESCE(v_evidence_record.manual_pdf_rotation_degrees, 0);
    END IF;

    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'TIMESHEET_EVIDENCE',
        'evidence_id', v_evidence_record.evidence_id,
        'timesheet_id', v_evidence_record.timesheet_id,
        'kind', v_stage_item_kind,
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(BTRIM(v_evidence_record.display_name), ''), regexp_replace(v_stage_item_storage_key, '^.*/', '')),
        'created_at', COALESCE(v_evidence_record.created_at, v_now),
        'created_by', v_evidence_record.created_by,
        'rotation_deg', v_stage_item_rotation
      )
    );
  END LOOP;

  v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_current_ts.manual_pdf_r2_key, '')), '^/+', ''), '');
  IF v_stage_item_storage_key IS NOT NULL AND NOT (v_stage_item_storage_key = ANY(v_seen_storage_keys)) THEN
    v_seen_storage_keys := array_append(v_seen_storage_keys, v_stage_item_storage_key);
    v_stage_items := v_stage_items || jsonb_build_array(
      jsonb_build_object(
        'source', 'LEGACY_MANUAL_PDF_POINTER',
        'evidence_id', NULL,
        'timesheet_id', v_current_ts.timesheet_id,
        'kind', 'TIMESHEET',
        'storage_key', v_stage_item_storage_key,
        'display_name', COALESCE(NULLIF(regexp_replace(v_stage_item_storage_key, '^.*/', ''), ''), 'file'),
        'created_at', v_now,
        'created_by', p_actor_user_id,
        'rotation_deg', COALESCE(v_current_ts.manual_pdf_rotation_degrees, 0)
      )
    );
  END IF;

  SELECT COALESCE(array_agg(DISTINCT item_rows.storage_key ORDER BY item_rows.storage_key), ARRAY[]::text[])
    INTO v_timesheet_stage_keys
  FROM (
    SELECT NULLIF(BTRIM(item_value.value ->> 'storage_key'), '') AS storage_key
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
    WHERE UPPER(COALESCE(NULLIF(BTRIM(item_value.value ->> 'kind'), ''), 'OTHER')) = 'TIMESHEET'
  ) AS item_rows
  WHERE item_rows.storage_key IS NOT NULL;

  SELECT COALESCE(array_agg(DISTINCT active_rows.storage_key ORDER BY active_rows.storage_key), ARRAY[]::text[])
    INTO v_active_timesheet_keys
  FROM (
    SELECT NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') AS storage_key
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    FOR UPDATE OF mq
  ) AS active_rows
  WHERE active_rows.storage_key IS NOT NULL;

  SELECT mq.id
    INTO v_active_timesheet_missing_key_id
  FROM public.manual_timesheet_queue AS mq
  WHERE mq.status = 'STAGED'
    AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
    AND UPPER(COALESCE(
      NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
      NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
      'TIMESHEET'
    )) = 'TIMESHEET'
    AND NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '') IS NULL
  LIMIT 1
  FOR UPDATE;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 0 AND v_active_timesheet_missing_key_id IS NOT NULL THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_TIMESHEET_EVIDENCE',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_active_timesheet_missing_key_id, 'reason', 'missing_storage_key')::text;
  END IF;

  IF COALESCE(array_length(v_timesheet_stage_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'dematerialised_storage_keys', to_jsonb(v_timesheet_stage_keys), 'timesheet_id', v_current_ts.timesheet_id)::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) > 1 THEN
    RAISE EXCEPTION USING
      MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'active_storage_keys', to_jsonb(v_active_timesheet_keys))::text;
  END IF;

  IF COALESCE(array_length(v_active_timesheet_keys, 1), 0) = 1 AND COALESCE(array_length(v_timesheet_stage_keys, 1), 0) = 1 THEN
    v_existing_active_key := v_active_timesheet_keys[1];
    IF v_existing_active_key IS DISTINCT FROM v_timesheet_stage_keys[1] THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_existing_active_key, 'dematerialised_storage_key', v_timesheet_stage_keys[1])::text;
    END IF;
  END IF;

  FOR v_queue_record IN
    SELECT
      mq.id,
      mq.r2_key,
      mq.meta_json,
      mq.uploaded_at_utc
    FROM public.manual_timesheet_queue AS mq
    WHERE mq.status = 'STAGED'
      AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      AND UPPER(COALESCE(
        NULLIF(BTRIM(mq.meta_json ->> 'staged_kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'kind'), ''),
        NULLIF(BTRIM(mq.meta_json ->> 'attached_kind'), ''),
        'TIMESHEET'
      )) = 'TIMESHEET'
    ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
    FOR UPDATE
  LOOP
    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(
      NULLIF(BTRIM(COALESCE(v_queue_record.r2_key, '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'r2_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'storage_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'file_key', '')), ''),
      NULLIF(BTRIM(COALESCE(v_queue_record.meta_json ->> 'canonical_key', '')), ''),
      ''
    ), '^/+', ''), '');

    IF v_queue_storage_key IS NOT NULL
       AND v_queue_storage_key = ANY(v_timesheet_stage_keys)
       AND NOT (v_queue_record.id = ANY(v_duplicate_queue_ids)) THEN
      IF v_existing_active_key IS NULL THEN
        v_existing_active_key := v_queue_storage_key;
      ELSE
        v_duplicate_queue_ids := array_append(v_duplicate_queue_ids, v_queue_record.id);
      END IF;
    END IF;
  END LOOP;

  -- Complete the pre-mutation validation before the retention decision so an
  -- invoice, authorisation, Archived, stale, evidence-conflict, or invalid-actor
  -- blocker is never replaced by the financial-history explanation.
  IF p_actor_user_id IS NULL AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(v_stage_items) AS staged_item(value)
    WHERE NULLIF(BTRIM(COALESCE(staged_item.value ->> 'storage_key', '')), '') IS NOT NULL
      AND NULLIF(BTRIM(COALESCE(staged_item.value ->> 'created_by', '')), '') IS NULL
  ) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'INVALID_PAYLOAD',
      DETAIL = jsonb_build_object(
        'field', 'p_actor_user_id',
        'reason', 'actor required to recreate staged evidence provenance'
      )::text;
  END IF;

  -- Use the same sticky retained-financial-history classifier as permanent Delete.
  -- Only its marker-backed archive_required result blocks Unprocess here; the
  -- earlier weekly validation continues to own invoice, authorisation, Archive,
  -- identity, evidence and stale-row errors.
  v_history := public.timesheet_removal_financial_history_v1(
    v_all_timesheet_ids,
    ARRAY[v_booking_id]::text[],
    ARRAY[v_week.id]::uuid[]
  );

  IF COALESCE((v_history ->> 'archive_required')::boolean, false) THEN
    RAISE EXCEPTION USING
      MESSAGE = 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
      DETAIL = jsonb_build_object(
        'message', 'This timesheet has already been financially linked and cannot be unprocessed. You can archive the timesheet instead.',
        'contract_week_id', v_week.id,
        'requested_timesheet_id', p_expected_timesheet_id,
        'current_timesheet_id', v_current_ts.timesheet_id,
        'timesheet_ids', to_jsonb(v_all_timesheet_ids),
        'current_row_signature', v_current_row_signature,
        'backend_row_signature', v_current_row_signature,
        'row_signature', v_current_row_signature,
        'has_retained_financial_history', true,
        'retention_reasons', COALESCE(v_history -> 'retention_reasons', '[]'::jsonb),
        'can_unprocess', false,
        'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
        'unprocess_action_visible', true,
        'row_patch', jsonb_build_object(
          'timesheet_id', v_current_ts.timesheet_id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'row_key', 'timesheet:' || v_current_ts.timesheet_id::text,
          'row_signature', v_current_row_signature,
          'backend_row_signature', v_current_row_signature,
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        ),
        'action_flags', jsonb_build_object(
          'has_retained_financial_history', true,
          'can_unprocess', false,
          'unprocess_block_reason', 'FINANCIAL_HISTORY_PREVENTS_UNPROCESS',
          'unprocess_action_visible', true
        )
      )::text;
  END IF;


  IF COALESCE(array_length(v_duplicate_queue_ids, 1), 0) > 0 THEN
    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'DISCARDED',
           timesheet_id = NULL,
           meta_json = (
             COALESCE(mq.meta_json, '{}'::jsonb)
               - 'deferred_target_timesheet_id'
               - 'materialised_to_timesheet_id'
               - 'materialisation_deferred_to_backend'
               - 'materialisation_deferred_at_utc'
               - 'materialised_storage_key'
               - 'materialised_at_utc'
               - 'deferred_rotation_degrees'
               - 'duplicate_of_queue_item_id'
               - 'duplicate_timesheet_evidence_identity'
               - 'materialisation_noop_reason'
           ) || jsonb_build_object(
             'contract_week_id', v_week.id::text,
             'staged_kind', 'TIMESHEET',
             'duplicate_timesheet_evidence_identity', true,
             'materialisation_noop_reason', 'same_storage_key_duplicate',
             'same_storage_duplicate_deactivated_at_utc', v_now
           )
     WHERE mq.id = ANY(v_duplicate_queue_ids);

    GET DIAGNOSTICS v_repaired_same_key_duplicate_count = ROW_COUNT;
  END IF;

  FOR v_stage_item IN
    SELECT item_value.value
    FROM jsonb_array_elements(v_stage_items) AS item_value(value)
  LOOP
    v_stage_item_kind := UPPER(COALESCE(NULLIF(BTRIM(v_stage_item ->> 'kind'), ''), 'OTHER'));
    IF v_stage_item_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
      v_stage_item_kind := 'OTHER';
    END IF;
    v_stage_item_storage_key := NULLIF(regexp_replace(BTRIM(COALESCE(v_stage_item ->> 'storage_key', '')), '^/+', ''), '');
    IF v_stage_item_storage_key IS NULL THEN
      CONTINUE;
    END IF;

    v_stage_item_display_name := COALESCE(NULLIF(BTRIM(v_stage_item ->> 'display_name'), ''), regexp_replace(v_stage_item_storage_key, '^.*/', ''), 'file');
    v_stage_item_created_at := COALESCE(NULLIF(v_stage_item ->> 'created_at', '')::timestamp with time zone, v_now);
    v_stage_item_created_by := NULLIF(v_stage_item ->> 'created_by', '')::uuid;
    v_stage_item_rotation := COALESCE(NULLIF(v_stage_item ->> 'rotation_deg', '')::integer, 0);
    IF v_stage_item_rotation NOT IN (0, 90, 180, 270) THEN
      v_stage_item_rotation := 0;
    END IF;
    v_stage_item_timesheet_id := NULLIF(v_stage_item ->> 'timesheet_id', '')::uuid;
    v_stage_item_evidence_id := NULLIF(v_stage_item ->> 'evidence_id', '')::uuid;

    IF COALESCE(v_stage_item_created_by, p_actor_user_id) IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'actor required to recreate staged evidence provenance')::text;
    END IF;

    v_existing_queue := NULL;
    SELECT existing_candidate.id,
           existing_candidate.r2_key,
           existing_candidate.original_filename,
           existing_candidate.mime_type,
           existing_candidate.content_hash,
           existing_candidate.uploaded_by_user_id,
           existing_candidate.uploaded_at_utc,
           existing_candidate.status,
           existing_candidate.timesheet_id,
           existing_candidate.last_rotation_deg,
           existing_candidate.meta_json
      INTO v_existing_queue
    FROM (
      SELECT mq.*, CASE
        WHEN mq.status = 'STAGED'
         AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text THEN 1
        ELSE 0
      END AS prefer_active_staged
      FROM public.manual_timesheet_queue AS mq
      WHERE (
          mq.timesheet_id = ANY(v_all_timesheet_ids)
          OR (
            mq.status = 'STAGED'
            AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
          )
        )
        AND NULLIF(regexp_replace(COALESCE(
          NULLIF(BTRIM(COALESCE(mq.r2_key, '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'r2_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'storage_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'file_key', '')), ''),
          NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'canonical_key', '')), ''),
          ''
        ), '^/+', ''), '') = v_stage_item_storage_key
      ORDER BY prefer_active_staged DESC, mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      LIMIT 1
      FOR UPDATE
    ) AS existing_candidate;

    v_clean_meta := COALESCE(v_existing_queue.meta_json, '{}'::jsonb)
      - 'deferred_target_timesheet_id'
      - 'materialised_to_timesheet_id'
      - 'materialisation_deferred_to_backend'
      - 'materialisation_deferred_at_utc'
      - 'materialised_storage_key'
      - 'materialised_at_utc'
      - 'deferred_rotation_degrees'
      - 'duplicate_of_queue_item_id'
      - 'duplicate_timesheet_evidence_identity'
      - 'materialisation_noop_reason';

    v_merged_meta := v_clean_meta || jsonb_build_object(
      'contract_week_id', v_week.id::text,
      'staged_kind', v_stage_item_kind,
      'dematerialised_from_timesheet_id', CASE WHEN v_stage_item_timesheet_id IS NULL THEN NULL ELSE v_stage_item_timesheet_id::text END,
      'dematerialised_from_booking_id', v_booking_id,
      'dematerialised_at_utc', v_now
    );

    IF v_existing_queue.id IS NOT NULL THEN
      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'STAGED',
             timesheet_id = NULL,
             r2_key = v_stage_item_storage_key,
             original_filename = v_stage_item_display_name,
             uploaded_by_user_id = COALESCE(v_existing_queue.uploaded_by_user_id, v_stage_item_created_by, p_actor_user_id),
             uploaded_at_utc = COALESCE(v_existing_queue.uploaded_at_utc, v_stage_item_created_at, v_now),
             last_rotation_deg = COALESCE(v_stage_item_rotation, v_existing_queue.last_rotation_deg, 0)::smallint,
             meta_json = v_merged_meta
       WHERE mq.id = v_existing_queue.id;
    ELSE
      INSERT INTO public.manual_timesheet_queue (
        r2_key,
        original_filename,
        mime_type,
        content_hash,
        uploaded_by_user_id,
        uploaded_at_utc,
        status,
        timesheet_id,
        last_rotation_deg,
        meta_json
      )
      VALUES (
        v_stage_item_storage_key,
        v_stage_item_display_name,
        NULL,
        'DEMATERIALISED:' || COALESCE(v_stage_item_evidence_id::text, v_stage_item_storage_key),
        COALESCE(v_stage_item_created_by, p_actor_user_id),
        COALESCE(v_stage_item_created_at, v_now),
        'STAGED',
        NULL,
        v_stage_item_rotation::smallint,
        v_merged_meta
      );
    END IF;

    IF v_stage_item_kind = 'TIMESHEET' AND v_dematerialised_primary_timesheet_storage_key IS NULL THEN
      v_dematerialised_primary_timesheet_storage_key := v_stage_item_storage_key;
    END IF;
  END LOOP;

  v_staged_count := jsonb_array_length(v_stage_items);

  DELETE FROM public.timesheet_evidence AS ev
  WHERE ev.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_evidence_count = ROW_COUNT;

  WITH cleared AS (
    UPDATE public.pay_item_snoozes AS ps
       SET cleared_at_utc = v_now,
           cleared_by_user_id = p_actor_user_id,
           updated_at_utc = v_now,
           updated_by_user_id = p_actor_user_id
     WHERE ps.cleared_at_utc IS NULL
       AND ps.source_ref IS NULL
       AND (
         ps.timesheet_id = ANY(v_all_timesheet_ids)
         OR (v_booking_id IS NOT NULL AND ps.booking_id = v_booking_id)
       )
     RETURNING ps.id
  )
  SELECT COALESCE(array_agg(cleared.id ORDER BY cleared.id), ARRAY[]::uuid[])
    INTO v_cleared_snooze_ids
  FROM cleared;

  DELETE FROM public.ts_pdfs_outbox AS tpo
  WHERE tpo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_ts_pdf_outbox_count = ROW_COUNT;

  DELETE FROM public.ts_financials_outbox AS tfo
  WHERE tfo.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_outbox_count = ROW_COUNT;

  DELETE FROM public.timesheet_validations AS tv
  WHERE tv.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_validation_count = ROW_COUNT;

  DELETE FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = ANY(v_all_timesheet_ids);
  GET DIAGNOSTICS v_deleted_tsfin_count = ROW_COUNT;

  DELETE FROM public.timesheets AS ts
  WHERE ts.booking_id = v_booking_id;
  GET DIAGNOSTICS v_deleted_timesheet_count = ROW_COUNT;

  v_reopen_snapshot := UPPER(COALESCE(NULLIF(BTRIM(v_current_ts.submission_mode::text), ''), NULLIF(BTRIM(v_week.submission_mode_snapshot::text), ''), 'MANUAL'));
  IF v_reopen_snapshot <> 'ELECTRONIC' THEN
    v_reopen_snapshot := 'MANUAL';
  END IF;

  IF v_week.totals_json IS NOT NULL AND jsonb_typeof(v_week.totals_json) = 'object' THEN
    v_existing_totals_json := v_week.totals_json;
  ELSE
    v_existing_totals_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.additional_units_week IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_week) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(week_units.key, to_jsonb((week_units.value_text)::numeric)), '{}'::jsonb)
      INTO v_additional_units_week_json
    FROM (
      SELECT UPPER(BTRIM(week_entry.key)) AS key,
             BTRIM(week_entry.value #>> '{}') AS value_text
      FROM jsonb_each(v_current_ts.additional_units_week) AS week_entry(key, value)
      WHERE NULLIF(BTRIM(week_entry.key), '') IS NOT NULL
        AND NULLIF(BTRIM(week_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (week_entry.value #>> '{}')::numeric > 0
    ) AS week_units;
  END IF;

  IF v_current_ts.additional_units_per_day IS NOT NULL AND jsonb_typeof(v_current_ts.additional_units_per_day) = 'object' THEN
    SELECT COALESCE(jsonb_object_agg(per_code.code, per_code.day_values), '{}'::jsonb)
      INTO v_additional_units_per_day_json
    FROM (
      SELECT
        UPPER(BTRIM(code_entry.key)) AS code,
        COALESCE(jsonb_object_agg(SUBSTRING(day_entry.key FROM 1 FOR 10), to_jsonb((day_entry.value #>> '{}')::numeric)), '{}'::jsonb) AS day_values
      FROM jsonb_each(v_current_ts.additional_units_per_day) AS code_entry(key, value)
      JOIN LATERAL jsonb_each(CASE WHEN jsonb_typeof(code_entry.value) = 'object' THEN code_entry.value ELSE '{}'::jsonb END) AS day_entry(key, value) ON true
      WHERE NULLIF(BTRIM(code_entry.key), '') IS NOT NULL
        AND SUBSTRING(day_entry.key FROM 1 FOR 10) ~ '^\d{4}-\d{2}-\d{2}$'
        AND NULLIF(BTRIM(day_entry.value #>> '{}'), '') ~ '^-?[0-9]+([.][0-9]+)?$'
        AND (day_entry.value #>> '{}')::numeric > 0
      GROUP BY UPPER(BTRIM(code_entry.key))
    ) AS per_code
    WHERE jsonb_typeof(per_code.day_values) = 'object'
      AND per_code.day_values <> '{}'::jsonb;
  END IF;

  v_expenses_draft_json := jsonb_build_object(
    'mileage_units', round(COALESCE(v_current_tsfin.mileage_units, 0), 2),
    'travel_pay', round(COALESCE(v_current_tsfin.travel_pay_ex_vat, 0), 2),
    'travel_charge', round(COALESCE(v_current_tsfin.travel_charge_ex_vat, 0), 2),
    'accommodation_pay', round(COALESCE(v_current_tsfin.accommodation_pay_ex_vat, 0), 2),
    'accommodation_charge', round(COALESCE(v_current_tsfin.accommodation_charge_ex_vat, 0), 2),
    'other_pay', round(COALESCE(v_current_tsfin.other_pay_ex_vat, 0), 2),
    'other_charge', round(COALESCE(v_current_tsfin.other_charge_ex_vat, 0), 2),
    'note', COALESCE(NULLIF(BTRIM(v_current_tsfin.expenses_description), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,note}'), ''), NULLIF(BTRIM(v_existing_totals_json #>> '{expenses_draft,notes}'), ''), '')
  );

  v_reopened_totals_json := v_existing_totals_json
    || jsonb_build_object(
      'hours', jsonb_build_object(
        'day', COALESCE(v_current_tsfin.hours_day, 0),
        'night', COALESCE(v_current_tsfin.hours_night, 0),
        'sat', COALESCE(v_current_tsfin.hours_sat, 0),
        'sun', COALESCE(v_current_tsfin.hours_sun, 0),
        'bh', COALESCE(v_current_tsfin.hours_bh, 0)
      ),
      'additional_units_week', COALESCE(v_additional_units_week_json, '{}'::jsonb),
      'additional_units_per_day', COALESCE(v_additional_units_per_day_json, '{}'::jsonb),
      'expenses_draft', v_expenses_draft_json
    );

  IF v_current_ts.day_references_json IS NOT NULL AND jsonb_typeof(v_current_ts.day_references_json) = 'object' THEN
    v_reopened_day_entries_json := v_current_ts.day_references_json;
  ELSE
    v_reopened_day_entries_json := '{}'::jsonb;
  END IF;

  IF v_current_ts.actual_schedule_json IS NOT NULL AND jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN
    v_reopened_planned_schedule_json := v_current_ts.actual_schedule_json;
  ELSE
    v_reopened_planned_schedule_json := NULL;
  END IF;

  UPDATE public.contract_weeks AS cw
     SET timesheet_id = NULL,
         status = 'OPEN'::public.contract_week_status_enum,
         submission_mode_snapshot = v_reopen_snapshot::public.submission_mode_enum,
         uploaded_pdf_r2_key = v_dematerialised_primary_timesheet_storage_key,
         planned_schedule_json = v_reopened_planned_schedule_json,
         totals_json = v_reopened_totals_json,
         day_entries_json = v_reopened_day_entries_json,
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING cw.* INTO v_week;

  v_after_signature_json := public.timesheet_lifecycle_signature_v1(NULL::uuid, v_week.id, false);
  v_signature_after_text := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', '')), '');

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    'CONTRACT_WEEK_MANUAL_TIMESHEET_UNPROCESSED',
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'new_contract_week_status', v_week.status::text,
      'new_row_signature', v_signature_after_text,
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'deleted_timesheet_count', v_deleted_timesheet_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'cleared_snooze_count', COALESCE(array_length(v_cleared_snooze_ids, 1), 0)
    ),
    'WEEKLY_MANUAL_UNPROCESS',
    p_actor_user_id
  );

  RETURN jsonb_build_object(
    'ok', true,
    'operation', 'weekly_unprocess',
    'contract_week_id', v_week.id,
    'previous_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', NULL,
    'deleted_timesheet_ids', to_jsonb(v_all_timesheet_ids),
    'deleted_timesheet_count', v_deleted_timesheet_count,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'new_processing_status', 'UNPROCESSED',
    'backend_row_signature', v_signature_after_text,
    'row_signature', v_signature_after_text,
    'affected_rows', jsonb_build_array(jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'timesheet_id', NULL,
      'booking_id', v_booking_id,
      'row_key', 'contract_week:' || v_week.id::text
    )),
    'staged_evidence_summary', jsonb_build_object(
      'staged_count', v_staged_count,
      'primary_timesheet_storage_key', v_dematerialised_primary_timesheet_storage_key,
      'repaired_same_key_duplicate_count', v_repaired_same_key_duplicate_count
    ),
    'cleanup_summary', jsonb_build_object(
      'deleted_evidence_count', v_deleted_evidence_count,
      'deleted_tsfin_count', v_deleted_tsfin_count,
      'deleted_validation_count', v_deleted_validation_count,
      'deleted_ts_pdf_outbox_count', v_deleted_ts_pdf_outbox_count,
      'deleted_tsfin_outbox_count', v_deleted_tsfin_outbox_count,
      'cleared_snooze_ids', to_jsonb(v_cleared_snooze_ids)
    ),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'previous_timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_booking_id
    )
  );
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING
        MESSAGE = 'STAGED_TIMESHEET_CONFLICT',
        DETAIL = jsonb_build_object(
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'constraint_name', v_error_constraint,
          'reason', 'active_staged_timesheet_uniqueness_race'
        )::text;
    END IF;
    RAISE;
  WHEN lock_not_available THEN
    RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'expected_timesheet_id', p_expected_timesheet_id)::text;
END;
$function$;

-- contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb)
CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_week public.contract_weeks%ROWTYPE;
  v_contract public.contracts%ROWTYPE;
  v_pointer_ts public.timesheets%ROWTYPE;
  v_current_ts public.timesheets%ROWTYPE;
  v_current_tsfin public.timesheets_financials%ROWTYPE;
  v_create_json jsonb := CASE WHEN p_timesheet_create_json IS NULL THEN NULL WHEN jsonb_typeof(p_timesheet_create_json) = 'object' THEN p_timesheet_create_json ELSE NULL END;
  v_patch_json jsonb := CASE WHEN p_timesheet_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_timesheet_patch_json) = 'object' THEN p_timesheet_patch_json ELSE NULL END;
  v_week_patch_json jsonb := CASE WHEN p_contract_week_patch_json IS NULL THEN '{}'::jsonb WHEN jsonb_typeof(p_contract_week_patch_json) = 'object' THEN p_contract_week_patch_json ELSE NULL END;
  v_tsfin_snapshot_json jsonb := CASE WHEN p_tsfin_snapshot_json IS NULL THEN NULL WHEN jsonb_typeof(p_tsfin_snapshot_json) = 'object' THEN p_tsfin_snapshot_json ELSE NULL END;
  v_rotation_json jsonb := CASE WHEN p_rotation_json IS NULL THEN NULL WHEN jsonb_typeof(p_rotation_json) = 'object' THEN p_rotation_json ELSE NULL END;
  v_queue_timesheet_materialisation_json jsonb := CASE WHEN p_queue_timesheet_materialisation_json IS NULL THEN NULL WHEN jsonb_typeof(p_queue_timesheet_materialisation_json) = 'object' THEN p_queue_timesheet_materialisation_json ELSE NULL END;
  v_suppress_timesheet_evidence_materialisation boolean := false;
  v_has_selected_queue_timesheet_materialisation boolean := false;
  v_create_rec public.timesheets%ROWTYPE;
  v_patch_rec public.timesheets%ROWTYPE;
  v_week_patch_rec public.contract_weeks%ROWTYPE;
  v_before_signature_json jsonb := '{}'::jsonb;
  v_after_signature_json jsonb := '{}'::jsonb;
  v_current_row_signature text := NULL;
  v_expected_row_signature text := NULL;
  v_after_row_signature text := NULL;
  v_created_now boolean := false;
  v_was_stale boolean := false;
  v_previous_contract_week_status text := NULL;
  v_previous_processing_status text := NULL;
  v_segment_invoice_lock boolean := false;
  v_rotation_action text := NULL;
  v_rotation_new_timesheet_id uuid := NULL;
  v_rotation_pending_qr boolean := false;
  v_rotation_revoke_reason text := NULL;
  v_next_version integer := NULL;
  v_rotated_ts public.timesheets%ROWTYPE;
  v_original_booking_id text := NULL;
  v_existing_same_booking public.timesheets%ROWTYPE;
  v_queue_item public.manual_timesheet_queue%ROWTYPE;
  v_queue_kind text := NULL;
  v_queue_storage_key text := NULL;
  v_primary_timesheet_storage_key text := NULL;
  v_primary_timesheet_rotation_raw integer := 0;
  v_primary_timesheet_rotation_deg integer := 0;
  v_primary_timesheet_queue_id uuid := NULL;
  v_timesheet_stage_key_count integer := 0;
  v_attached_evidence_count integer := 0;
  v_attached_queue_count integer := 0;
  v_duplicate_queue_count integer := 0;
  v_selected_queue_id_text text := NULL;
  v_selected_queue_id uuid := NULL;
  v_selected_queue_storage_key text := NULL;
  v_selected_queue_contract_week_text text := NULL;
  v_existing_timesheet_evidence_conflict_count integer := 0;
  v_tsfin_result jsonb := '{}'::jsonb;
  v_summary_refresh_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_constraint text := NULL;
  v_temp_log_enabled boolean := false;
  v_signature_diag_json jsonb := '{}'::jsonb;
  v_diag_started_at timestamp with time zone := clock_timestamp();
  v_diag_stage_started_at timestamp with time zone := clock_timestamp();
  v_diag_elapsed_ms numeric := 0;
  v_diag_duration_ms numeric := 0;
  v_diag_step_index integer := 0;
  v_staged_evidence_loop_count integer := 0;
  v_candidate_final_state_guard jsonb := '{}'::jsonb;
  v_candidate_route_guard jsonb := '{}'::jsonb;
  v_candidate_capability_guard jsonb := '{}'::jsonb;
  v_candidate_workflow_id uuid := NULL;
  v_candidate_workflow_kind text := NULL;
  v_candidate_workflow_route text := NULL;
  v_candidate_electronic_context boolean :=
    COALESCE(current_setting('cloudtms.candidate_electronic_finalise', true), '') <> ''
    AND private._candidate_feature_enabled_current_v1('candidate_app_writes');
BEGIN
  PERFORM set_config('lock_timeout', '2500ms', true);
  PERFORM set_config('cloudtms.lifecycle_mutation_context', 'manual_timesheet_save', true);
  PERFORM set_config('cloudtms.lifecycle_defer_summary_refresh', 'on', true);
  PERFORM set_config('cloudtms.summary_refresh_mode', 'ordinary_manual_save_lightweight', true);
  PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', COALESCE(p_expected_timesheet_id::text, ''), true);

  BEGIN
    SELECT COALESCE(sd.temp_log, false)
      INTO v_temp_log_enabled
    FROM public.settings_defaults AS sd
    ORDER BY sd.id
    LIMIT 1;
  EXCEPTION
    WHEN undefined_table OR undefined_column THEN
      v_temp_log_enabled := false;
    WHEN OTHERS THEN
      v_temp_log_enabled := false;
  END;

  IF p_week_id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_week_id')::text;
  END IF;
  IF p_timesheet_patch_json IS NOT NULL AND v_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_patch_json')::text;
  END IF;
  IF p_contract_week_patch_json IS NOT NULL AND v_week_patch_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_contract_week_patch_json')::text;
  END IF;
  IF p_timesheet_create_json IS NOT NULL AND v_create_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json')::text;
  END IF;
  IF NOT v_candidate_electronic_context AND (
       COALESCE(v_create_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
       OR COALESCE(v_patch_json,'{}'::jsonb) ?| ARRAY[
         'candidate_workflow_id','candidate_workflow_generation','candidate_manager_approved_at_utc']
     ) THEN
    RAISE EXCEPTION USING MESSAGE='CANDIDATE_FINALISE_CONTEXT_REQUIRED';
  END IF;
  IF p_tsfin_snapshot_json IS NOT NULL AND v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF p_rotation_json IS NOT NULL AND v_rotation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json')::text;
  END IF;
  IF p_queue_timesheet_materialisation_json IS NOT NULL AND v_queue_timesheet_materialisation_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_queue_timesheet_materialisation_json')::text;
  END IF;

  IF v_queue_timesheet_materialisation_json IS NOT NULL THEN
    v_suppress_timesheet_evidence_materialisation := LOWER(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'suppress_timesheet_evidence_materialisation', v_queue_timesheet_materialisation_json ->> 'suppressTimesheetEvidenceMaterialisation', ''))) IN ('true','1','yes','y','on');
    v_has_selected_queue_timesheet_materialisation := NOT v_suppress_timesheet_evidence_materialisation
      AND NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '') IS NOT NULL;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'entry_payload_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'has_create_json', v_create_json IS NOT NULL,
          'has_patch_json', v_patch_json <> '{}'::jsonb,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb,
          'has_tsfin_snapshot_json', v_tsfin_snapshot_json IS NOT NULL,
          'has_rotation_json', v_rotation_json IS NOT NULL,
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_queue_timesheet_materialisation_json', v_queue_timesheet_materialisation_json IS NOT NULL,
          'suppress_timesheet_evidence_materialisation', v_suppress_timesheet_evidence_materialisation,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'expected_row_signature_present', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), '') IS NOT NULL
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT cw.*
    INTO v_week
  FROM public.contract_weeks AS cw
  WHERE cw.id = p_week_id
  FOR UPDATE;

  IF v_week.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', p_week_id)::text;
  END IF;
  v_previous_contract_week_status := v_week.status::text;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text,
          'contract_week_timesheet_id', v_week.timesheet_id,
          'week_ending_date', v_week.week_ending_date,
          'previous_contract_week_status', v_previous_contract_week_status
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM pg_advisory_xact_lock(hashtext('contract_week_staged_timesheet:' || v_week.id::text));

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_timesheet_advisory_lock_acquired',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'advisory_lock_scope', 'contract_week_staged_timesheet',
          'contract_id', v_week.contract_id,
          'week_status', v_week.status::text
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT c.*
    INTO v_contract
  FROM public.contracts AS c
  WHERE c.id = v_week.contract_id
  FOR UPDATE;

  IF v_contract.id IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_id', v_week.contract_id)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_locked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_id', v_contract.id,
          'candidate_id', v_contract.candidate_id,
          'client_id', v_contract.client_id,
          'pay_method_snapshot', v_contract.pay_method_snapshot,
          'default_submission_mode', v_contract.default_submission_mode
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.status IN ('AUTHORISED'::public.contract_week_status_enum, 'INVOICED'::public.contract_week_status_enum, 'CANCELLED'::public.contract_week_status_enum) THEN
    RAISE EXCEPTION USING
      MESSAGE = CASE WHEN v_week.status = 'INVOICED'::public.contract_week_status_enum THEN 'INVOICED_OR_LOCKED' ELSE 'TIMESHEET_LOCKED_OR_PAID' END,
      DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'contract_week_status', v_week.status::text)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_status_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_status', v_week.status::text,
          'locked_status_gate_passed', true
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_week.timesheet_id IS NOT NULL THEN
    SELECT ts.*
      INTO v_pointer_ts
    FROM public.timesheets AS ts
    WHERE ts.timesheet_id = v_week.timesheet_id
    FOR UPDATE;

    IF v_pointer_ts.timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TARGET_NOT_FOUND', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_week.timesheet_id)::text;
    END IF;

    IF COALESCE(v_pointer_ts.is_current, false) THEN
      v_current_ts := v_pointer_ts;
    ELSE
      SELECT ts.*
        INTO v_current_ts
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_pointer_ts.booking_id
        AND ts.is_current = true
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      IF v_current_ts.timesheet_id IS NULL THEN
        v_current_ts := v_pointer_ts;
      ELSE
        v_was_stale := true;
      END IF;
    END IF;

    IF p_expected_timesheet_id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_expected_timesheet_id')::text;
    END IF;
    IF p_expected_timesheet_id IS DISTINCT FROM v_current_ts.timesheet_id THEN
      RAISE EXCEPTION USING
        MESSAGE = 'EXPECTED_TIMESHEET_MISMATCH',
        DETAIL = jsonb_build_object('expected_timesheet_id', p_expected_timesheet_id, 'current_timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_timesheet_resolved',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_pointer_timesheet_id', v_week.timesheet_id,
          'pointer_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'booking_id', CASE WHEN v_current_ts.booking_id IS NULL THEN NULL ELSE v_current_ts.booking_id END,
          'timesheet_version', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.version END,
          'timesheet_is_current', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.is_current END,
          'was_stale', v_was_stale
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    IF v_current_ts.archived_at_utc IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_ARCHIVED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;
    IF v_current_ts.authorised_at_server IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'ALREADY_AUTHORISED', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'contract_week_id', v_week.id)::text;
    END IF;

    SELECT tf.*
      INTO v_current_tsfin
    FROM public.timesheets_financials AS tf
    WHERE tf.timesheet_id = v_current_ts.timesheet_id
      AND tf.is_current = true
    ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_current_tsfin.id IS NOT NULL THEN
      v_previous_processing_status := v_current_tsfin.processing_status::text;
      IF v_current_tsfin.locked_by_invoice_id IS NOT NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'invoice_id', v_current_tsfin.locked_by_invoice_id)::text;
      END IF;

      SELECT EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
          CASE
            WHEN v_current_tsfin.invoice_breakdown_json IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'array' THEN v_current_tsfin.invoice_breakdown_json
            WHEN jsonb_typeof(v_current_tsfin.invoice_breakdown_json) = 'object'
             AND jsonb_typeof(v_current_tsfin.invoice_breakdown_json -> 'segments') = 'array' THEN v_current_tsfin.invoice_breakdown_json -> 'segments'
            ELSE '[]'::jsonb
          END
        ) AS invoice_segment(segment_json)
        WHERE NULLIF(BTRIM(COALESCE(invoice_segment.segment_json ->> 'invoice_locked_invoice_id', '')), '') IS NOT NULL
      ) INTO v_segment_invoice_lock;

      IF COALESCE(v_segment_invoice_lock, false) THEN
        RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_LOCKED_BY_INVOICE', DETAIL = jsonb_build_object('timesheet_id', v_current_ts.timesheet_id, 'lock_scope', 'segment')::text;
      END IF;
    END IF;
  END IF;

  IF v_candidate_electronic_context THEN
    BEGIN
      v_candidate_workflow_id := split_part(
        current_setting('cloudtms.candidate_electronic_finalise', true),
        ':',
        1
      )::uuid;
    EXCEPTION WHEN invalid_text_representation THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_CONTEXT_INVALID' USING ERRCODE='42501';
    END;

    SELECT upper(w.workflow_kind),upper(w.route)
      INTO v_candidate_workflow_kind,v_candidate_workflow_route
    FROM public.candidate_submission_workflows AS w
    WHERE w.id=v_candidate_workflow_id
      AND w.contract_week_id=v_week.id
      AND w.state IN ('READY_TO_FINALISE','RECEIVED')
    FOR SHARE;

    IF NOT FOUND OR v_candidate_workflow_kind NOT IN ('CONTRACT_HOURS','CONTRACT_COMBINED') THEN
      RAISE EXCEPTION 'CANDIDATE_FINALISE_WORKFLOW_INVALID' USING ERRCODE='42501';
    END IF;

    v_candidate_route_guard := private._candidate_route_family_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      v_week.id
    );
    v_candidate_capability_guard := private._candidate_record_capabilities_v1(
      CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
      v_week.id
    );

    IF NOT COALESCE((v_candidate_capability_guard->>'can_edit_hours')::boolean,false)
       OR v_candidate_route_guard->>'route_family' IN ('IMPORT_AUTHORITATIVE','MANUAL_NON_QR')
       OR (v_candidate_workflow_route='ELECTRONIC'
           AND v_candidate_route_guard->>'route_family'<>'ELECTRONIC')
       OR (v_candidate_workflow_route='PAPER'
           AND NOT COALESCE(
             (v_candidate_route_guard->>'candidate_paper_submission_allowed')::boolean,
             false
           )) THEN
      RAISE EXCEPTION 'CANDIDATE_ROUTE_NOT_ALLOWED'
        USING ERRCODE='42501',
              DETAIL=jsonb_build_object(
                'workflow_id',v_candidate_workflow_id,
                'workflow_route',v_candidate_workflow_route,
                'route_family',v_candidate_route_guard->>'route_family'
              )::text;
    END IF;
  END IF;

  v_candidate_final_state_guard := private._candidate_weekly_final_state_guard_v1(
    v_week.id,
    CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
    v_create_json,
    v_patch_json,
    v_tsfin_snapshot_json
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_and_lock_gate_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_tsfin_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.id END,
          'previous_processing_status', v_previous_processing_status,
          'processing_status', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.processing_status::text END,
          'paid_at_utc', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.paid_at_utc END,
          'locked_by_invoice_id', CASE WHEN v_current_tsfin.id IS NULL THEN NULL ELSE v_current_tsfin.locked_by_invoice_id END,
          'segment_invoice_lock', v_segment_invoice_lock
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'before_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature_input', NULLIF(BTRIM(COALESCE(p_expected_row_signature, '')), ''),
          'patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'backend_row_signature', '')), ''),
          'patch_row_signature', NULLIF(BTRIM(COALESCE(v_patch_json ->> 'row_signature', '')), ''),
          'week_patch_backend_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'backend_row_signature', '')), ''),
          'week_patch_row_signature', NULLIF(BTRIM(COALESCE(v_week_patch_json ->> 'row_signature', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_before_signature_json := public.timesheet_lifecycle_guard_signature_v1(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_current_row_signature := NULLIF(BTRIM(COALESCE(v_before_signature_json ->> 'backend_row_signature', v_before_signature_json ->> 'row_signature', v_before_signature_json ->> 'signature', '')), '');
  v_expected_row_signature := NULLIF(BTRIM(COALESCE(p_expected_row_signature, v_patch_json ->> 'backend_row_signature', v_patch_json ->> 'row_signature', v_week_patch_json ->> 'backend_row_signature', v_week_patch_json ->> 'row_signature', '')), '');

  IF v_expected_row_signature IS NOT NULL AND COALESCE(v_current_row_signature, '') IS DISTINCT FROM v_expected_row_signature THEN
    IF COALESCE(v_temp_log_enabled, false) THEN
      PERFORM public._temp_diag_log(
        'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, v_week.id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TIMESHEET_LIFECYCLE_SIGNATURE_DIAG',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_mismatch_before_manual_upsert',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
          'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
          'contract_week_id', v_week.id,
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'expected_row_signature', v_expected_row_signature,
          'current_row_signature', v_current_row_signature,
          'current_signature_payload', v_before_signature_json
        ))
      );
    END IF;
    RAISE EXCEPTION USING
      MESSAGE = 'ROW_SIGNATURE_MISMATCH',
      DETAIL = jsonb_build_object('expected_row_signature', v_expected_row_signature, 'current_row_signature', v_current_row_signature, 'contract_week_id', v_week.id, 'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'row_signature_guard_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id END,
          'current_row_signature', v_current_row_signature,
          'expected_row_signature', v_expected_row_signature,
          'expected_row_signature_present', v_expected_row_signature IS NOT NULL,
          'signature_match', CASE WHEN v_expected_row_signature IS NULL THEN true ELSE COALESCE(v_current_row_signature, '') IS NOT DISTINCT FROM v_expected_row_signature END
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NULL THEN
    IF v_create_json IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_timesheet_create_json', 'reason', 'required_when_no_current_timesheet')::text;
    END IF;

    v_create_rec := jsonb_populate_record(NULL::public.timesheets, v_create_json);
    IF NULLIF(BTRIM(COALESCE(v_create_rec.booking_id, '')), '') IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'booking_id')::text;
    END IF;
    IF COALESCE(v_create_rec.week_ending_date, v_week.week_ending_date) IS DISTINCT FROM v_week.week_ending_date THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'week_ending_date', 'expected_value', v_week.week_ending_date)::text;
    END IF;
    IF COALESCE(v_create_rec.contract_id, v_week.contract_id) IS DISTINCT FROM v_week.contract_id THEN
      RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'contract_id', 'expected_value', v_week.contract_id)::text;
    END IF;

    v_original_booking_id := NULLIF(BTRIM(v_create_rec.booking_id), '');
    PERFORM pg_advisory_xact_lock(hashtext(v_original_booking_id));

    SELECT ts.*
      INTO v_existing_same_booking
    FROM public.timesheets AS ts
    WHERE ts.booking_id = v_original_booking_id
      AND ts.contract_id = v_week.contract_id
      AND ts.week_ending_date = v_week.week_ending_date
      AND ts.is_current = true
    ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
    LIMIT 1
    FOR UPDATE;

    IF v_existing_same_booking.timesheet_id IS NOT NULL THEN
      v_current_ts := v_existing_same_booking;
      v_was_stale := true;
      v_patch_json := (v_create_json - 'timesheet_id' - 'booking_id' - 'version' - 'is_current' - 'contract_id' - 'week_ending_date' - 'created_at') || COALESCE(v_patch_json, '{}'::jsonb);
    ELSE
      SELECT COALESCE(MAX(ts.version), 0) + 1
        INTO v_create_rec.version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_original_booking_id;

      INSERT INTO public.timesheets (
        timesheet_id,
        booking_id,
        occupant_key_norm,
        hospital_norm,
        ward_norm,
        job_title_norm,
        shift_label_norm,
        week_ending_date,
        authorised_at_server,
        auth_name,
        auth_job_title,
        r2_nurse_key,
        r2_auth_key,
        img_sha256_nurse,
        img_sha256_auth,
        candidate_workflow_id,
        candidate_workflow_generation,
        candidate_manager_approved_at_utc,
        status,
        created_at,
        updated_at,
        version,
        is_current,
        contract_id,
        submission_mode,
        manual_pdf_r2_key,
        line_type,
        sheet_scope,
        actual_schedule_json,
        additional_units_week,
        additional_units_per_day,
        qr_token,
        qr_status,
        qr_payload_json,
        qr_generated_at,
        qr_scanned_at,
        qr_scan_info_json,
        qr_r2_key,
        day_references_json,
        manual_pdf_rotation_degrees,
        qr_last_sent_hash,
        qr_last_sent_at_utc,
        qr_signed_hash,
        qr_signed_at_utc,
        candidate_hint_text,
        band,
        is_adjustment
      )
      VALUES (
        COALESCE(v_create_rec.timesheet_id, gen_random_uuid()),
        v_original_booking_id,
        COALESCE(v_create_rec.occupant_key_norm, ''),
        COALESCE(v_create_rec.hospital_norm, ''),
        COALESCE(v_create_rec.ward_norm, ''),
        COALESCE(v_create_rec.job_title_norm, ''),
        COALESCE(v_create_rec.shift_label_norm, 'weekly'),
        v_week.week_ending_date,
        v_create_rec.authorised_at_server,
        v_create_rec.auth_name,
        v_create_rec.auth_job_title,
        v_create_rec.r2_nurse_key,
        v_create_rec.r2_auth_key,
        v_create_rec.img_sha256_nurse,
        v_create_rec.img_sha256_auth,
        v_create_rec.candidate_workflow_id,
        v_create_rec.candidate_workflow_generation,
        v_create_rec.candidate_manager_approved_at_utc,
        COALESCE(v_create_rec.status, 'RECEIVED'::public.timesheet_status_enum),
        COALESCE(v_create_rec.created_at, v_now),
        v_now,
        COALESCE(v_create_rec.version, 1),
        true,
        v_week.contract_id,
        COALESCE(v_create_rec.submission_mode, 'MANUAL'::public.submission_mode_enum),
        v_create_rec.manual_pdf_r2_key,
        COALESCE(v_create_rec.line_type, 'HOURS'::public.timesheet_line_type_enum),
        COALESCE(v_create_rec.sheet_scope, 'WEEKLY'::public.timesheet_scope_enum),
        COALESCE(v_create_rec.actual_schedule_json, '[]'::jsonb),
        COALESCE(v_create_rec.additional_units_week, '{}'::jsonb),
        COALESCE(v_create_rec.additional_units_per_day, '{}'::jsonb),
        v_create_rec.qr_token,
        v_create_rec.qr_status,
        COALESCE(v_create_rec.qr_payload_json, '{}'::jsonb),
        v_create_rec.qr_generated_at,
        v_create_rec.qr_scanned_at,
        v_create_rec.qr_scan_info_json,
        v_create_rec.qr_r2_key,
        v_create_rec.day_references_json,
        COALESCE(v_create_rec.manual_pdf_rotation_degrees, 0),
        v_create_rec.qr_last_sent_hash,
        v_create_rec.qr_last_sent_at_utc,
        v_create_rec.qr_signed_hash,
        v_create_rec.qr_signed_at_utc,
        v_create_rec.candidate_hint_text,
        v_create_rec.band,
        COALESCE(v_create_rec.is_adjustment, COALESCE(v_week.is_adjustment, false))
      )
      RETURNING * INTO v_current_ts;
      v_created_now := true;
    END IF;
  ELSE
    IF v_rotation_json IS NOT NULL THEN
      IF p_actor_user_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_actor_user_id', 'reason', 'required_for_rotation')::text;
      END IF;
      v_rotation_action := UPPER(NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'qr_action', '')), ''));
      IF v_rotation_action NOT IN ('INVALIDATE', 'REISSUE', 'REVOKE_TO_MANUAL') THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.qr_action')::text;
      END IF;
      v_rotation_new_timesheet_id := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'new_timesheet_id', '')), '')::uuid;
      IF v_rotation_new_timesheet_id IS NULL THEN
        RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_rotation_json.new_timesheet_id')::text;
      END IF;
      v_rotation_pending_qr := v_rotation_action IN ('INVALIDATE', 'REISSUE');
      v_rotation_revoke_reason := NULLIF(BTRIM(COALESCE(v_rotation_json ->> 'revoke_reason', '')), '');
      PERFORM pg_advisory_xact_lock(hashtext(v_current_ts.booking_id));
      SELECT ts.version
        INTO v_next_version
      FROM public.timesheets AS ts
      WHERE ts.booking_id = v_current_ts.booking_id
      ORDER BY ts.version DESC NULLS LAST, ts.updated_at DESC NULLS LAST, ts.created_at DESC NULLS LAST, ts.timesheet_id DESC
      LIMIT 1
      FOR UPDATE;
      v_next_version := COALESCE(v_next_version, COALESCE(v_current_ts.version, 1)) + 1;

      UPDATE public.timesheets AS ts
         SET is_current = false,
             status = 'REVOKED'::public.timesheet_status_enum,
             revoked_reason = v_rotation_revoke_reason,
             revoked_by = p_actor_user_id::text,
             updated_at = v_now
       WHERE ts.timesheet_id = v_current_ts.timesheet_id
         AND ts.is_current = true;

      v_rotated_ts := v_current_ts;
      v_rotated_ts.timesheet_id := v_rotation_new_timesheet_id;
      v_rotated_ts.version := v_next_version;
      v_rotated_ts.is_current := true;
      v_rotated_ts.status := 'RECEIVED'::public.timesheet_status_enum;
      v_rotated_ts.revoked_at := NULL;
      v_rotated_ts.revoked_reason := NULL;
      v_rotated_ts.revoked_by := NULL;
      v_rotated_ts.authorised_at_server := NULL;
      v_rotated_ts.auth_name := NULL;
      v_rotated_ts.auth_job_title := NULL;
      v_rotated_ts.r2_nurse_key := NULL;
      v_rotated_ts.r2_auth_key := NULL;
      v_rotated_ts.img_sha256_nurse := NULL;
      v_rotated_ts.img_sha256_auth := NULL;
      v_rotated_ts.candidate_workflow_id := NULL;
      v_rotated_ts.candidate_workflow_generation := NULL;
      v_rotated_ts.candidate_manager_approved_at_utc := NULL;
      v_rotated_ts.qr_token := NULL;
      v_rotated_ts.qr_status := CASE WHEN v_rotation_pending_qr THEN 'PENDING'::public.timesheet_qr_status_enum ELSE NULL END;
      v_rotated_ts.qr_payload_json := '{}'::jsonb;
      v_rotated_ts.qr_generated_at := NULL;
      v_rotated_ts.qr_scanned_at := NULL;
      v_rotated_ts.qr_scan_info_json := NULL;
      v_rotated_ts.qr_r2_key := NULL;
      v_rotated_ts.qr_last_sent_hash := NULL;
      v_rotated_ts.qr_last_sent_at_utc := NULL;
      v_rotated_ts.qr_signed_hash := NULL;
      v_rotated_ts.qr_signed_at_utc := NULL;
      v_rotated_ts.manual_pdf_r2_key := NULL;
      v_rotated_ts.created_at := v_now;
      v_rotated_ts.updated_at := v_now;
      INSERT INTO public.timesheets SELECT (v_rotated_ts).* RETURNING * INTO v_current_ts;
    END IF;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_identity_ready_for_patch',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'rotation_action', v_rotation_action,
          'rotation_new_timesheet_id', v_rotation_new_timesheet_id,
          'rotation_pending_qr', v_rotation_pending_qr,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_current_ts.timesheet_id IS NOT NULL THEN
    PERFORM set_config('cloudtms.lifecycle_target_timesheet_id', v_current_ts.timesheet_id::text, true);
  END IF;

  IF v_patch_json <> '{}'::jsonb THEN
    v_patch_rec := jsonb_populate_record(v_current_ts, v_patch_json);
    UPDATE public.timesheets AS ts
       SET occupant_key_norm = COALESCE(v_patch_rec.occupant_key_norm, ts.occupant_key_norm),
           hospital_norm = COALESCE(v_patch_rec.hospital_norm, ts.hospital_norm),
           ward_norm = COALESCE(v_patch_rec.ward_norm, ts.ward_norm),
           job_title_norm = COALESCE(v_patch_rec.job_title_norm, ts.job_title_norm),
           shift_label_norm = v_patch_rec.shift_label_norm,
           authorised_at_server = v_patch_rec.authorised_at_server,
           auth_name = v_patch_rec.auth_name,
           auth_job_title = v_patch_rec.auth_job_title,
           r2_nurse_key = v_patch_rec.r2_nurse_key,
           r2_auth_key = v_patch_rec.r2_auth_key,
           img_sha256_nurse = v_patch_rec.img_sha256_nurse,
           img_sha256_auth = v_patch_rec.img_sha256_auth,
           candidate_workflow_id = v_patch_rec.candidate_workflow_id,
           candidate_workflow_generation = v_patch_rec.candidate_workflow_generation,
           candidate_manager_approved_at_utc = v_patch_rec.candidate_manager_approved_at_utc,
           status = COALESCE(v_patch_rec.status, ts.status),
           submission_mode = COALESCE(v_patch_rec.submission_mode, ts.submission_mode),
           manual_pdf_r2_key = v_patch_rec.manual_pdf_r2_key,
           line_type = COALESCE(v_patch_rec.line_type, ts.line_type),
           sheet_scope = COALESCE(v_patch_rec.sheet_scope, ts.sheet_scope),
           actual_schedule_json = COALESCE(v_patch_rec.actual_schedule_json, ts.actual_schedule_json, '[]'::jsonb),
           additional_units_week = COALESCE(v_patch_rec.additional_units_week, ts.additional_units_week, '{}'::jsonb),
           additional_units_per_day = COALESCE(v_patch_rec.additional_units_per_day, ts.additional_units_per_day, '{}'::jsonb),
           qr_token = v_patch_rec.qr_token,
           qr_status = v_patch_rec.qr_status,
           qr_payload_json = COALESCE(v_patch_rec.qr_payload_json, '{}'::jsonb),
           qr_generated_at = v_patch_rec.qr_generated_at,
           qr_scanned_at = v_patch_rec.qr_scanned_at,
           qr_scan_info_json = v_patch_rec.qr_scan_info_json,
           qr_r2_key = v_patch_rec.qr_r2_key,
           day_references_json = v_patch_rec.day_references_json,
           manual_pdf_rotation_degrees = COALESCE(v_patch_rec.manual_pdf_rotation_degrees, ts.manual_pdf_rotation_degrees, 0),
           qr_last_sent_hash = v_patch_rec.qr_last_sent_hash,
           qr_last_sent_at_utc = v_patch_rec.qr_last_sent_at_utc,
           qr_signed_hash = v_patch_rec.qr_signed_hash,
           qr_signed_at_utc = v_patch_rec.qr_signed_at_utc,
           candidate_hint_text = v_patch_rec.candidate_hint_text,
           band = COALESCE(v_patch_rec.band, ts.band),
           is_adjustment = COALESCE(v_patch_rec.is_adjustment, ts.is_adjustment),
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'timesheet_patch_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'booking_id', v_current_ts.booking_id,
          'timesheet_version', v_current_ts.version,
          'timesheet_status', v_current_ts.status::text,
          'submission_mode', v_current_ts.submission_mode::text,
          'sheet_scope', v_current_ts.sheet_scope::text,
          'line_type', v_current_ts.line_type::text,
          'actual_schedule_count', CASE WHEN jsonb_typeof(v_current_ts.actual_schedule_json) = 'array' THEN jsonb_array_length(v_current_ts.actual_schedule_json) ELSE NULL END,
          'has_patch_json', v_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence AND v_has_selected_queue_timesheet_materialisation THEN
    v_selected_queue_id_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'queue_id', v_queue_timesheet_materialisation_json ->> 'queueId', '')), '');
    v_selected_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'storageKey', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'r2_key', '')), ''), ''), '^/+', ''), '');
    v_selected_queue_contract_week_text := NULLIF(BTRIM(COALESCE(v_queue_timesheet_materialisation_json ->> 'contract_week_id', v_queue_timesheet_materialisation_json ->> 'contractWeekId', '')), '');

    IF v_selected_queue_id_text IS NULL OR v_selected_queue_storage_key IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'missing_queue_identity')::text;
    END IF;

    BEGIN
      v_selected_queue_id := v_selected_queue_id_text::uuid;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id_text, 'storage_key', v_selected_queue_storage_key, 'reason', 'invalid_queue_id')::text;
    END;

    IF v_selected_queue_contract_week_text IS NOT NULL AND v_selected_queue_contract_week_text <> v_week.id::text THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'supplied_contract_week_id', v_selected_queue_contract_week_text, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'active_contract_week_mismatch')::text;
    END IF;

    SELECT mq.*
      INTO v_queue_item
      FROM public.manual_timesheet_queue AS mq
     WHERE mq.id = v_selected_queue_id
     FOR UPDATE;

    IF v_queue_item.id IS NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'PREVIEW_QUEUE_IMAGE_MISSING', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'queue_row_not_found')::text;
    END IF;

    v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
    IF v_queue_storage_key IS NULL OR v_queue_storage_key IS DISTINCT FROM v_selected_queue_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_STORAGE_MISMATCH', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'expected_storage_key', v_selected_queue_storage_key, 'actual_storage_key', v_queue_storage_key)::text;
    END IF;

    IF UPPER(COALESCE(v_queue_item.status, '')) <> 'QUEUED' OR v_queue_item.timesheet_id IS NOT NULL THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'status', v_queue_item.status, 'timesheet_id', v_queue_item.timesheet_id)::text;
    END IF;

    v_queue_kind := 'TIMESHEET';

    IF v_primary_timesheet_storage_key IS NULL THEN
      v_primary_timesheet_storage_key := v_queue_storage_key;
      v_primary_timesheet_queue_id := v_queue_item.id;
      v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
      v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
    ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
    END IF;
    v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;

    SELECT COUNT(*)
      INTO v_existing_timesheet_evidence_conflict_count
      FROM public.timesheet_evidence AS te
     WHERE te.timesheet_id = v_current_ts.timesheet_id
       AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
       AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') IS DISTINCT FROM v_queue_storage_key;
    IF COALESCE(v_existing_timesheet_evidence_conflict_count, 0) > 0 THEN
      RAISE EXCEPTION USING MESSAGE = 'TIMESHEET_EVIDENCE_ALREADY_EXISTS', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'timesheet_id', v_current_ts.timesheet_id, 'queue_id', v_queue_item.id, 'storage_key', v_queue_storage_key)::text;
    END IF;

    IF NOT EXISTS (
      SELECT 1
        FROM public.timesheet_evidence AS te
       WHERE te.timesheet_id = v_current_ts.timesheet_id
         AND UPPER(COALESCE(te.kind, '')) = 'TIMESHEET'
         AND NULLIF(regexp_replace(COALESCE(te.storage_key, ''), '^/+', ''), '') = v_queue_storage_key
    ) THEN
      INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
      VALUES (v_current_ts.timesheet_id, 'TIMESHEET', v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
      v_attached_evidence_count := v_attached_evidence_count + 1;
    ELSE
      v_duplicate_queue_count := v_duplicate_queue_count + 1;
    END IF;

    UPDATE public.manual_timesheet_queue AS mq
       SET status = 'ATTACHED',
           timesheet_id = v_current_ts.timesheet_id,
           r2_key = v_queue_storage_key,
           meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
             || jsonb_build_object(
               'contract_week_id', v_week.id::text,
               'staged_kind', 'TIMESHEET',
               'selected_queue_timesheet_materialisation', true,
               'materialisation_deferred_to_backend', false,
               'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
               'materialised_at_utc', v_now,
               'materialised_storage_key', v_queue_storage_key,
               'materialised_from_process_preview', true,
               'preview_selection_key', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_selection_key', v_queue_timesheet_materialisation_json ->> 'previewSelectionKey'),
               'preview_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'preview_identity', v_queue_timesheet_materialisation_json ->> 'previewIdentity'),
               'active_identity', COALESCE(v_queue_timesheet_materialisation_json ->> 'active_identity', v_queue_timesheet_materialisation_json ->> 'activeIdentity')
             )
     WHERE mq.id = v_queue_item.id
       AND mq.status = 'QUEUED'
       AND mq.timesheet_id IS NULL;
    IF NOT FOUND THEN
      RAISE EXCEPTION USING MESSAGE = 'QUEUE_ITEM_NOT_AVAILABLE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_selected_queue_id, 'storage_key', v_selected_queue_storage_key, 'reason', 'conditional_attach_failed')::text;
    END IF;
    v_attached_queue_count := v_attached_queue_count + 1;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'selected_queue_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'has_selected_queue_timesheet_materialisation', v_has_selected_queue_timesheet_materialisation,
          'selected_queue_id', v_selected_queue_id,
          'selected_queue_storage_key', v_selected_queue_storage_key,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF p_materialise_staged_evidence THEN
    FOR v_queue_item IN
      SELECT mq.*
      FROM public.manual_timesheet_queue AS mq
      WHERE mq.status = 'STAGED'
        AND NULLIF(BTRIM(COALESCE(mq.meta_json ->> 'contract_week_id', '')), '') = v_week.id::text
      ORDER BY mq.uploaded_at_utc ASC NULLS LAST, mq.id ASC
      FOR UPDATE
    LOOP
      v_staged_evidence_loop_count := v_staged_evidence_loop_count + 1;
      v_queue_kind := UPPER(COALESCE(NULLIF(BTRIM(v_queue_item.meta_json ->> 'staged_kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'kind'), ''), NULLIF(BTRIM(v_queue_item.meta_json ->> 'attached_kind'), ''), 'TIMESHEET'));
      IF v_queue_kind NOT IN ('TIMESHEET','MILEAGE','TRAVEL','ACCOMMODATION','OTHER') THEN
        v_queue_kind := 'OTHER';
      END IF;
      IF v_queue_kind = 'TIMESHEET' AND (v_suppress_timesheet_evidence_materialisation OR v_has_selected_queue_timesheet_materialisation) THEN
        CONTINUE;
      END IF;
      v_queue_storage_key := NULLIF(regexp_replace(COALESCE(NULLIF(BTRIM(COALESCE(v_queue_item.r2_key, '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'r2_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'storage_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'file_key', '')), ''), NULLIF(BTRIM(COALESCE(v_queue_item.meta_json ->> 'canonical_key', '')), ''), ''), '^/+', ''), '');
      IF v_queue_storage_key IS NULL THEN
        IF v_queue_kind = 'TIMESHEET' THEN
          RAISE EXCEPTION USING MESSAGE = 'INVALID_TIMESHEET_EVIDENCE', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'queue_id', v_queue_item.id, 'reason', 'missing_storage_key')::text;
        END IF;
        CONTINUE;
      END IF;

      IF v_queue_kind = 'TIMESHEET' THEN
        IF v_primary_timesheet_storage_key IS NULL THEN
          v_primary_timesheet_storage_key := v_queue_storage_key;
          v_primary_timesheet_queue_id := v_queue_item.id;
          v_primary_timesheet_rotation_raw := ((COALESCE(v_queue_item.last_rotation_deg, 0)::integer % 360) + 360) % 360;
          v_primary_timesheet_rotation_deg := CASE WHEN v_primary_timesheet_rotation_raw >= 315 OR v_primary_timesheet_rotation_raw < 45 THEN 0 WHEN v_primary_timesheet_rotation_raw >= 45 AND v_primary_timesheet_rotation_raw < 135 THEN 90 WHEN v_primary_timesheet_rotation_raw >= 135 AND v_primary_timesheet_rotation_raw < 225 THEN 180 ELSE 270 END;
        ELSIF v_queue_storage_key IS DISTINCT FROM v_primary_timesheet_storage_key THEN
          RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', v_week.id, 'existing_storage_key', v_primary_timesheet_storage_key, 'conflicting_storage_key', v_queue_storage_key, 'queue_id', v_queue_item.id)::text;
        END IF;
        v_timesheet_stage_key_count := v_timesheet_stage_key_count + 1;
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM public.timesheet_evidence AS te
        WHERE te.timesheet_id = v_current_ts.timesheet_id
          AND te.kind = v_queue_kind
          AND te.storage_key = v_queue_storage_key
      ) THEN
        INSERT INTO public.timesheet_evidence(timesheet_id, kind, display_name, storage_key, created_at, created_by)
        VALUES (v_current_ts.timesheet_id, v_queue_kind, v_queue_item.original_filename, v_queue_storage_key, COALESCE(v_queue_item.uploaded_at_utc, v_now), COALESCE(v_queue_item.uploaded_by_user_id, p_actor_user_id));
        v_attached_evidence_count := v_attached_evidence_count + 1;
      ELSE
        v_duplicate_queue_count := v_duplicate_queue_count + 1;
      END IF;

      UPDATE public.manual_timesheet_queue AS mq
         SET status = 'ATTACHED',
             timesheet_id = v_current_ts.timesheet_id,
             r2_key = v_queue_storage_key,
             meta_json = (COALESCE(mq.meta_json, '{}'::jsonb) - 'deferred_target_timesheet_id' - 'materialisation_deferred_at_utc' - 'deferred_rotation_degrees' - 'dematerialised_from_timesheet_id' - 'dematerialised_from_booking_id' - 'dematerialised_at_utc')
               || jsonb_build_object(
                 'contract_week_id', v_week.id::text,
                 'staged_kind', v_queue_kind,
                 'materialisation_deferred_to_backend', false,
                 'materialised_to_timesheet_id', v_current_ts.timesheet_id::text,
                 'materialised_at_utc', v_now,
                 'materialised_storage_key', v_queue_storage_key,
                 'duplicate_timesheet_evidence_identity', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN true ELSE false END,
                 'duplicate_of_queue_item_id', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN v_primary_timesheet_queue_id::text ELSE NULL END,
                 'materialisation_noop_reason', CASE WHEN v_queue_kind = 'TIMESHEET' AND v_queue_item.id IS DISTINCT FROM v_primary_timesheet_queue_id THEN 'same_storage_key_duplicate' ELSE NULL END
               )
       WHERE mq.id = v_queue_item.id;
      v_attached_queue_count := v_attached_queue_count + 1;
    END LOOP;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'staged_evidence_materialisation_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'materialise_staged_evidence', p_materialise_staged_evidence,
          'staged_evidence_loop_count', v_staged_evidence_loop_count,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count,
          'timesheet_stage_key_count', v_timesheet_stage_key_count,
          'primary_timesheet_queue_id', v_primary_timesheet_queue_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_primary_timesheet_storage_key IS NOT NULL THEN
    UPDATE public.timesheets AS ts
       SET manual_pdf_r2_key = v_primary_timesheet_storage_key,
           manual_pdf_rotation_degrees = v_primary_timesheet_rotation_deg,
           updated_at = v_now
     WHERE ts.timesheet_id = v_current_ts.timesheet_id
       AND ts.is_current = true
     RETURNING * INTO v_current_ts;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'primary_timesheet_storage_applied',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
          'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
          'manual_pdf_r2_key', v_current_ts.manual_pdf_r2_key,
          'manual_pdf_rotation_degrees', v_current_ts.manual_pdf_rotation_degrees
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_week_patch_rec := jsonb_populate_record(v_week, v_week_patch_json);
  UPDATE public.contract_weeks AS cw
     SET status = COALESCE(v_week_patch_rec.status, 'SUBMITTED'::public.contract_week_status_enum),
         submission_mode_snapshot = COALESCE(v_week_patch_rec.submission_mode_snapshot, cw.submission_mode_snapshot, 'MANUAL'::public.submission_mode_enum),
         timesheet_id = v_current_ts.timesheet_id,
         uploaded_pdf_r2_key = COALESCE(v_week_patch_rec.uploaded_pdf_r2_key, v_primary_timesheet_storage_key, cw.uploaded_pdf_r2_key),
         day_entries_json = COALESCE(v_week_patch_rec.day_entries_json, cw.day_entries_json),
         totals_json = COALESCE(v_week_patch_rec.totals_json, cw.totals_json),
         planned_schedule_json = COALESCE(v_week_patch_rec.planned_schedule_json, cw.planned_schedule_json),
         is_adjustment = COALESCE(v_week_patch_rec.is_adjustment, cw.is_adjustment),
         enforce_day_partition = COALESCE(v_week_patch_rec.enforce_day_partition, cw.enforce_day_partition),
         allowed_days_mask = COALESCE(v_week_patch_rec.allowed_days_mask, cw.allowed_days_mask),
         split_boundary_date = COALESCE(v_week_patch_rec.split_boundary_date, cw.split_boundary_date),
         worker_note = COALESCE(v_week_patch_rec.worker_note, cw.worker_note),
         split_group_key = COALESCE(v_week_patch_rec.split_group_key, cw.split_group_key),
         updated_at = v_now
   WHERE cw.id = v_week.id
   RETURNING * INTO v_week;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'contract_week_update_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'contract_week_id', v_week.id,
          'current_timesheet_id', v_current_ts.timesheet_id,
          'new_contract_week_status', v_week.status::text,
          'submission_mode_snapshot', v_week.submission_mode_snapshot::text,
          'uploaded_pdf_r2_key', v_week.uploaded_pdf_r2_key,
          'has_week_patch_json', v_week_patch_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF v_tsfin_snapshot_json IS NULL THEN
    RAISE EXCEPTION USING MESSAGE = 'INVALID_PAYLOAD', DETAIL = jsonb_build_object('field', 'p_tsfin_snapshot_json')::text;
  END IF;
  IF NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), '')::uuid IS DISTINCT FROM v_contract.client_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'client_id', 'expected_value', v_contract.client_id, 'supplied_value', v_tsfin_snapshot_json ->> 'client_id')::text;
  END IF;
  IF v_contract.candidate_id IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '') IS NOT NULL
     AND NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), '')::uuid IS DISTINCT FROM v_contract.candidate_id THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_SNAPSHOT_MISMATCH', DETAIL = jsonb_build_object('field', 'candidate_id', 'expected_value', v_contract.candidate_id, 'supplied_value', v_tsfin_snapshot_json ->> 'candidate_id')::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_snapshot_validated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'contract_client_id', v_contract.client_id,
          'contract_candidate_id', v_contract.candidate_id,
          'snapshot_client_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'client_id', '')), ''),
          'snapshot_candidate_id', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'candidate_id', '')), ''),
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_snapshot_json := v_tsfin_snapshot_json || jsonb_build_object(
    'timesheet_id', v_current_ts.timesheet_id::text,
    'timesheet_version', v_current_ts.version,
    'processing_status', CASE
      WHEN COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH') = 'AWAITING_MANUAL_SIGNATURE'
       AND v_current_ts.submission_mode = 'MANUAL'::public.submission_mode_enum
       AND v_current_ts.sheet_scope = 'WEEKLY'::public.timesheet_scope_enum
       AND v_current_ts.qr_status IS NULL
      THEN 'PENDING_AUTH'
      ELSE COALESCE(NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''), 'PENDING_AUTH')
    END
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'snapshot_processing_status', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'processing_status', '')), ''),
          'snapshot_basis', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'basis', '')), ''),
          'snapshot_total_hours', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_hours', '')), ''),
          'snapshot_total_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_pay_ex_vat', '')), ''),
          'snapshot_total_charge_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'total_charge_ex_vat', '')), ''),
          'snapshot_expenses_pay_ex_vat', NULLIF(BTRIM(COALESCE(v_tsfin_snapshot_json ->> 'expenses_pay_ex_vat', '')), '')
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_tsfin_result := public.tsfin_write_current_snapshot_single_bounded(
    p_timesheet_id => v_current_ts.timesheet_id,
    p_timesheet_version => v_current_ts.version,
    p_snapshot_json => v_tsfin_snapshot_json,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => v_now
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_completed',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false),
          'tsfin_result_code', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'code', v_tsfin_result ->> 'error_code', '')), ''),
          'tsfin_result_message', NULLIF(BTRIM(COALESCE(v_tsfin_result ->> 'message', v_tsfin_result ->> 'error', '')), ''),
          'tsfin_result_keys', (SELECT jsonb_agg(result_key ORDER BY result_key) FROM jsonb_object_keys(COALESCE(v_tsfin_result, '{}'::jsonb)) AS result_keys(result_key))
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE((v_tsfin_result ->> 'ok')::boolean, false) IS DISTINCT FROM true THEN
    RAISE EXCEPTION USING MESSAGE = 'TSFIN_UPDATE_FAILED', DETAIL = COALESCE(v_tsfin_result, '{}'::jsonb)::text;
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'tsfin_write_result_checked',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'timesheet_version', v_current_ts.version,
          'tsfin_result_ok', COALESCE((v_tsfin_result ->> 'ok')::boolean, false)
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  SELECT tf.*
    INTO v_current_tsfin
  FROM public.timesheets_financials AS tf
  WHERE tf.timesheet_id = v_current_ts.timesheet_id
    AND tf.is_current = true
  ORDER BY tf.computed_at_utc DESC NULLS LAST, tf.updated_at DESC NULLS LAST, tf.created_at DESC NULLS LAST, tf.id DESC
  LIMIT 1;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'current_tsfin_reloaded_after_write',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'current_tsfin_id', v_current_tsfin.id,
          'processing_status', v_current_tsfin.processing_status::text,
          'total_hours', v_current_tsfin.total_hours,
          'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
          'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
          'margin_ex_vat', v_current_tsfin.margin_ex_vat,
          'computed_at_utc', v_current_tsfin.computed_at_utc
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_summary_refresh_result := public.pay_timesheet_summary_pay_state_refresh(
    ARRAY[v_current_ts.timesheet_id]::uuid[],
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'summary_pay_state_lightweight_patch_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'summary_refresh_result', v_summary_refresh_result
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generation_started',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  v_after_signature_json := public.timesheet_lifecycle_guard_signature_v1(v_current_ts.timesheet_id, v_week.id, COALESCE(v_temp_log_enabled, false));
  v_after_row_signature := NULLIF(BTRIM(COALESCE(v_after_signature_json ->> 'backend_row_signature', v_after_signature_json ->> 'row_signature', v_after_signature_json ->> 'signature', '')), '');

  IF COALESCE(v_temp_log_enabled, false) THEN
    PERFORM public._temp_diag_log(
      'TIMESHEET_SAVE_SIGNATURE_DIAG',
      'TEMP_TIMESHEET_LIFECYCLE',
      v_current_ts.timesheet_id::text,
      jsonb_strip_nulls(jsonb_build_object(
        'tag', 'TIMESHEET_SAVE_SIGNATURE_DIAG',
        'function_name', 'contract_week_manual_upsert_atomic',
        'stage', 'after_manual_upsert_signature_generated',
        'action', 'manual_upsert',
        'route_family', 'contract_week_manual_upsert',
        'elapsed_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3),
        'duration_ms', ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3),
        'timesheet_id', v_current_ts.timesheet_id,
        'contract_week_id', v_week.id,
        'previous_row_signature', v_current_row_signature,
        'new_row_signature', v_after_row_signature,
        'signature_payload', v_after_signature_json
      ))
    );
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'after_signature_generated',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'previous_row_signature', v_current_row_signature,
          'new_row_signature', v_after_row_signature,
          'signature_payload_present', v_after_signature_json <> '{}'::jsonb
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  PERFORM public._audit_insert(
    'contract_week',
    v_week.id::text,
    CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'previous_timesheet_id', CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id END,
      'previous_contract_week_status', v_previous_contract_week_status,
      'previous_processing_status', v_previous_processing_status,
      'previous_row_signature', v_current_row_signature
    ),
    jsonb_build_object(
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'new_contract_week_status', v_week.status::text,
      'new_processing_status', v_current_tsfin.processing_status::text,
      'new_row_signature', v_after_row_signature,
      'created_now', v_created_now,
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key
    ),
    'WEEKLY_MANUAL_PROCESS',
    p_actor_user_id
  );

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'audit_done',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'audit_action', CASE WHEN v_created_now THEN 'CONTRACT_WEEK_MANUAL_TIMESHEET_CREATED_PROCESSED' ELSE 'CONTRACT_WEEK_MANUAL_TIMESHEET_UPDATED_PROCESSED' END,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  IF COALESCE(v_temp_log_enabled, false) THEN
    v_diag_step_index := v_diag_step_index + 1;
    v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
    v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
    PERFORM public._temp_diag_log(
      'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
      'TEMP_TIMESHEET_LIFECYCLE',
      COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
      jsonb_strip_nulls(
        jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'return',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'week_id', p_week_id,
          'contract_week_id', CASE WHEN v_week.id IS NULL THEN p_week_id ELSE v_week.id END,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'expected_timesheet_id', p_expected_timesheet_id,
          'created_now', v_created_now,
          'was_stale', v_was_stale,
          'temp_log_enabled', v_temp_log_enabled
        )
        || jsonb_build_object(
          'current_timesheet_id', v_current_ts.timesheet_id,
          'contract_week_id', v_week.id,
          'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
          'new_contract_week_status', v_week.status::text,
          'new_processing_status', v_current_tsfin.processing_status::text,
          'new_row_signature', v_after_row_signature,
          'attached_evidence_count', v_attached_evidence_count,
          'attached_queue_count', v_attached_queue_count,
          'duplicate_queue_count', v_duplicate_queue_count
        )
      )
    );
    v_diag_stage_started_at := clock_timestamp();
  END IF;

  RETURN jsonb_build_object(
    'ok', true,
    'success', true,
    'operation', CASE WHEN v_created_now THEN 'weekly_manual_process_create' ELSE 'weekly_manual_process_update' END,
    'contract_week_id', v_week.id,
    'contract_id', v_week.contract_id,
    'timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_id', v_current_ts.timesheet_id,
    'current_timesheet_version', v_current_ts.version,
    'was_stale', v_was_stale,
    'created_now', v_created_now,
    'previous_contract_week_status', v_previous_contract_week_status,
    'new_contract_week_status', v_week.status::text,
    'previous_processing_status', v_previous_processing_status,
    'processing_status', v_current_tsfin.processing_status::text,
    'new_processing_status', v_current_tsfin.processing_status::text,
    'timesheet_financials_id', v_current_tsfin.id,
    'backend_row_signature', v_after_row_signature,
    'row_signature', v_after_row_signature,
    'expected_row_signature', v_after_row_signature,
    'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
    'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
    'requires_authorise_preflight', v_after_row_signature IS NULL,
    'requires_affected_row_refresh', v_after_row_signature IS NULL,
    'refresh_required', v_after_row_signature IS NULL,
    'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
    'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
    'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
    'lifecycle_patch', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'timesheet_financials_id', v_current_tsfin.id,
      'is_current', v_current_ts.is_current,
      'tsfin_is_current', v_current_tsfin.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'lifecycle_signature_stable', v_after_row_signature IS NOT NULL,
      'lifecycle_signature_pending_reason', CASE WHEN v_after_row_signature IS NULL THEN 'POST_SAVE_ROW_SIGNATURE_UNAVAILABLE' ELSE NULL::text END,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'refresh_required', v_after_row_signature IS NULL,
      'row_stale', v_after_row_signature IS NULL,
      'lifecycle_refresh_failed', v_after_row_signature IS NULL
    )),
    'timesheet', jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'status', v_current_ts.status::text,
      'is_current', v_current_ts.is_current,
      'authorised_at_server', v_current_ts.authorised_at_server,
      'revoked_at', v_current_ts.revoked_at,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature
    )),
    'tsfin', jsonb_strip_nulls(jsonb_build_object(
      'id', v_current_tsfin.id,
      'timesheet_id', v_current_tsfin.timesheet_id,
      'is_current', v_current_tsfin.is_current,
      'processing_status', v_current_tsfin.processing_status::text,
      'total_hours', v_current_tsfin.total_hours,
      'total_pay_ex_vat', v_current_tsfin.total_pay_ex_vat,
      'total_charge_ex_vat', v_current_tsfin.total_charge_ex_vat,
      'margin_ex_vat', v_current_tsfin.margin_ex_vat,
      'computed_at_utc', v_current_tsfin.computed_at_utc,
      'updated_at', v_current_tsfin.updated_at
    )),
    'summary_pay_state_refresh', COALESCE(v_summary_refresh_result, '{}'::jsonb),
    'evidence_summary', jsonb_build_object(
      'attached_evidence_count', v_attached_evidence_count,
      'attached_queue_count', v_attached_queue_count,
      'duplicate_queue_count', v_duplicate_queue_count,
      'primary_timesheet_storage_key', v_primary_timesheet_storage_key,
      'primary_timesheet_rotation_degrees', v_primary_timesheet_rotation_deg,
      'selected_queue_timesheet_queue_id', v_selected_queue_id,
      'selected_queue_timesheet_storage_key', v_selected_queue_storage_key
    ),
    'affected_rows', jsonb_build_array(jsonb_strip_nulls(jsonb_build_object(
      'timesheet_id', v_current_ts.timesheet_id,
      'current_timesheet_id', v_current_ts.timesheet_id,
      'contract_week_id', v_week.id,
      'booking_id', v_current_ts.booking_id,
      'timesheet_status', v_current_ts.status::text,
      'contract_week_status', v_week.status::text,
      'processing_status', v_current_tsfin.processing_status::text,
      'tsfin_processing_status', v_current_tsfin.processing_status::text,
      'row_signature', v_after_row_signature,
      'backend_row_signature', v_after_row_signature,
      'expected_row_signature', v_after_row_signature,
      'permission_state_patch_complete', v_after_row_signature IS NOT NULL,
      'priority_badges_patch_complete', v_after_row_signature IS NOT NULL,
      'immediate_lifecycle_patch_available', v_after_row_signature IS NOT NULL,
      'requires_network_before_authorise', v_after_row_signature IS NULL,
      'row_key', 'timesheet:' || v_current_ts.timesheet_id::text
    ))),
    'cache_invalidation_hints', jsonb_build_object(
      'changed_domains', jsonb_build_array('timesheets', 'timesheets_financials', 'timesheet_summary_pay_state_cache', 'timesheet_pay_state', 'contract_weeks', 'timesheet_evidence', 'manual_timesheet_queue'),
      'contract_week_id', v_week.id,
      'timesheet_id', v_current_ts.timesheet_id,
      'booking_id', v_current_ts.booking_id
    )
  ) || CASE
    WHEN private._candidate_feature_enabled_current_v1('candidate_record_role_capabilities') THEN
      jsonb_build_object(
        'candidate_record_role',v_candidate_final_state_guard->>'record_role',
        'candidate_expected_line_type',v_candidate_final_state_guard->>'expected_line_type',
        'candidate_final_state_guard',v_candidate_final_state_guard)
    ELSE '{}'::jsonb END;
EXCEPTION
  WHEN unique_violation THEN
    GET STACKED DIAGNOSTICS v_error_constraint = CONSTRAINT_NAME;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_unique_violation',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'constraint_name', v_error_constraint
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_constraint = 'uq_manual_timesheet_queue_one_active_staged_timesheet_per_contr' THEN
      RAISE EXCEPTION USING MESSAGE = 'STAGED_TIMESHEET_CONFLICT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'constraint_name', v_error_constraint)::text;
    END IF;
    RAISE;
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_error_state = RETURNED_SQLSTATE, v_error_message = MESSAGE_TEXT;

    IF COALESCE(v_temp_log_enabled, false) THEN
      v_diag_step_index := v_diag_step_index + 1;
      v_diag_elapsed_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_started_at)) * 1000)::numeric, 3);
      v_diag_duration_ms := ROUND((EXTRACT(EPOCH FROM (clock_timestamp() - v_diag_stage_started_at)) * 1000)::numeric, 3);
      PERFORM public._temp_diag_log(
        'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
        'TEMP_TIMESHEET_LIFECYCLE',
        COALESCE(CASE WHEN v_current_ts.timesheet_id IS NULL THEN NULL ELSE v_current_ts.timesheet_id::text END, CASE WHEN v_pointer_ts.timesheet_id IS NULL THEN NULL ELSE v_pointer_ts.timesheet_id::text END, p_expected_timesheet_id::text, p_week_id::text),
        jsonb_strip_nulls(jsonb_build_object(
          'tag', 'TEMP_CONTRACT_WEEK_MANUAL_UPSERT_STAGE',
          'function_name', 'contract_week_manual_upsert_atomic',
          'stage', 'exception_others',
          'action', 'manual_upsert',
          'route_family', 'contract_week_manual_upsert',
          'step_index', v_diag_step_index,
          'elapsed_ms', v_diag_elapsed_ms,
          'duration_ms', v_diag_duration_ms,
          'contract_week_id', p_week_id,
          'timesheet_id', CASE WHEN v_current_ts.timesheet_id IS NULL THEN p_expected_timesheet_id ELSE v_current_ts.timesheet_id END,
          'error_state', v_error_state,
          'error_message', v_error_message
        ))
      );
      v_diag_stage_started_at := clock_timestamp();
    END IF;
    IF v_error_state = '55P03' THEN
      RAISE EXCEPTION USING MESSAGE = 'LOCK_TIMEOUT', DETAIL = jsonb_build_object('contract_week_id', p_week_id, 'error_state', v_error_state)::text;
    END IF;
    RAISE;
END;
$function$;

-- contract_week_manual_upsert_bulk_process_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamp with time zone,text,jsonb,jsonb,text,jsonb)
CREATE OR REPLACE FUNCTION public.contract_week_manual_upsert_bulk_process_atomic(p_week_id uuid, p_expected_timesheet_id uuid DEFAULT NULL::uuid, p_timesheet_create_json jsonb DEFAULT NULL::jsonb, p_timesheet_patch_json jsonb DEFAULT '{}'::jsonb, p_contract_week_patch_json jsonb DEFAULT '{}'::jsonb, p_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_rotation_json jsonb DEFAULT NULL::jsonb, p_actor_user_id uuid DEFAULT NULL::uuid, p_materialise_staged_evidence boolean DEFAULT true, p_now_utc timestamp with time zone DEFAULT now(), p_expected_row_signature text DEFAULT NULL::text, p_expected_current_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_next_tsfin_snapshot_json jsonb DEFAULT NULL::jsonb, p_response_context text DEFAULT NULL::text, p_queue_timesheet_materialisation_json jsonb DEFAULT NULL::jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
DECLARE
  v_now timestamp with time zone := COALESCE(p_now_utc, now());
  v_operation text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'contract_week_manual_upsert_bulk_authorise' ELSE 'contract_week_manual_upsert_bulk_process' END;
  v_response_context text := CASE WHEN LOWER(BTRIM(COALESCE(p_response_context, ''))) = 'bulk_authorise' THEN 'bulk_authorise' ELSE 'bulk_process' END;
  v_result jsonb := '{}'::jsonb;
  v_error_state text := NULL;
  v_error_message text := NULL;
  v_error_detail text := NULL;
  v_detail_json jsonb := NULL;
  v_result_error_code text := NULL;
  v_result_message text := NULL;
  v_attempt integer := 0;
  v_max_attempts constant integer := 6;
  v_retry_wait_ms integer := 0;
  v_total_retry_wait_ms integer := 0;
  v_transient_contention boolean := FALSE;
BEGIN

  if p_expected_timesheet_id is not null
     and coalesce((public._ctms_import_correction_classify_v1(p_expected_timesheet_id)
       ->> 'is_import_authoritative_correction')::boolean, false) then
    declare v_transition jsonb;
    begin
      v_transition := public.timesheet_correction_pair_transition_v1(
        p_expected_timesheet_id, 'PROCESS', p_actor_user_id,
        null::uuid, null::text, true, 100
      );
      if coalesce((v_transition ->> 'action_ready')::boolean, false) is not true
         or coalesce((v_transition ->> 'expected_member_count')::integer, 0) > 1 then
        return jsonb_build_object(
          'ok', false,
          'error_code', 'IMPORT_CORRECTION_UNIT_REQUIRES_ATOMIC_PROCESS_ORCHESTRATION',
          'transition', v_transition
        );
      end if;
    end;
  end if;
  PERFORM set_config('lock_timeout', '2500ms', true);

  IF p_week_id IS NULL THEN
    RETURN jsonb_build_object(
      'ok', false,
      'success', false,
      'operation', v_operation,
      'error_code', 'CONTRACT_WEEK_ID_REQUIRED',
      'message', 'p_week_id is required.'
    );
  END IF;

  LOOP
    v_attempt := v_attempt + 1;
    v_result := '{}'::jsonb;
    v_error_state := NULL;
    v_error_message := NULL;
    v_error_detail := NULL;
    v_detail_json := NULL;
    v_result_error_code := NULL;
    v_result_message := NULL;
    v_transient_contention := FALSE;

    BEGIN
      v_result := public.contract_week_manual_upsert_atomic(
        p_week_id => p_week_id,
        p_expected_timesheet_id => p_expected_timesheet_id,
        p_timesheet_create_json => p_timesheet_create_json,
        p_timesheet_patch_json => p_timesheet_patch_json,
        p_contract_week_patch_json => p_contract_week_patch_json,
        p_tsfin_snapshot_json => COALESCE(p_next_tsfin_snapshot_json, p_tsfin_snapshot_json),
        p_rotation_json => p_rotation_json,
        p_actor_user_id => p_actor_user_id,
        p_materialise_staged_evidence => p_materialise_staged_evidence,
        p_now_utc => v_now,
        p_expected_row_signature => p_expected_row_signature,
        p_queue_timesheet_materialisation_json => p_queue_timesheet_materialisation_json
      );

      v_result_error_code := UPPER(BTRIM(COALESCE(v_result ->> 'error_code', v_result ->> 'error', '')));
      v_result_message := LOWER(BTRIM(COALESCE(v_result ->> 'message', '')));
      v_transient_contention := COALESCE((v_result ->> 'ok')::boolean, true) IS DISTINCT FROM true
        AND (
          v_result_error_code IN ('LOCK_TIMEOUT', '55P03', '40P01', '40001', '57014', 'TRANSIENT_PROCESSING_CONTENTION')
          OR LOWER(v_result_error_code) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(BTRIM(COALESCE(v_result ->> 'sqlstate', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR LOWER(BTRIM(COALESCE(v_result -> 'detail_json' ->> 'error_state', ''))) IN ('55p03', '40p01', '40001', '57014')
          OR v_result_message ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
          OR LOWER(COALESCE(v_result ->> 'detail', '')) ~ '(55p03|40p01|40001|57014|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
        );

      IF v_transient_contention IS DISTINCT FROM true THEN
        EXIT;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_error_state = RETURNED_SQLSTATE,
        v_error_message = MESSAGE_TEXT,
        v_error_detail = PG_EXCEPTION_DETAIL;

      BEGIN
        v_detail_json := v_error_detail::jsonb;
      EXCEPTION WHEN OTHERS THEN
        v_detail_json := NULL;
      END;

      v_transient_contention := (
        v_error_state IN ('55P03', '40P01', '40001', '57014')
        OR (
          v_error_state = 'P0001'
          AND UPPER(BTRIM(COALESCE(v_error_message, ''))) IN ('LOCK_TIMEOUT', 'TRANSIENT_PROCESSING_CONTENTION')
        )
        OR LOWER(COALESCE(v_error_message, '')) ~ '(lock[_ ]timeout|deadlock|could not obtain lock|serialization failure|concurrent update)'
        OR LOWER(COALESCE(v_error_detail, '')) ~ '(55p03|40p01|40001|lock[_ ]timeout|deadlock|could not obtain lock|serialization failure)'
      );

      IF v_transient_contention IS DISTINCT FROM true THEN
        RETURN jsonb_build_object(
          'ok', false,
          'success', false,
          'operation', v_operation,
          'response_context', v_response_context,
          'bulk_process', v_response_context = 'bulk_process',
          'bulk_authorise', v_response_context = 'bulk_authorise',
          'error_code', CASE WHEN v_error_state = 'P0001' AND NULLIF(BTRIM(v_error_message), '') IS NOT NULL THEN v_error_message ELSE v_error_state END,
          'sqlstate', v_error_state,
          'message', v_error_message,
          'detail', v_error_detail,
          'detail_json', v_detail_json,
          'contract_week_id', p_week_id,
          'expected_timesheet_id', p_expected_timesheet_id,
          'process_retry_count', GREATEST(v_attempt - 1, 0),
          'process_retry_wait_ms', v_total_retry_wait_ms,
          'refresh_required', true,
          'cache_invalidation_hints', jsonb_build_object(
            'changed_domains', jsonb_build_array('timesheet_lifecycle'),
            'contract_week_id', p_week_id,
            'timesheet_id', p_expected_timesheet_id
          )
        );
      END IF;
    END;

    IF v_attempt >= v_max_attempts THEN
      RETURN jsonb_build_object(
        'ok', false,
        'success', false,
        'operation', v_operation,
        'response_context', v_response_context,
        'bulk_process', v_response_context = 'bulk_process',
        'bulk_authorise', v_response_context = 'bulk_authorise',
        'error_code', 'TRANSIENT_PROCESSING_CONTENTION',
        'message', 'The timesheet could not be completed immediately. The row has been safely refreshed.',
        'contract_week_id', p_week_id,
        'expected_timesheet_id', p_expected_timesheet_id,
        'process_retry_count', GREATEST(v_attempt - 1, 0),
        'process_retry_wait_ms', v_total_retry_wait_ms,
        'refresh_required', true,
        'cache_invalidation_hints', jsonb_build_object(
          'changed_domains', jsonb_build_array('timesheet_lifecycle'),
          'contract_week_id', p_week_id,
          'timesheet_id', p_expected_timesheet_id
        )
      );
    END IF;

    v_retry_wait_ms := LEAST(2000, (150::numeric * POWER(2::numeric, GREATEST(v_attempt - 1, 0)::numeric))::integer) + FLOOR(RANDOM() * 125)::integer;
    v_total_retry_wait_ms := v_total_retry_wait_ms + v_retry_wait_ms;
    PERFORM pg_sleep(v_retry_wait_ms::numeric / 1000::numeric);
  END LOOP;

  RETURN COALESCE(v_result, '{}'::jsonb)
    || jsonb_build_object(
      'ok', COALESCE((v_result ->> 'ok')::boolean, true),
      'success', COALESCE((v_result ->> 'success')::boolean, true),
      'operation', v_operation,
      'response_context', v_response_context,
      'bulk_process', v_response_context = 'bulk_process',
      'bulk_authorise', v_response_context = 'bulk_authorise',
      'process_retry_count', GREATEST(v_attempt - 1, 0),
      'process_retry_wait_ms', v_total_retry_wait_ms,
      'transient_contention_recovered', v_attempt > 1,
      'requires_affected_row_refresh', true,
      'refresh_required', true
    );
END;
$function$;

-- contracts_clone_and_extend_atomic(uuid,date,date,date,boolean,uuid,text,jsonb,boolean,boolean,boolean,uuid)
CREATE OR REPLACE FUNCTION public.contracts_clone_and_extend_atomic(p_contract_id uuid, p_new_start_date date, p_new_end_date date, p_end_existing_on date, p_assign_existing_candidate boolean, p_new_candidate_id uuid DEFAULT NULL::uuid, p_split_worker_note text DEFAULT NULL::text, p_successor_overrides jsonb DEFAULT NULL::jsonb, p_force_schedule_clashes boolean DEFAULT false, p_force_already_split_week boolean DEFAULT false, p_confirmed_split_week boolean DEFAULT false, p_actor_user_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now                 timestamptz := now();

  v_cur                 public.contracts%rowtype;
  v_succ                public.contracts%rowtype;

  v_ov                  jsonb := coalesce(p_successor_overrides, '{}'::jsonb);

  v_close_to            date := p_end_existing_on;
  v_new_start           date := p_new_start_date;
  v_new_end             date := p_new_end_date;

  v_wew_pred            int;
  v_wew_succ            int;

  v_end_we_old          date;
  v_boundary_week_end   date;
  v_boundary_week_start date;

  v_split_week          boolean := false;
  v_already_split       boolean := false;
  v_split_note          text := null;
  v_split_group_key     text := null;
  v_old_mask            text := null;
  v_new_mask            text := null;

  v_schedule_clashes    jsonb := null;
  v_overlap_warnings    jsonb := '[]'::jsonb;
  v_clash_count         int := 0;

  v_err                 jsonb;

  -- Successor computed fields (merged overrides)
  v_succ_candidate_id   uuid;
  v_succ_client_id      uuid;
  v_succ_role           text;
  v_succ_band           text;
  v_succ_display_site   text;
  v_succ_ward_hint      text;

  v_succ_pay_method_snapshot text;
  v_succ_rates_json          jsonb;
  v_succ_std_schedule_json   jsonb;
  v_succ_std_hours_json      jsonb;
  v_succ_bucket_labels_json  jsonb;
  v_succ_additional_rates_json jsonb;

  v_succ_weekly_timesheet_source public.weekly_timesheet_source_enum;
  v_succ_overrideclientsettings boolean;
  v_succ_no_timesheet_required boolean;
  v_succ_daily_calc_of_invoices boolean;
  v_succ_group_nightsat_sunbh boolean;
  v_succ_is_nhsp boolean;
  v_succ_autoprocess_hr boolean;
  v_succ_requires_hr boolean;
  v_succ_hr_attach_to_invoice boolean;
  v_succ_ts_attach_to_invoice boolean;
  v_succ_reference_number_required_to_issue_invoice boolean;
  v_succ_send_manual_invoices_to_different_email boolean;
  v_succ_manual_invoices_alt_email_address text;
  v_succ_send_ts_queries_to_different_email boolean;
  v_succ_ts_queries_alt_email_address text;
  v_succ_is_ad_hoc boolean;
  v_succ_default_submission_mode public.submission_mode_enum;

  -- std_hours derivation scratch
  v_day_key text;
  v_day_cfg jsonb;
  v_start_str text;
  v_end_str text;
  v_break_minutes numeric;
  v_start_h int;
  v_start_m int;
  v_end_h int;
  v_end_m int;
  v_start_mins int;
  v_end_mins int;
  v_minutes int;
  v_expected_minutes int;
  v_hours numeric;

  -- Split boundary hard-block helper
  v_bad_contract_id uuid;
  v_bad_timesheet_id uuid;

  -- Audit helpers
  v_before_state jsonb;
  v_after_state jsonb;
  v_audit_reason text;

  -- sink for CTE
  v_dummy int;

  -- =====================================================
  -- DEBUGGING (gated by settings_defaults.invoice_debug)
  -- =====================================================
  v_invoice_debug boolean := false;
  v_dbg_started_at timestamptz := clock_timestamp();
  v_dbg_steps jsonb := '[]'::jsonb;
  v_dbg_sqlstate text := null;
  v_dbg_error text := null;
  v_dbg_detail text := null;
  v_dbg_hint text := null;
  v_dbg_context text := null;
  v_dbg_stats jsonb := '{}'::jsonb;

  v_rc int := 0;

begin
  -- ─────────────────────────────────────────────────────────────
  -- Load invoice_debug flag (safe if column/table not present)
  -- ─────────────────────────────────────────────────────────────
  begin
    select coalesce(sd.invoice_debug, false)
      into v_invoice_debug
      from public.settings_defaults sd
     where sd.id = 1
     limit 1;
  exception
    when undefined_column then
      v_invoice_debug := false;
    when undefined_table then
      v_invoice_debug := false;
    when others then
      -- never allow debug flag read to break functional flow
      v_invoice_debug := false;
  end;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','start',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'inputs', jsonb_build_object(
          'p_contract_id', coalesce(p_contract_id::text,''),
          'p_new_start_date', coalesce(p_new_start_date::text,''),
          'p_new_end_date', coalesce(p_new_end_date::text,''),
          'p_end_existing_on', coalesce(p_end_existing_on::text,''),
          'p_assign_existing_candidate', coalesce(p_assign_existing_candidate, true),
          'p_new_candidate_id', coalesce(p_new_candidate_id::text,''),
          'p_split_worker_note', coalesce(p_split_worker_note,''),
          'p_successor_overrides_type', case when p_successor_overrides is null then 'null' else jsonb_typeof(p_successor_overrides) end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false),
          'p_force_already_split_week', coalesce(p_force_already_split_week,false),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'p_actor_user_id', coalesce(p_actor_user_id::text,'')
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Basic input validation
  -- ─────────────────────────────────────────────────────────────
  if p_contract_id is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_contract_id is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','p_contract_id is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start is null or v_new_end is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date and p_new_end_date are required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','new_start or new_end is null',
          'error', v_err,
          'computed', jsonb_build_object('v_new_start', coalesce(v_new_start::text,''), 'v_new_end', coalesce(v_new_end::text,''))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_new_start > v_new_end then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_new_start_date must be <= p_new_end_date');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_new_start > v_new_end',
          'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to is null then
    v_err := jsonb_build_object('error','INVALID_INPUT','message','p_end_existing_on is required');

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','v_close_to is null',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','validation_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object('v_new_start', v_new_start, 'v_new_end', v_new_end, 'v_close_to', v_close_to)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Lock predecessor contract
  -- ─────────────────────────────────────────────────────────────
  select c.*
    into v_cur
    from public.contracts as c
   where c.id = p_contract_id
   for update;

  if not found then
    v_err := jsonb_build_object('error','CONTRACT_NOT_FOUND','contract_id',p_contract_id);

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','contract_not_found',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','locked_predecessor',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'predecessor', jsonb_build_object(
          'id', v_cur.id,
          'candidate_id', coalesce(v_cur.candidate_id::text,''),
          'client_id', coalesce(v_cur.client_id::text,''),
          'start_date', v_cur.start_date,
          'end_date', v_cur.end_date,
          'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- Close window rules (end-existing enforced)
  if v_close_to < v_cur.start_date then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be >= predecessor.start_date',
      'predecessor_start_date', v_cur.start_date,
      'end_existing_on', v_close_to
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to < predecessor.start_date',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_close_to > (v_new_start - 1) then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','p_end_existing_on must be <= p_new_start_date - 1 day',
      'end_existing_on', v_close_to,
      'new_start_date', v_new_start
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_close_window_rule',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'reason','close_to > new_start - 1',
          'error', v_err,
          'computed', jsonb_build_object('new_start_minus_1', (v_new_start - 1))
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','close_window_rules_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'predecessor_start_date', v_cur.start_date,
          'close_to', v_close_to,
          'new_start', v_new_start,
          'close_to_max', (v_new_start - 1)
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Determine successor candidate assignment
  -- ─────────────────────────────────────────────────────────────
  if coalesce(p_assign_existing_candidate, true) then
    v_succ_candidate_id := v_cur.candidate_id;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','succ_candidate_assignment',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'path','assign_existing_candidate',
          'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
        )
      );
    end if;

  else
    if p_new_candidate_id is not null then
      v_succ_candidate_id := p_new_candidate_id;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','explicit_param_new_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    elsif (v_ov ? 'candidate_id') and nullif(btrim(v_ov->>'candidate_id'), '') is not null then
      v_succ_candidate_id := (v_ov->>'candidate_id')::uuid;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','override_candidate_id',
            'successor_candidate_id', coalesce(v_succ_candidate_id::text,'')
          )
        );
      end if;

    else
      v_succ_candidate_id := null;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','succ_candidate_assignment',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','no_candidate_assigned',
            'successor_candidate_id',''
          )
        );
      end if;

    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Predecessor week-ending snapshot validation + derived dates
  -- ─────────────────────────────────────────────────────────────
  v_wew_pred := coalesce(v_cur.week_ending_weekday_snapshot, 0);

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','predecessor_wew_loaded',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'week_ending_weekday_snapshot', v_wew_pred
      )
    );
  end if;

  if v_wew_pred < 0 or v_wew_pred > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_CONTRACT_STATE',
      'message','predecessor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_cur.week_ending_weekday_snapshot
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_predecessor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- endWEOld = week ending date of the week containing close_to (using predecessor snapshot)
  v_end_we_old :=
    (v_close_to + (((v_wew_pred - extract(dow from v_close_to)::int + 7) % 7)) * interval '1 day')::date;

  -- boundary week for newStart (using predecessor snapshot)
  v_boundary_week_end :=
    (v_new_start + (((v_wew_pred - extract(dow from v_new_start)::int + 7) % 7)) * interval '1 day')::date;
  v_boundary_week_start := (v_boundary_week_end - interval '6 days')::date;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','derived_dates',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'computed', jsonb_build_object(
          'v_wew_pred', v_wew_pred,
          'v_end_we_old', v_end_we_old,
          'v_boundary_week_start', v_boundary_week_start,
          'v_boundary_week_end', v_boundary_week_end,
          'v_new_start', v_new_start,
          'v_new_end', v_new_end,
          'v_close_to', v_close_to
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Hard block: submitted beyond close window
  -- ─────────────────────────────────────────────────────────────
  if exists (
    select 1
      from public.contract_weeks as cw
     where cw.contract_id = v_cur.id
       and cw.timesheet_id is not null
       and cw.week_ending_date > v_end_we_old
  ) then
    v_err := jsonb_build_object(
      'error','SUBMITTED_BEYOND_CLOSE',
      'contract_id', v_cur.id,
      'end_week_ending_date', v_end_we_old
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_submitted_beyond_close',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','submitted_beyond_close_check_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'contract_id', v_cur.id::text,
        'end_we_old', v_end_we_old
      )
    );
  end if;

  -- NEW HARD RULE: ending week submitted + midweek truncation
  if v_close_to < v_end_we_old then
    if exists (
      select 1
        from public.contract_weeks as cw
       where cw.contract_id = v_cur.id
         and cw.week_ending_date = v_end_we_old
         and cw.timesheet_id is not null
    ) then
      v_err := jsonb_build_object(
        'error','ENDING_WEEK_SUBMITTED_CANNOT_TRUNCATE',
        'contract_id', v_cur.id,
        'close_to', v_close_to,
        'week_ending_date', v_end_we_old
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_ending_week_submitted_midweek_truncation',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ending_week_truncation_rule_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'close_to', v_close_to,
        'end_we_old', v_end_we_old,
        'is_midweek_truncation', (v_close_to < v_end_we_old)
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week eligibility (same candidate on both halves + newStart midweek + predecessor ends in that same week)
  -- ─────────────────────────────────────────────────────────────
  v_split_week := (
    v_cur.candidate_id is not null
    and v_succ_candidate_id is not null
    and v_cur.candidate_id = v_succ_candidate_id
    and v_new_start <> v_boundary_week_start
    and v_end_we_old = v_boundary_week_end
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','split_week_eligibility',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'components', jsonb_build_object(
          'pred_candidate_not_null', (v_cur.candidate_id is not null),
          'succ_candidate_not_null', (v_succ_candidate_id is not null),
          'same_candidate', (v_cur.candidate_id is not null and v_succ_candidate_id is not null and v_cur.candidate_id = v_succ_candidate_id),
          'new_start_is_midweek', (v_new_start <> v_boundary_week_start),
          'pred_end_week_equals_boundary_week', (v_end_we_old = v_boundary_week_end)
        ),
        'result', v_split_week
      )
    );
  end if;

  if v_split_week then
    -- Hard block: boundary week already submitted (contract-based definition)
    select cw2.contract_id, cw2.timesheet_id
      into v_bad_contract_id, v_bad_timesheet_id
      from public.contracts as c2
      join public.contract_weeks as cw2
        on cw2.contract_id = c2.id
     where c2.candidate_id = v_cur.candidate_id
       and c2.client_id = v_cur.client_id
       and c2.start_date <= v_boundary_week_end
       and c2.end_date >= v_boundary_week_start
       and cw2.week_ending_date = v_boundary_week_end
       and cw2.timesheet_id is not null
     limit 1;

    if found then
      v_err := jsonb_build_object(
        'error','BOUNDARY_WEEK_TIMESHEET_ALREADY_SUBMITTED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_week_end', v_boundary_week_end,
        'contract_id', v_bad_contract_id,
        'timesheet_id', v_bad_timesheet_id
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_boundary_week_already_submitted',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'found_contract_id', coalesce(v_bad_contract_id::text,''),
            'found_timesheet_id', coalesce(v_bad_timesheet_id::text,''),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','boundary_week_submitted_check_ok',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'boundary_week_end', v_boundary_week_end
        )
      );
    end if;

    -- Already split detection (another overlapping contract for same candidate+client in boundary week)
    select exists (
      select 1
        from public.contracts as c2
       where c2.id <> v_cur.id
         and c2.candidate_id = v_cur.candidate_id
         and c2.client_id = v_cur.client_id
         and c2.start_date <= v_boundary_week_end
         and c2.end_date >= v_boundary_week_start
    ) into v_already_split;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','already_split_detection',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'already_split', v_already_split,
          'p_force_already_split_week', coalesce(p_force_already_split_week,false)
        )
      );
    end if;

    if v_already_split and not coalesce(p_force_already_split_week, false) then
      v_err := jsonb_build_object(
        'error','ALREADY_SPLIT_WEEK',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'overlapping_contract_ids', (
          select coalesce(jsonb_agg(c2.id order by c2.start_date), '[]'::jsonb)
            from public.contracts as c2
           where c2.candidate_id = v_cur.candidate_id
             and c2.client_id = v_cur.client_id
             and c2.start_date <= v_boundary_week_end
             and c2.end_date >= v_boundary_week_start
        )
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_already_split_week_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    -- Allowed day masks (Mon..Sun)
    -- Old half: max(predecessor.start_date, week_start) .. closeTo
    if greatest(v_cur.start_date, v_boundary_week_start) > v_close_to then
      v_old_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_old_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   greatest(v_cur.start_date, v_boundary_week_start)::timestamp,
                   v_close_to::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','old_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
            'old_allowed_to', v_close_to,
            'old_mask', v_old_mask
          )
        );
      end if;

    end if;

    -- New half: newStart .. min(newEnd, week_end)
    if v_new_start > least(v_new_end, v_boundary_week_end) then
      v_new_mask := '0000000';

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','empty_range',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    else
      select string_agg(case when d.pos is not null then '1' else '0' end, '' order by p.pos)
        into v_new_mask
        from generate_series(0, 6) as p(pos)
        left join (
          select distinct ((extract(dow from dt)::int + 6) % 7) as pos
            from generate_series(
                   v_new_start::timestamp,
                   least(v_new_end, v_boundary_week_end)::timestamp,
                   interval '1 day'
                 ) as dt
        ) as d
          on d.pos = p.pos;

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','new_mask_computed',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'path','generate_series',
            'new_allowed_from', v_new_start,
            'new_allowed_to', least(v_new_end, v_boundary_week_end),
            'new_mask', v_new_mask
          )
        );
      end if;

    end if;

    -- Confirm gating (note required)
    v_split_note := btrim(coalesce(p_split_worker_note, ''));

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirm_gating',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'p_confirmed_split_week', coalesce(p_confirmed_split_week,false),
          'split_note_empty', (v_split_note = ''),
          'split_note_len', length(v_split_note),
          'old_mask', v_old_mask,
          'new_mask', v_new_mask
        )
      );
    end if;

    if (not coalesce(p_confirmed_split_week, false)) or v_split_note = '' then
      v_err := jsonb_build_object(
        'error','SPLIT_WEEK_CONFIRM_REQUIRED',
        'candidate_id', v_cur.candidate_id,
        'client_id', v_cur.client_id,
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_from', greatest(v_cur.start_date, v_boundary_week_start),
        'old_allowed_to', v_close_to,
        'new_allowed_from', v_new_start,
        'new_allowed_to', least(v_new_end, v_boundary_week_end),
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'suggested_worker_note',
          ('Contract rates have changed this week and therefore you need to submit two timesheets. One timesheet for work completed for ' ||
           to_char(greatest(v_cur.start_date, v_boundary_week_start),'YYYY-MM-DD') || ' to ' ||
           to_char(v_close_to,'YYYY-MM-DD') || ' and another timesheet for ' ||
           to_char(v_new_start,'YYYY-MM-DD') || ' to ' ||
           to_char(least(v_new_end, v_boundary_week_end),'YYYY-MM-DD') || '.')
      );

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_split_week_confirm_required',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;

    v_split_group_key := gen_random_uuid()::text;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_confirmed',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'split_group_key', v_split_group_key,
          'worker_note', v_split_note
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Build successor merged fields from overrides
  -- ─────────────────────────────────────────────────────────────
  v_succ_client_id := coalesce(nullif(btrim(v_ov->>'client_id'), '')::uuid, v_cur.client_id);
  v_succ_role := coalesce(v_ov->>'role', v_cur.role);
  v_succ_band := coalesce(v_ov->>'band', v_cur.band);
  v_succ_display_site := coalesce(v_ov->>'display_site', v_cur.display_site);
  v_succ_ward_hint := coalesce(v_ov->>'ward_hint', v_cur.ward_hint);

  v_succ_pay_method_snapshot := coalesce(nullif(btrim(v_ov->>'pay_method_snapshot'), ''), v_cur.pay_method_snapshot);

  if (v_ov ? 'rates_json') and jsonb_typeof(v_ov->'rates_json') <> 'null' then
    v_succ_rates_json := v_ov->'rates_json';
  else
    v_succ_rates_json := v_cur.rates_json;
  end if;
  v_succ_rates_json := coalesce(v_succ_rates_json, '{}'::jsonb);

  if (v_ov ? 'std_schedule_json') then
    if jsonb_typeof(v_ov->'std_schedule_json') = 'null' then
      v_succ_std_schedule_json := null;
    else
      v_succ_std_schedule_json := v_ov->'std_schedule_json';
    end if;
  else
    v_succ_std_schedule_json := v_cur.std_schedule_json;
  end if;

  if v_succ_std_schedule_json is not null and jsonb_typeof(v_succ_std_schedule_json) = 'object' then
    -- Derive std_hours_json from std_schedule_json
    v_succ_std_hours_json := '{}'::jsonb;

    foreach v_day_key in array array['mon','tue','wed','thu','fri','sat','sun'] loop
      v_day_cfg := v_succ_std_schedule_json->v_day_key;

      if v_day_cfg is not null and jsonb_typeof(v_day_cfg) = 'object' then
        v_start_str := v_day_cfg->>'start';
        v_end_str := v_day_cfg->>'end';

        if v_start_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_start_h := split_part(v_start_str,':',1)::int;
          v_start_m := split_part(v_start_str,':',2)::int;
          if v_start_h between 0 and 23 and v_start_m between 0 and 59 then
            v_start_mins := v_start_h * 60 + v_start_m;
          else
            v_start_mins := null;
          end if;
        else
          v_start_mins := null;
        end if;

        if v_end_str ~ '^[0-9]{1,2}:[0-9]{2}$' then
          v_end_h := split_part(v_end_str,':',1)::int;
          v_end_m := split_part(v_end_str,':',2)::int;
          if v_end_h between 0 and 23 and v_end_m between 0 and 59 then
            v_end_mins := v_end_h * 60 + v_end_m;
          else
            v_end_mins := null;
          end if;
        else
          v_end_mins := null;
        end if;

        v_break_minutes := 0;
        if (v_day_cfg ? 'break_minutes')
          and (v_day_cfg->>'break_minutes') is not null
          and (v_day_cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
        then
          v_break_minutes := (v_day_cfg->>'break_minutes')::numeric;
        end if;

        if v_start_mins is not null and v_end_mins is not null then
          if v_end_mins <= v_start_mins then
            v_minutes := 1440 - v_start_mins + v_end_mins;
          else
            v_minutes := v_end_mins - v_start_mins;
          end if;

          v_expected_minutes := greatest(0, (v_minutes - v_break_minutes)::int);
          v_hours := round((v_expected_minutes::numeric / 60.0), 2);
        else
          v_hours := 0;
        end if;
      else
        v_hours := 0;
      end if;

      v_succ_std_hours_json := v_succ_std_hours_json || jsonb_build_object(v_day_key, v_hours);
    end loop;
  else
    if (v_ov ? 'std_hours_json') then
      if jsonb_typeof(v_ov->'std_hours_json') = 'null' then
        v_succ_std_hours_json := null;
      else
        v_succ_std_hours_json := v_ov->'std_hours_json';
      end if;
    else
      v_succ_std_hours_json := v_cur.std_hours_json;
    end if;
  end if;

  if (v_ov ? 'bucket_labels_json') then
    if jsonb_typeof(v_ov->'bucket_labels_json') = 'null' then
      v_succ_bucket_labels_json := null;
    else
      v_succ_bucket_labels_json := v_ov->'bucket_labels_json';
    end if;
  else
    v_succ_bucket_labels_json := v_cur.bucket_labels_json;
  end if;

  if (v_ov ? 'additional_rates_json') then
    if jsonb_typeof(v_ov->'additional_rates_json') = 'null' then
      v_succ_additional_rates_json := null;
    else
      v_succ_additional_rates_json := v_ov->'additional_rates_json';
    end if;
  else
    v_succ_additional_rates_json := v_cur.additional_rates_json;
  end if;

  if (v_ov ? 'weekly_timesheet_source') then
    v_succ_weekly_timesheet_source := nullif(btrim(v_ov->>'weekly_timesheet_source'), '')::public.weekly_timesheet_source_enum;
  else
    v_succ_weekly_timesheet_source := v_cur.weekly_timesheet_source;
  end if;

  if (v_ov ? 'overrideclientsettings') and jsonb_typeof(v_ov->'overrideclientsettings') <> 'null' then
    v_succ_overrideclientsettings := (v_ov->>'overrideclientsettings')::boolean;
  else
    v_succ_overrideclientsettings := v_cur.overrideclientsettings;
  end if;
  v_succ_overrideclientsettings := coalesce(v_succ_overrideclientsettings, false);

  if (v_ov ? 'no_timesheet_required') then
    if jsonb_typeof(v_ov->'no_timesheet_required') = 'null' then
      v_succ_no_timesheet_required := null;
    else
      v_succ_no_timesheet_required := (v_ov->>'no_timesheet_required')::boolean;
    end if;
  else
    v_succ_no_timesheet_required := v_cur.no_timesheet_required;
  end if;

  if (v_ov ? 'daily_calc_of_invoices') then
    if jsonb_typeof(v_ov->'daily_calc_of_invoices') = 'null' then
      v_succ_daily_calc_of_invoices := null;
    else
      v_succ_daily_calc_of_invoices := (v_ov->>'daily_calc_of_invoices')::boolean;
    end if;
  else
    v_succ_daily_calc_of_invoices := v_cur.daily_calc_of_invoices;
  end if;

  if (v_ov ? 'group_nightsat_sunbh') then
    if jsonb_typeof(v_ov->'group_nightsat_sunbh') = 'null' then
      v_succ_group_nightsat_sunbh := null;
    else
      v_succ_group_nightsat_sunbh := (v_ov->>'group_nightsat_sunbh')::boolean;
    end if;
  else
    v_succ_group_nightsat_sunbh := v_cur.group_nightsat_sunbh;
  end if;

  if (v_ov ? 'is_nhsp') then
    if jsonb_typeof(v_ov->'is_nhsp') = 'null' then
      v_succ_is_nhsp := null;
    else
      v_succ_is_nhsp := (v_ov->>'is_nhsp')::boolean;
    end if;
  else
    v_succ_is_nhsp := v_cur.is_nhsp;
  end if;

  if (v_ov ? 'autoprocess_hr') then
    if jsonb_typeof(v_ov->'autoprocess_hr') = 'null' then
      v_succ_autoprocess_hr := null;
    else
      v_succ_autoprocess_hr := (v_ov->>'autoprocess_hr')::boolean;
    end if;
  else
    v_succ_autoprocess_hr := v_cur.autoprocess_hr;
  end if;

  if (v_ov ? 'requires_hr') then
    if jsonb_typeof(v_ov->'requires_hr') = 'null' then
      v_succ_requires_hr := null;
    else
      v_succ_requires_hr := (v_ov->>'requires_hr')::boolean;
    end if;
  else
    v_succ_requires_hr := v_cur.requires_hr;
  end if;

  if (v_ov ? 'hr_attach_to_invoice') then
    if jsonb_typeof(v_ov->'hr_attach_to_invoice') = 'null' then
      v_succ_hr_attach_to_invoice := null;
    else
      v_succ_hr_attach_to_invoice := (v_ov->>'hr_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_hr_attach_to_invoice := v_cur.hr_attach_to_invoice;
  end if;

  if (v_ov ? 'ts_attach_to_invoice') then
    if jsonb_typeof(v_ov->'ts_attach_to_invoice') = 'null' then
      v_succ_ts_attach_to_invoice := null;
    else
      v_succ_ts_attach_to_invoice := (v_ov->>'ts_attach_to_invoice')::boolean;
    end if;
  else
    v_succ_ts_attach_to_invoice := v_cur.ts_attach_to_invoice;
  end if;

  if (v_ov ? 'reference_number_required_to_issue_invoice') then
    if jsonb_typeof(v_ov->'reference_number_required_to_issue_invoice') = 'null' then
      v_succ_reference_number_required_to_issue_invoice := null;
    else
      v_succ_reference_number_required_to_issue_invoice := (v_ov->>'reference_number_required_to_issue_invoice')::boolean;
    end if;
  else
    v_succ_reference_number_required_to_issue_invoice := v_cur.reference_number_required_to_issue_invoice;
  end if;

  if (v_ov ? 'send_manual_invoices_to_different_email') then
    if jsonb_typeof(v_ov->'send_manual_invoices_to_different_email') = 'null' then
      v_succ_send_manual_invoices_to_different_email := null;
    else
      v_succ_send_manual_invoices_to_different_email := (v_ov->>'send_manual_invoices_to_different_email')::boolean;
    end if;
  else
    v_succ_send_manual_invoices_to_different_email := v_cur.send_manual_invoices_to_different_email;
  end if;

  if (v_ov ? 'manual_invoices_alt_email_address') then
    v_succ_manual_invoices_alt_email_address := nullif(btrim(v_ov->>'manual_invoices_alt_email_address'), '');
  else
    v_succ_manual_invoices_alt_email_address := v_cur.manual_invoices_alt_email_address;
  end if;

  if (v_ov ? 'send_ts_queries_to_different_email') then
    if jsonb_typeof(v_ov->'send_ts_queries_to_different_email') = 'null' then
      v_succ_send_ts_queries_to_different_email := coalesce(v_cur.send_ts_queries_to_different_email,false);
    else
      v_succ_send_ts_queries_to_different_email := (v_ov->>'send_ts_queries_to_different_email')::boolean;
    end if;
  else
    v_succ_send_ts_queries_to_different_email := coalesce(v_cur.send_ts_queries_to_different_email,false);
  end if;

  if (v_ov ? 'ts_queries_alt_email_address') then
    v_succ_ts_queries_alt_email_address := nullif(btrim(v_ov->>'ts_queries_alt_email_address'), '');
  else
    v_succ_ts_queries_alt_email_address := v_cur.ts_queries_alt_email_address;
  end if;

  if (v_ov ? 'is_ad_hoc') and jsonb_typeof(v_ov->'is_ad_hoc') <> 'null' then
    v_succ_is_ad_hoc := (v_ov->>'is_ad_hoc')::boolean;
  else
    v_succ_is_ad_hoc := v_cur.is_ad_hoc;
  end if;
  v_succ_is_ad_hoc := coalesce(v_succ_is_ad_hoc, false);

  if (v_ov ? 'default_submission_mode') then
    v_succ_default_submission_mode := nullif(btrim(v_ov->>'default_submission_mode'), '')::public.submission_mode_enum;
  else
    v_succ_default_submission_mode := v_cur.default_submission_mode;
  end if;

  if (v_ov ? 'week_ending_weekday_snapshot') and nullif(btrim(v_ov->>'week_ending_weekday_snapshot'), '') is not null then
    v_wew_succ := (v_ov->>'week_ending_weekday_snapshot')::int;
  else
    v_wew_succ := coalesce(v_cur.week_ending_weekday_snapshot, 0);
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','successor_fields_merged',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'merged', jsonb_build_object(
          'succ_candidate_id', coalesce(v_succ_candidate_id::text,''),
          'succ_client_id', coalesce(v_succ_client_id::text,''),
          'succ_role', coalesce(v_succ_role,''),
          'succ_band', coalesce(v_succ_band,''),
          'succ_display_site', coalesce(v_succ_display_site,''),
          'succ_ward_hint', coalesce(v_succ_ward_hint,''),
          'succ_pay_method_snapshot', coalesce(v_succ_pay_method_snapshot,''),
          'succ_week_ending_weekday_snapshot', v_wew_succ,
          'succ_overrideclientsettings', coalesce(v_succ_overrideclientsettings,false),
          'succ_weekly_timesheet_source', coalesce(v_succ_weekly_timesheet_source::text,''),
          'succ_no_timesheet_required', case when v_succ_no_timesheet_required is null then null else v_succ_no_timesheet_required end,
          'succ_daily_calc_of_invoices', case when v_succ_daily_calc_of_invoices is null then null else v_succ_daily_calc_of_invoices end,
          'succ_group_nightsat_sunbh', case when v_succ_group_nightsat_sunbh is null then null else v_succ_group_nightsat_sunbh end,
          'succ_is_nhsp', case when v_succ_is_nhsp is null then null else v_succ_is_nhsp end,
          'succ_autoprocess_hr', case when v_succ_autoprocess_hr is null then null else v_succ_autoprocess_hr end,
          'succ_requires_hr', case when v_succ_requires_hr is null then null else v_succ_requires_hr end,
          'succ_hr_attach_to_invoice', case when v_succ_hr_attach_to_invoice is null then null else v_succ_hr_attach_to_invoice end,
          'succ_ts_attach_to_invoice', case when v_succ_ts_attach_to_invoice is null then null else v_succ_ts_attach_to_invoice end,
          'succ_reference_number_required_to_issue_invoice', case when v_succ_reference_number_required_to_issue_invoice is null then null else v_succ_reference_number_required_to_issue_invoice end,
          'succ_send_manual_invoices_to_different_email', case when v_succ_send_manual_invoices_to_different_email is null then null else v_succ_send_manual_invoices_to_different_email end,
          'succ_manual_invoices_alt_email_address', coalesce(v_succ_manual_invoices_alt_email_address,''),
          'succ_send_ts_queries_to_different_email', case when v_succ_send_ts_queries_to_different_email is null then null else v_succ_send_ts_queries_to_different_email end,
          'succ_ts_queries_alt_email_address', coalesce(v_succ_ts_queries_alt_email_address,''),
          'succ_is_ad_hoc', coalesce(v_succ_is_ad_hoc,false),
          'succ_default_submission_mode', coalesce(v_succ_default_submission_mode::text,'')
        ),
        'overrides_keys', (
          select coalesce(jsonb_agg(k), '[]'::jsonb)
          from jsonb_object_keys(v_ov) as k
        )
      )
    );
  end if;

  if v_wew_succ < 0 or v_wew_succ > 6 then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','successor.week_ending_weekday_snapshot must be 0..6',
      'week_ending_weekday_snapshot', v_wew_succ
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_successor_wew_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  -- Route flag validations
  if v_succ_is_nhsp is true and v_succ_autoprocess_hr is true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','is_nhsp and autoprocess_hr cannot both be true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','is_nhsp && autoprocess_hr',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_succ_no_timesheet_required is true and v_succ_autoprocess_hr is distinct from true then
    v_err := jsonb_build_object(
      'error','INVALID_INPUT',
      'message','no_timesheet_required=true requires autoprocess_hr=true for successor'
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','fail_route_flag_validation',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'which','no_timesheet_required && autoprocess_hr != true',
          'error', v_err
        )
      );
    end if;

    raise exception using message = v_err::text;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','route_flags_ok',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
      )
    );
  end if;

  -- Snapshot predecessor state for audit before mutations
  v_before_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_start_date', v_cur.start_date,
    'predecessor_end_date', v_cur.end_date,
    'predecessor_candidate_id', v_cur.candidate_id,
    'predecessor_client_id', v_cur.client_id,
    'new_start_date', v_new_start,
    'new_end_date', v_new_end,
    'end_existing_on', v_close_to
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','before_state_snapshot',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'before_state', v_before_state
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Insert successor contract
  -- ─────────────────────────────────────────────────────────────
  insert into public.contracts as c (
    candidate_id,
    client_id,
    role,
    band,
    display_site,
    ward_hint,
    start_date,
    end_date,
    pay_method_snapshot,
    rates_json,
    std_hours_json,
    default_submission_mode,
    week_ending_weekday_snapshot,
    auto_invoice,
    require_reference_to_pay,
    require_reference_to_invoice,
    bucket_labels_json,
    std_schedule_json,
    mileage_pay_rate,
    mileage_charge_rate,
    additional_rates_json,
    created_at,
    updated_at,
    self_bill,
    weekly_timesheet_source,
    no_timesheet_required,
    daily_calc_of_invoices,
    group_nightsat_sunbh,
    is_nhsp,
    autoprocess_hr,
    requires_hr,
    hr_attach_to_invoice,
    ts_attach_to_invoice,
    overrideclientsettings,
    reference_number_required_to_issue_invoice,
    send_manual_invoices_to_different_email,
    manual_invoices_alt_email_address,
    send_ts_queries_to_different_email,
    ts_queries_alt_email_address,
    is_ad_hoc
  ) values (
    v_succ_candidate_id,
    v_succ_client_id,
    v_succ_role,
    v_succ_band,
    v_succ_display_site,
    v_succ_ward_hint,
    v_new_start,
    v_new_end,
    v_succ_pay_method_snapshot,
    v_succ_rates_json,
    v_succ_std_hours_json,
    v_succ_default_submission_mode,
    v_wew_succ::smallint,
    v_cur.auto_invoice,
    v_cur.require_reference_to_pay,
    v_cur.require_reference_to_invoice,
    v_succ_bucket_labels_json,
    v_succ_std_schedule_json,
    v_cur.mileage_pay_rate,
    v_cur.mileage_charge_rate,
    v_succ_additional_rates_json,
    v_now,
    v_now,
    v_cur.self_bill,
    v_succ_weekly_timesheet_source,
    v_succ_no_timesheet_required,
    v_succ_daily_calc_of_invoices,
    v_succ_group_nightsat_sunbh,
    v_succ_is_nhsp,
    v_succ_autoprocess_hr,
    v_succ_requires_hr,
    v_succ_hr_attach_to_invoice,
    v_succ_ts_attach_to_invoice,
    v_succ_overrideclientsettings,
    v_succ_reference_number_required_to_issue_invoice,
    v_succ_send_manual_invoices_to_different_email,
    v_succ_manual_invoices_alt_email_address,
    v_succ_send_ts_queries_to_different_email,
    v_succ_ts_queries_alt_email_address,
    v_succ_is_ad_hoc
  )
  returning c.* into v_succ;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','inserted_successor_contract',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'successor', jsonb_build_object(
          'id', v_succ.id,
          'candidate_id', coalesce(v_succ.candidate_id::text,''),
          'client_id', coalesce(v_succ.client_id::text,''),
          'start_date', v_succ.start_date,
          'end_date', v_succ.end_date,
          'week_ending_weekday_snapshot', v_succ.week_ending_weekday_snapshot
        )
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Update predecessor end_date (end-existing enforced)
  -- ─────────────────────────────────────────────────────────────
  update public.contracts as c
     set end_date = v_close_to,
         updated_at = v_now
   where c.id = v_cur.id;

  get diagnostics v_rc = row_count;

  v_cur.end_date := v_close_to;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','updated_predecessor_end_date',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'predecessor_id', v_cur.id::text,
        'new_end_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Generate/ensure base contract_weeks rows for predecessor + successor in their final windows
  -- (including planned_schedule_json derived from std_schedule_json when present)
  -- ─────────────────────────────────────────────────────────────
  with contract_targets as (
    select
      v_cur.id as contract_id,
      v_cur.client_id as client_id,
      v_cur.start_date as start_date,
      v_cur.end_date as end_date,
      v_wew_pred as wew,
      v_cur.std_schedule_json as std_schedule_json,
      v_cur.overrideclientsettings as overrideclientsettings,
      v_cur.default_submission_mode as default_submission_mode
    union all
    select
      v_succ.id as contract_id,
      v_succ.client_id as client_id,
      v_succ.start_date as start_date,
      v_succ.end_date as end_date,
      v_wew_succ as wew,
      v_succ.std_schedule_json as std_schedule_json,
      v_succ.overrideclientsettings as overrideclientsettings,
      v_succ.default_submission_mode as default_submission_mode
  ),
  targets as (
    select
      ct.contract_id,
      ct.client_id,
      ct.start_date,
      ct.end_date,
      ct.wew,
      ct.std_schedule_json,
      ct.overrideclientsettings,
      ct.default_submission_mode,
      (
        select cs.default_submission_mode
          from public.client_settings as cs
         where cs.client_id = ct.client_id
         order by cs.effective_from desc nulls last, cs.updated_at desc
         limit 1
      ) as client_default_submission_mode
    from contract_targets as ct
  ),
  targets2 as (
    select
      t.contract_id,
      t.start_date,
      t.end_date,
      t.wew,
      t.std_schedule_json,
      case
        when t.overrideclientsettings is true
          then coalesce(t.default_submission_mode, t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
        else coalesce(t.client_default_submission_mode, 'ELECTRONIC'::public.submission_mode_enum)
      end as submission_mode_snapshot,
      (t.start_date + (((t.wew - extract(dow from t.start_date)::int + 7) % 7)) * interval '1 day')::date as start_we,
      (t.end_date   + (((t.wew - extract(dow from t.end_date)::int   + 7) % 7)) * interval '1 day')::date as end_we
    from targets as t
  ),
  weeks as (
    select
      t2.contract_id,
      t2.start_date,
      t2.end_date,
      t2.wew,
      t2.std_schedule_json,
      t2.submission_mode_snapshot,
      gs::date as week_end
    from targets2 as t2
    cross join lateral generate_series(t2.start_we::timestamp, t2.end_we::timestamp, interval '7 days') as gs
  ),
  ins as (
    insert into public.contract_weeks as cw (
      contract_id,
      week_ending_date,
      additional_seq,
      status,
      timesheet_id,
      planned_schedule_json,
      created_at,
      updated_at,
      submission_mode_snapshot,
      is_adjustment,
      enforce_day_partition,
      allowed_days_mask,
      split_boundary_date,
      worker_note,
      split_group_key
    )
    select
      w.contract_id,
      w.week_end,
      0 as additional_seq,
      case
        when w.week_end <= current_date then 'OPEN'::public.contract_week_status_enum
        else 'PLANNED'::public.contract_week_status_enum
      end as status,
      null::uuid as timesheet_id,
      case
        when pj.plan_json is null then null
        when jsonb_typeof(pj.plan_json) <> 'array' then pj.plan_json
        when jsonb_array_length(pj.plan_json) = 0 then null
        else pj.plan_json
      end as planned_schedule_json,
      v_now as created_at,
      v_now as updated_at,
      w.submission_mode_snapshot,
      false as is_adjustment,
      false as enforce_day_partition,
      null::text as allowed_days_mask,
      null::date as split_boundary_date,
      null::text as worker_note,
      null::text as split_group_key
    from weeks as w
    left join lateral (
      select jsonb_agg(ent.entry_json order by ent.entry_date) as plan_json
      from (
        select
          (dt)::date as entry_date,
          jsonb_build_object(
            'date', to_char((dt)::date, 'YYYY-MM-DD'),
            'start', sc.cfg->>'start',
            'end',   sc.cfg->>'end',
            'breaks', case when jsonb_typeof(sc.cfg->'breaks') = 'array' then sc.cfg->'breaks' else '[]'::jsonb end,
            'break_minutes', br.break_minutes,
            'overnight', ov.overnight_flag,
            'expected_minutes', ov.expected_minutes
          ) as entry_json
        from generate_series(
               greatest(((w.week_end - interval '6 days')::date), w.start_date)::timestamp,
               least(w.week_end, w.end_date)::timestamp,
               interval '1 day'
             ) as dt
        cross join lateral (
          select case extract(dow from dt)::int
            when 1 then 'mon'
            when 2 then 'tue'
            when 3 then 'wed'
            when 4 then 'thu'
            when 5 then 'fri'
            when 6 then 'sat'
            else 'sun'
          end as day_key
        ) as dk
        cross join lateral (
          select (w.std_schedule_json -> dk.day_key) as cfg
        ) as sc
        cross join lateral (
          select
            case
              when (sc.cfg ? 'break_minutes')
               and (sc.cfg->>'break_minutes') is not null
               and (sc.cfg->>'break_minutes') ~ '^[0-9]+(\.[0-9]+)?$'
              then (sc.cfg->>'break_minutes')::numeric
              else 0::numeric
            end as break_minutes
        ) as br
        cross join lateral (
          select
            (sc.cfg->>'start') as start_str,
            (sc.cfg->>'end')   as end_str
        ) as se
        cross join lateral (
          select
            case
              when se.start_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.start_str,':',1)::int between 0 and 23
               and split_part(se.start_str,':',2)::int between 0 and 59
              then (split_part(se.start_str,':',1)::int * 60 + split_part(se.start_str,':',2)::int)
              else null
            end as start_mins,
            case
              when se.end_str ~ '^[0-9]{1,2}:[0-9]{2}$'
               and split_part(se.end_str,':',1)::int between 0 and 23
               and split_part(se.end_str,':',2)::int between 0 and 59
              then (split_part(se.end_str,':',1)::int * 60 + split_part(se.end_str,':',2)::int)
              else null
            end as end_mins
        ) as tm
        cross join lateral (
          select
            case
              when tm.start_mins is null or tm.end_mins is null then false
              when tm.end_mins <= tm.start_mins then true
              else false
            end as overnight_flag,
            case
              when tm.start_mins is null or tm.end_mins is null then 0
              when tm.end_mins <= tm.start_mins then (1440 - tm.start_mins + tm.end_mins)
              else (tm.end_mins - tm.start_mins)
            end as minutes_diff
        ) as md
        cross join lateral (
          select
            md.overnight_flag,
            greatest(0, (md.minutes_diff - br.break_minutes)::int) as expected_minutes
        ) as ov
        where w.std_schedule_json is not null
          and jsonb_typeof(w.std_schedule_json) = 'object'
          and jsonb_typeof(sc.cfg) = 'object'
          and tm.start_mins is not null
          and tm.end_mins is not null
      ) as ent
    ) as pj on true
    on conflict on constraint uq_contract_week do nothing
    returning 1
  )
  select 1 into v_dummy from ins limit 1;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','ensure_contract_weeks_done',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'note','weeks ensure CTE executed (v_dummy indicates at least one insert when non-null)',
        'v_dummy', v_dummy,
        'predecessor_id', v_cur.id::text,
        'successor_id', v_succ.id::text
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Delete predecessor draft weeks beyond truncated window (never touches submitted weeks)
  -- ─────────────────────────────────────────────────────────────
  delete from public.contract_weeks as cw
   where cw.contract_id = v_cur.id
     and cw.timesheet_id is null
     and cw.week_ending_date > v_end_we_old;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','deleted_predecessor_draft_weeks_beyond_trunc',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'v_end_we_old', v_end_we_old
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Clamp predecessor ending week planned schedule (draft rows only; never delete the week row)
  -- If none remain, set planned_schedule_json = []
  -- ─────────────────────────────────────────────────────────────
  with tgt as (
    select
      cw.id as contract_week_id,
      case
        when cw.planned_schedule_json is null then '[]'::jsonb
        when jsonb_typeof(cw.planned_schedule_json) <> 'array' then cw.planned_schedule_json
        else (
          select coalesce(jsonb_agg(e.elem order by e.ord), '[]'::jsonb)
          from jsonb_array_elements(cw.planned_schedule_json) with ordinality as e(elem, ord)
          where (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            and (e.elem->>'date')::date <= v_close_to
        )
      end as new_plan
    from public.contract_weeks as cw
    where cw.contract_id = v_cur.id
      and cw.week_ending_date = v_end_we_old
      and cw.timesheet_id is null
  )
  update public.contract_weeks as cw
     set planned_schedule_json = tgt.new_plan,
         updated_at = v_now
    from tgt
   where cw.id = tgt.contract_week_id;

  get diagnostics v_rc = row_count;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','clamped_predecessor_ending_week_plan',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'row_count', v_rc,
        'contract_id', v_cur.id::text,
        'week_ending_date', v_end_we_old,
        'clamp_to_date', v_close_to
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Split-week enforcement patch (draft rows only; includes additional_seq variants)
  -- ─────────────────────────────────────────────────────────────
  if v_split_week then
    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_old_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_cur.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_predecessor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_cur.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_old_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

    update public.contract_weeks as cw
       set enforce_day_partition = true,
           allowed_days_mask = v_new_mask,
           split_boundary_date = v_new_start,
           worker_note = v_split_note,
           split_group_key = v_split_group_key,
           updated_at = v_now
     where cw.contract_id = v_succ.id
       and cw.week_ending_date = v_boundary_week_end
       and cw.timesheet_id is null;

    get diagnostics v_rc = row_count;

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','split_week_patch_successor_weeks',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'row_count', v_rc,
          'contract_id', v_succ.id::text,
          'week_end', v_boundary_week_end,
          'allowed_days_mask', v_new_mask,
          'split_boundary_date', v_new_start,
          'split_group_key', v_split_group_key
        )
      );
    end if;

  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Compute schedule clashes inside DB (block unless forced)
  -- NOTE: avoid using CTE name "overlaps" (keyword/operator in SQL); use ovl_rows instead.
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_start',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'candidate_id', v_succ_candidate_id::text,
          'new_start', v_new_start,
          'new_end', v_new_end,
          'p_force_schedule_clashes', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    with params as (
      select
        (v_new_start - 1) as scan_from,
        (v_new_end + 1) as scan_to,
        (v_new_start - 7) as check_we_from,
        (v_new_end + 7) as check_we_to
    ),
    a_entries as (
      select
        cw.id as a_contract_week_id,
        cw.contract_id as a_contract_id,
        cw.week_ending_date as a_week_end,
        cw.additional_seq as a_additional_seq,
        (e.elem->>'date')::date as a_day_date,
        (e.elem->>'start') as a_start,
        (e.elem->>'end') as a_end,
        coalesce((e.elem->>'overnight')::boolean, false) as a_overnight
      from public.contract_weeks as cw
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      where cw.contract_id = v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= v_new_start
        and (e.elem->>'date')::date <= v_new_end
    ),
    a_ts as (
      select
        a.*,
        (a.a_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (a.a_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((a.a_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (a.a_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from a_entries as a
      cross join lateral (
        select
          case
            when a.a_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_start,':',1)::int between 0 and 23
             and split_part(a.a_start,':',2)::int between 0 and 59
            then (split_part(a.a_start,':',1)::int * 60 + split_part(a.a_start,':',2)::int)
            else null
          end as start_mins,
          case
            when a.a_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(a.a_end,':',1)::int between 0 and 23
             and split_part(a.a_end,':',2)::int between 0 and 59
            then (split_part(a.a_end,':',1)::int * 60 + split_part(a.a_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    b_entries as (
      select
        cw.id as b_contract_week_id,
        cw.contract_id as b_contract_id,
        c.client_id as b_client_id,
        cw.week_ending_date as b_week_end,
        cw.additional_seq as b_additional_seq,
        (e.elem->>'date')::date as b_day_date,
        (e.elem->>'start') as b_start,
        (e.elem->>'end') as b_end,
        coalesce((e.elem->>'overnight')::boolean, false) as b_overnight
      from public.contract_weeks as cw
      join public.contracts as c
        on c.id = cw.contract_id
      cross join lateral jsonb_array_elements(cw.planned_schedule_json) as e(elem)
      cross join params as p
      where c.candidate_id = v_succ_candidate_id
        and c.id <> v_succ.id
        and cw.planned_schedule_json is not null
        and jsonb_typeof(cw.planned_schedule_json) = 'array'
        and cw.week_ending_date >= p.check_we_from
        and cw.week_ending_date <= p.check_we_to
        and (e.elem->>'date') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        and (e.elem->>'date')::date >= p.scan_from
        and (e.elem->>'date')::date <= p.scan_to
    ),
    b_ts as (
      select
        b.*,
        (b.b_day_date::timestamp + (tm.start_mins * interval '1 minute')) as start_ts,
        (
          case
            when (b.b_overnight is true) or (tm.end_mins <= tm.start_mins)
              then ((b.b_day_date + 1)::timestamp + (tm.end_mins * interval '1 minute'))
            else (b.b_day_date::timestamp + (tm.end_mins * interval '1 minute'))
          end
        ) as end_ts
      from b_entries as b
      cross join lateral (
        select
          case
            when b.b_start ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_start,':',1)::int between 0 and 23
             and split_part(b.b_start,':',2)::int between 0 and 59
            then (split_part(b.b_start,':',1)::int * 60 + split_part(b.b_start,':',2)::int)
            else null
          end as start_mins,
          case
            when b.b_end ~ '^[0-9]{1,2}:[0-9]{2}$'
             and split_part(b.b_end,':',1)::int between 0 and 23
             and split_part(b.b_end,':',2)::int between 0 and 59
            then (split_part(b.b_end,':',1)::int * 60 + split_part(b.b_end,':',2)::int)
            else null
          end as end_mins
      ) as tm
      where tm.start_mins is not null
        and tm.end_mins is not null
    ),
    ovl_rows as (
      select
        greatest(a.start_ts, b.start_ts) as overlap_start,
        least(a.end_ts, b.end_ts) as overlap_end,

        a.a_contract_week_id,
        a.a_contract_id,
        a.a_week_end,
        a.a_additional_seq,
        to_char(a.a_day_date, 'YYYY-MM-DD') as a_date,
        a.a_start as a_start,
        a.a_end as a_end,

        b.b_contract_week_id,
        b.b_contract_id,
        b.b_client_id,
        b.b_week_end,
        b.b_additional_seq,
        to_char(b.b_day_date, 'YYYY-MM-DD') as b_date,
        b.b_start as b_start,
        b.b_end as b_end,

        row_number() over (order by greatest(a.start_ts, b.start_ts)) as rn,
        count(*) over () as total_count
      from a_ts as a
      join b_ts as b
        on a.start_ts < b.end_ts
       and a.end_ts > b.start_ts
      where least(a.end_ts, b.end_ts) > greatest(a.start_ts, b.start_ts)
    )
    select
      coalesce(max(o.total_count), 0),
      coalesce(
        jsonb_agg(
          jsonb_build_object(
            'overlap_start_utc', to_char(o.overlap_start, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
            'overlap_end_utc', to_char(o.overlap_end, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),

            'a_source', 'proposed',
            'a_contract_week_id', o.a_contract_week_id,
            'a_contract_id', o.a_contract_id,
            'a_week_end', o.a_week_end,
            'a_additional_seq', o.a_additional_seq,
            'a_date', o.a_date,
            'a_start', o.a_start,
            'a_end', o.a_end,

            'b_source', 'existing',
            'b_contract_week_id', o.b_contract_week_id,
            'b_contract_id', o.b_contract_id,
            'b_client_id', o.b_client_id,
            'b_week_end', o.b_week_end,
            'b_additional_seq', o.b_additional_seq,
            'b_date', o.b_date,
            'b_start', o.b_start,
            'b_end', o.b_end
          )
          order by o.overlap_start
        ) filter (where o.rn <= 500),
        '[]'::jsonb
      )
      into v_clash_count, v_schedule_clashes
      from ovl_rows as o;

    v_schedule_clashes := jsonb_build_object(
      'candidate_id', v_succ_candidate_id,
      'scan_from', to_char(v_new_start - 1, 'YYYY-MM-DD'),
      'scan_to', to_char(v_new_end + 1, 'YYYY-MM-DD'),
      'check_we_from', to_char(v_new_start - 7, 'YYYY-MM-DD'),
      'check_we_to', to_char(v_new_end + 7, 'YYYY-MM-DD'),
      'clash_count', coalesce(v_clash_count, 0),
      'clashes', coalesce(v_schedule_clashes, '[]'::jsonb)
    );

    if v_invoice_debug then
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','schedule_clash_scan_done',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'clash_count', coalesce(v_clash_count,0),
          'forced', coalesce(p_force_schedule_clashes,false)
        )
      );
    end if;

    if coalesce(v_clash_count, 0) > 0 and not coalesce(p_force_schedule_clashes, false) then
      v_err := v_schedule_clashes || jsonb_build_object('error','SCHEDULE_CLASH');

      if v_invoice_debug then
        v_dbg_steps := v_dbg_steps || jsonb_build_array(
          jsonb_build_object(
            'step','fail_schedule_clash_not_forced',
            'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
            'error', v_err
          )
        );
      end if;

      raise exception using message = v_err::text;
    end if;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Overlap warnings (date-range overlap with other contracts for successor candidate)
  -- ─────────────────────────────────────────────────────────────
  if v_succ_candidate_id is not null then
    select coalesce(
      jsonb_agg(
        jsonb_build_object(
          'contract_id', c2.id,
          'client_id', c2.client_id,
          'overlap_from', greatest(c2.start_date, v_new_start),
          'overlap_to', least(c2.end_date, v_new_end)
        )
        order by greatest(c2.start_date, v_new_start)
      ),
      '[]'::jsonb
    )
    into v_overlap_warnings
    from (
      select c2.*
      from public.contracts as c2
      where c2.candidate_id = v_succ_candidate_id
        and c2.id <> v_cur.id
        and c2.id <> v_succ.id
        and c2.start_date <= v_new_end
        and c2.end_date >= v_new_start
      order by c2.start_date
      limit 50
    ) as c2;
  end if;

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','overlap_warnings_computed',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'overlap_warning_count', case when v_overlap_warnings is null or jsonb_typeof(v_overlap_warnings) <> 'array' then null else jsonb_array_length(v_overlap_warnings) end
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Audit event (non-debug canonical writer)
  -- ─────────────────────────────────────────────────────────────
  v_after_state := jsonb_build_object(
    'predecessor_id', v_cur.id,
    'predecessor_closed_at', v_close_to,
    'successor_id', v_succ.id,
    'successor_start_date', v_succ.start_date,
    'successor_end_date', v_succ.end_date,
    'successor_candidate_id', v_succ.candidate_id,
    'successor_client_id', v_succ.client_id,
    'split_week', v_split_week,
    'split_group_key', v_split_group_key,
    'forced_schedule_clashes', coalesce(p_force_schedule_clashes, false),
    'forced_already_split_week', coalesce(p_force_already_split_week, false)
  );

  v_audit_reason :=
    'Clone & Extend: predecessor ' || v_cur.id::text ||
    ' closed to ' || to_char(v_close_to,'YYYY-MM-DD') ||
    '; successor ' || v_succ.id::text ||
    ' ' || to_char(v_succ.start_date,'YYYY-MM-DD') || '→' || to_char(v_succ.end_date,'YYYY-MM-DD') ||
    case when v_split_week then '; split week boundary ' || to_char(v_new_start,'YYYY-MM-DD') else '' end ||
    case when coalesce(p_force_schedule_clashes,false) then '; schedule clashes forced' else '' end ||
    case when coalesce(p_force_already_split_week,false) then '; already-split-week forced' else '' end;

  perform public._audit_insert(
    'contracts',
    v_cur.id::text,
    'CLONE_EXTEND',
    v_before_state,
    v_after_state,
    v_audit_reason,
    p_actor_user_id
  );

  if v_invoice_debug then
    v_dbg_steps := v_dbg_steps || jsonb_build_array(
      jsonb_build_object(
        'step','canonical_audit_written',
        'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'event','CLONE_EXTEND',
        'actor_user_id', coalesce(p_actor_user_id::text,'')
      )
    );
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- DEBUG AUDIT (single row per call; best-effort, never breaks flow)
  -- ─────────────────────────────────────────────────────────────
  if v_invoice_debug then
    begin
      v_dbg_stats := jsonb_build_object(
        'predecessor_id', v_cur.id,
        'successor_id', v_succ.id,
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', v_boundary_week_start,
        'boundary_week_end', v_boundary_week_end,
        'end_we_old', v_end_we_old,
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','finish',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'stats', v_dbg_stats
        )
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_DEBUG',
        jsonb_build_object(
          'predecessor_id', v_cur.id::text,
          'successor_id', v_succ.id::text,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps,
          'warnings', jsonb_build_object(
            'schedule_clashes', v_schedule_clashes,
            'overlap_warnings', v_overlap_warnings
          )
        ),
        'contracts',
        ('contract:' || v_cur.id::text),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  -- ─────────────────────────────────────────────────────────────
  -- Return payload
  -- ─────────────────────────────────────────────────────────────
  return jsonb_build_object(
    'successor', jsonb_build_object(
      'id', v_succ.id,
      'candidate_id', v_succ.candidate_id,
      'client_id', v_succ.client_id,
      'role', v_succ.role,
      'band', v_succ.band,
      'display_site', v_succ.display_site,
      'ward_hint', v_succ.ward_hint,
      'start_date', v_succ.start_date,
      'end_date', v_succ.end_date
    ),
    'closed_at', v_close_to,
    'split', case
      when v_split_week then jsonb_build_object(
        'week_start', v_boundary_week_start,
        'week_end', v_boundary_week_end,
        'boundary_date', v_new_start,
        'old_allowed_mask', v_old_mask,
        'new_allowed_mask', v_new_mask,
        'split_boundary_date', v_new_start,
        'worker_note', v_split_note,
        'split_group_key', v_split_group_key,
        'already_split', v_already_split
      )
      else null
    end,
    'overlap_warnings', v_overlap_warnings,
    'warnings', jsonb_build_object(
      'schedule_clashes', v_schedule_clashes,
      'overlap_warnings', v_overlap_warnings
    )
  );

exception when others then
  v_dbg_sqlstate := SQLSTATE;
  v_dbg_error := SQLERRM;

  begin
    get stacked diagnostics
      v_dbg_detail = PG_EXCEPTION_DETAIL,
      v_dbg_hint = PG_EXCEPTION_HINT,
      v_dbg_context = PG_EXCEPTION_CONTEXT;
  exception when others then
    v_dbg_detail := null;
    v_dbg_hint := null;
    v_dbg_context := null;
  end;

  if v_invoice_debug then
    begin
      v_dbg_steps := v_dbg_steps || jsonb_build_array(
        jsonb_build_object(
          'step','exception',
          'at_utc', to_char(clock_timestamp() at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint
        )
      );

      v_dbg_stats := jsonb_build_object(
        'predecessor_id', coalesce(v_cur.id::text,''),
        'successor_id', coalesce(v_succ.id::text,''),
        'split_week', v_split_week,
        'already_split', v_already_split,
        'boundary_week_start', coalesce(v_boundary_week_start::text,''),
        'boundary_week_end', coalesce(v_boundary_week_end::text,''),
        'end_we_old', coalesce(v_end_we_old::text,''),
        'clash_count', coalesce(v_clash_count,0),
        'forced_schedule_clashes', coalesce(p_force_schedule_clashes,false),
        'forced_already_split_week', coalesce(p_force_already_split_week,false)
      );

      perform public._inv_write_audit(
        null,
        'CONTRACTS_CLONE_EXTEND_ERROR',
        jsonb_build_object(
          'predecessor_id', coalesce(v_cur.id::text,''),
          'successor_id', coalesce(v_succ.id::text,''),
          'sqlstate', v_dbg_sqlstate,
          'error', v_dbg_error,
          'detail', v_dbg_detail,
          'hint', v_dbg_hint,
          'context', v_dbg_context,
          'stats', v_dbg_stats,
          'steps', v_dbg_steps
        ),
        'contracts',
        ('contract:' || coalesce(p_contract_id::text,'')),
        null,
        'INVOICE_DEBUG',
        null, null, null
      );
    exception when others then
      null;
    end;
  end if;

  raise;
end;
$function$;

-- contracts_enforce_overrideclientsettings()
CREATE OR REPLACE FUNCTION public.contracts_enforce_overrideclientsettings()
 RETURNS trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_no_timesheet_required boolean;
  v_daily_calc_of_invoices boolean;
  v_group_nightsat_sunbh boolean;
  v_is_nhsp boolean;
  v_autoprocess_hr boolean;
  v_requires_hr boolean;
  v_hr_attach_to_invoice boolean;
  v_ts_attach_to_invoice boolean;
  v_reference_number_required_to_issue_invoice boolean;
  v_send_manual_invoices_to_different_email boolean;
  v_manual_invoices_alt_email_address text;
begin
  -- Normalise NULL -> false (shouldn't happen because column is NOT NULL, but safe)
  new.overrideclientsettings := coalesce(new.overrideclientsettings, false);

  -- ------------------------------------------------------------
  -- If override is OFF: clear all governed override fields
  -- ------------------------------------------------------------
  if new.overrideclientsettings = false then
    -- nullable policy flags in contracts (must be NULL when override is off)
    new.no_timesheet_required   := null;
    new.daily_calc_of_invoices  := null;
    new.group_nightsat_sunbh    := null;
    new.is_nhsp                 := null;
    new.autoprocess_hr          := null;
    new.requires_hr             := null;
    new.hr_attach_to_invoice    := null;
    new.ts_attach_to_invoice    := null;

    -- new governed fields
    new.reference_number_required_to_issue_invoice := null;
    new.send_manual_invoices_to_different_email   := null;
    new.manual_invoices_alt_email_address         := null;

    -- default_submission_mode is governed; when override is off we store NULL (inherit)
    new.default_submission_mode := null;

    return new;
  end if;

  -- ------------------------------------------------------------
  -- If override is ON: fill any NULL booleans from client_settings
  -- ------------------------------------------------------------
  select
    cs.no_timesheet_required,
    cs.daily_calc_of_invoices,
    cs.group_nightsat_sunbh,
    cs.is_nhsp,
    cs.autoprocess_hr,
    cs.requires_hr,
    cs.hr_attach_to_invoice,
    cs.ts_attach_to_invoice,

    cs.reference_number_required_to_issue_invoice,
    cs.send_manual_invoices_to_different_email,
    cs.manual_invoices_alt_email_address
  into
    v_no_timesheet_required,
    v_daily_calc_of_invoices,
    v_group_nightsat_sunbh,
    v_is_nhsp,
    v_autoprocess_hr,
    v_requires_hr,
    v_hr_attach_to_invoice,
    v_ts_attach_to_invoice,
    v_reference_number_required_to_issue_invoice,
    v_send_manual_invoices_to_different_email,
    v_manual_invoices_alt_email_address
  from public.client_settings cs
  where cs.client_id = new.client_id
  order by cs.effective_from desc nulls last, cs.updated_at desc
  limit 1;

  -- For booleans: never leave NULL when override is ON.
  -- If no client_settings row exists, fall back to defaults that match client_settings defaults.
  new.no_timesheet_required  := coalesce(new.no_timesheet_required,  v_no_timesheet_required,  false);
  new.daily_calc_of_invoices := coalesce(new.daily_calc_of_invoices, v_daily_calc_of_invoices, false);
  new.group_nightsat_sunbh   := coalesce(new.group_nightsat_sunbh,   v_group_nightsat_sunbh,   false);
  new.is_nhsp                := coalesce(new.is_nhsp,                v_is_nhsp,                false);
  new.autoprocess_hr         := coalesce(new.autoprocess_hr,         v_autoprocess_hr,         false);
  new.requires_hr            := coalesce(new.requires_hr,            v_requires_hr,            false);

  -- These default TRUE in client_settings
  new.hr_attach_to_invoice   := coalesce(new.hr_attach_to_invoice,   v_hr_attach_to_invoice,   true);
  new.ts_attach_to_invoice   := coalesce(new.ts_attach_to_invoice,   v_ts_attach_to_invoice,   true);

  -- New governed issue/email flags (default FALSE in client_settings)
  new.reference_number_required_to_issue_invoice :=
    coalesce(new.reference_number_required_to_issue_invoice, v_reference_number_required_to_issue_invoice, false);

  new.send_manual_invoices_to_different_email :=
    coalesce(new.send_manual_invoices_to_different_email, v_send_manual_invoices_to_different_email, false);

  -- If send_manual is TRUE, alt email must be present (try fill from client_settings first)
  if new.send_manual_invoices_to_different_email = true then
    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      new.manual_invoices_alt_email_address := v_manual_invoices_alt_email_address;
    end if;

    if new.manual_invoices_alt_email_address is null or btrim(new.manual_invoices_alt_email_address) = '' then
      raise exception 'manual_invoices_alt_email_address is required when send_manual_invoices_to_different_email is true';
    end if;

    -- Store trimmed version
    new.manual_invoices_alt_email_address := btrim(new.manual_invoices_alt_email_address);
  end if;

  -- default_submission_mode:
  -- You explicitly want contracts.default_submission_mode to be allowed NULL to inherit client settings.
  -- Therefore we DO NOT auto-fill it here.

  return new;
end;
$function$;

-- correction_financials_policy_resolve_v1(uuid,uuid,text,text,text,boolean,integer)
CREATE OR REPLACE FUNCTION public.correction_financials_policy_resolve_v1(p_timesheet_id uuid, p_operation_id uuid, p_source_row_key text, p_correction_action text, p_expected_envelope_fingerprint text DEFAULT NULL::text, p_lock_rows boolean DEFAULT false, p_max_depth integer DEFAULT 32)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_operation public.import_apply_operations%rowtype;
  v_contract jsonb;
  v_contract_fingerprint text;
  v_root_timesheet_id uuid;
  v_units jsonb;
  v_unit jsonb;
  v_unit_count integer;
  v_envelope jsonb;
  v_envelope_fingerprint text;
  v_source_shift public.nhsp_shifts%rowtype;
begin
  if p_timesheet_id is null or p_operation_id is null
     or nullif(btrim(coalesce(p_source_row_key, '')), '') is null then
    raise exception 'CORRECTION_POLICY_RESOLVER_SCOPE_REQUIRED'
      using errcode = '22023';
  end if;
  if upper(btrim(coalesce(p_correction_action, ''))) not in ('CHANGED_HOURS','CANCELLATION') then
    raise exception 'CORRECTION_POLICY_ACTION_INVALID' using errcode = '22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_POLICY_MAX_DEPTH_INVALID' using errcode = '22023';
  end if;

  if p_lock_rows then
    select o.* into v_operation
    from public.import_apply_operations o
    where o.id = p_operation_id
    for update;
  else
    select o.* into v_operation
    from public.import_apply_operations o
    where o.id = p_operation_id;
  end if;
  if not found then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_FOUND' using errcode = 'P0002';
  end if;
  if v_operation.state in ('BLOCKED','FAILED_BEFORE_COMMIT') then
    raise exception 'CORRECTION_POLICY_OPERATION_NOT_ACTIVE'
      using errcode = 'P0001', detail = jsonb_build_object('state', v_operation.state)::text;
  end if;

  v_contract := v_operation.response_json -> 'correction_operation_contract';
  if jsonb_typeof(v_contract) <> 'object'
     or v_contract ->> 'schema_version' <> 'IMPORT_CORRECTION_OPERATION_V2'
     or v_contract ->> 'route_family' <> 'IMPORT_AUTHORITATIVE'
     or v_contract ->> 'operation_id' is distinct from p_operation_id::text
     or v_contract ->> 'import_id' is distinct from v_operation.import_id::text
     or v_contract ->> 'request_hash' is distinct from v_operation.request_hash then
    raise exception 'CORRECTION_POLICY_OPERATION_CONTRACT_INVALID'
      using errcode = 'P0001';
  end if;

  v_contract_fingerprint := encode(
    extensions.digest(
      convert_to((v_contract - 'operation_contract_fingerprint')::text, 'UTF8'),
      'sha256'::text
    ), 'hex'
  );
  if nullif(v_contract ->> 'operation_contract_fingerprint', '') is distinct from v_contract_fingerprint then
    raise exception 'CORRECTION_POLICY_OPERATION_CONTRACT_FINGERPRINT_INVALID'
      using errcode = 'P0001';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id = p_timesheet_id
    union all
    select parent.timesheet_id, parent.parent_timesheet_id, a.depth + 1,
           a.path || parent.timesheet_id,
           parent.timesheet_id = any(a.path)
    from ancestors a
    join public.timesheets parent on parent.timesheet_id = a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth < p_max_depth
  )
  select a.timesheet_id into v_root_timesheet_id
  from ancestors a order by a.depth desc limit 1;
  if v_root_timesheet_id is null then
    raise exception 'CORRECTION_POLICY_TIMESHEET_NOT_FOUND' using errcode = 'P0002';
  end if;

  v_units := v_contract -> 'correction_units';
  select count(*)::integer, min(unit::text)::jsonb
  into v_unit_count, v_unit
  from jsonb_array_elements(case when jsonb_typeof(v_units)='array' then v_units else '[]'::jsonb end) unit
  where unit ->> 'root_timesheet_id' = v_root_timesheet_id::text
    and unit ->> 'source_row_key' = p_source_row_key
    and upper(unit ->> 'correction_action') = upper(p_correction_action);

  if v_unit_count <> 1 then
    raise exception 'CORRECTION_POLICY_OPERATION_UNIT_NOT_UNIQUE'
      using errcode = 'P0001',
            detail = jsonb_build_object('matching_unit_count', v_unit_count)::text;
  end if;

  v_envelope := v_unit -> 'policy_envelope';
  if jsonb_typeof(v_envelope) <> 'object'
     or v_envelope #>> '{operation,operation_id}' is distinct from p_operation_id::text
     or v_envelope #>> '{classification,source_row_key}' is distinct from p_source_row_key
     or v_envelope ->> 'root_timesheet_id' is distinct from v_root_timesheet_id::text
     or v_envelope ->> 'correction_shape' is distinct from v_unit ->> 'correction_shape'
     or v_envelope -> 'expected_member_roles' is distinct from v_unit -> 'expected_member_roles'
     or v_envelope ->> 'expected_member_count' is distinct from v_unit ->> 'expected_member_count' then
    raise exception 'CORRECTION_POLICY_OPERATION_ENVELOPE_SCOPE_MISMATCH'
      using errcode = 'P0001';
  end if;

  v_envelope_fingerprint := encode(
    extensions.digest(convert_to((v_envelope - 'envelope_fingerprint')::text,'UTF8'),'sha256'::text),
    'hex'
  );
  if nullif(v_envelope ->> 'envelope_fingerprint', '') is distinct from v_envelope_fingerprint
     or v_unit ->> 'policy_envelope_fingerprint' is distinct from v_envelope_fingerprint then
    raise exception 'CORRECTION_POLICY_ENVELOPE_FINGERPRINT_INVALID'
      using errcode = 'P0001';
  end if;

  if p_lock_rows then
    select ns.* into v_source_shift
    from public.nhsp_shifts ns
    where ns.external_row_key=p_source_row_key
    for update;
  else
    select ns.* into v_source_shift
    from public.nhsp_shifts ns
    where ns.external_row_key=p_source_row_key;
  end if;
  if not found
     or v_source_shift.id::text is distinct from v_envelope#>>'{classification,source_shift_id}'
     or v_source_shift.source_system is distinct from v_operation.source_system then
    raise exception 'CORRECTION_POLICY_LIVE_SOURCE_IDENTITY_MISMATCH'
      using errcode='P0001';
  end if;
  if upper(p_correction_action)='CHANGED_HOURS'
     and not exists (
       select 1
       from public.hr_rows source_row
       where source_row.import_id = v_operation.import_id
         and source_row.external_row_key = p_source_row_key
     ) then
    -- Phase 3 resolves the frozen correction envelope before it replaces the
    -- source shift truth.  At this point latest_import_id must still identify
    -- the preceding committed revision.  Canonicality is instead proven by
    -- the fingerprinted operation contract above plus the exact immutable row
    -- in the operation's import; cancellation keeps its separate committed
    -- cancellation proof below.
    raise exception 'CORRECTION_POLICY_CHANGED_HOURS_ACTION_NOT_CANONICAL'
      using errcode='P0001';
  end if;
  if upper(p_correction_action)='CANCELLATION'
     and v_source_shift.cancelled_by_import_id is distinct from v_operation.import_id then
    raise exception 'CORRECTION_POLICY_CANCELLATION_ACTION_NOT_CANONICAL'
      using errcode='P0001';
  end if;

  if nullif(btrim(coalesce(p_expected_envelope_fingerprint, '')), '') is not null
     and p_expected_envelope_fingerprint is distinct from v_envelope_fingerprint then
    raise exception 'CORRECTION_POLICY_EXPECTED_FINGERPRINT_MISMATCH'
      using errcode = '40001',
            detail = jsonb_build_object(
              'expected', p_expected_envelope_fingerprint,
              'actual', v_envelope_fingerprint
            )::text;
  end if;
  return v_envelope;
end;
$function$;

-- document_templates_delete(uuid,uuid)
CREATE OR REPLACE FUNCTION public.document_templates_delete(p_template_id uuid, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_exists boolean;
  v_mail_outbox_rows int := 0;
  v_comms_outbox_rows int := 0;
  v_mailshot_runs_rows int := 0;
  v_deleted int := 0;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  select true
    into v_exists
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  -- To allow deletion while keeping outbox logs indefinitely, we detach references first.
  update public.mail_outbox mo
  set document_template_id = null
  where mo.document_template_id = p_template_id;
  get diagnostics v_mail_outbox_rows = row_count;

  update public.comms_outbox co
  set document_template_id = null
  where co.document_template_id = p_template_id;
  get diagnostics v_comms_outbox_rows = row_count;

  update public.mailshot_runs mr
  set document_template_id = null
  where mr.document_template_id = p_template_id;
  get diagnostics v_mailshot_runs_rows = row_count;

  delete from public.document_templates dt
  where dt.id = p_template_id;
  get diagnostics v_deleted = row_count;

  return jsonb_build_object(
    'ok', (v_deleted = 1),
    'deleted', v_deleted,
    'detached', jsonb_build_object(
      'mail_outbox_rows', v_mail_outbox_rows,
      'comms_outbox_rows', v_comms_outbox_rows,
      'mailshot_runs_rows', v_mailshot_runs_rows
    )
  );
end;
$function$;

-- document_templates_duplicate(uuid,text,uuid)
CREATE OR REPLACE FUNCTION public.document_templates_duplicate(p_template_id uuid, p_new_filename text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_src public.document_templates%rowtype;
  v_new_id uuid;
  v_new public.document_templates%rowtype;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_new_filename is null or length(btrim(p_new_filename)) = 0 then
    raise exception 'new_filename required';
  end if;

  select dt.*
    into v_src
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  insert into public.document_templates(
    entity_type,
    output_type,
    filename,
    description,
    email_type,
    selected_field_keys,
    template_content_json,
    created_by,
    created_at_utc,
    updated_at_utc
  )
  values (
    v_src.entity_type,
    v_src.output_type,
    btrim(p_new_filename),
    v_src.description,
    v_src.email_type,
    v_src.selected_field_keys,
    v_src.template_content_json,
    p_actor_user_id,
    v_now,
    v_now
  )
  returning id into v_new_id;

  select dt.*
    into v_new
  from public.document_templates dt
  where dt.id = v_new_id;

  return jsonb_build_object(
    'ok', true,
    'template', jsonb_build_object(
      'id', v_new.id::text,
      'entity_type', v_new.entity_type,
      'output_type', v_new.output_type,
      'filename', v_new.filename,
      'description', v_new.description,
      'email_type', v_new.email_type,
      'selected_field_keys', to_jsonb(v_new.selected_field_keys),
      'template_content_json', v_new.template_content_json,
      'created_by', case when v_new.created_by is null then null else v_new.created_by::text end,
      'created_at_utc', v_new.created_at_utc::text,
      'updated_at_utc', v_new.updated_at_utc::text
    )
  );
end;
$function$;

-- document_templates_get(uuid)
CREATE OR REPLACE FUNCTION public.document_templates_get(p_template_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_row public.document_templates%rowtype;
begin
  if p_template_id is null then
    raise exception 'template_id required';
  end if;

  select dt.*
    into v_row
  from public.document_templates dt
  where dt.id = p_template_id;

  if not found then
    raise exception 'document_template not found: %', p_template_id;
  end if;

  return jsonb_build_object(
    'id', v_row.id::text,
    'entity_type', v_row.entity_type,
    'output_type', v_row.output_type,
    'filename', v_row.filename,
    'description', v_row.description,
    'email_type', v_row.email_type,
    'selected_field_keys', to_jsonb(v_row.selected_field_keys),
    'template_content_json', v_row.template_content_json,
    'created_by', case when v_row.created_by is null then null else v_row.created_by::text end,
    'created_at_utc', v_row.created_at_utc::text,
    'updated_at_utc', v_row.updated_at_utc::text
  );
end;
$function$;

-- document_templates_list(text,text)
CREATE OR REPLACE FUNCTION public.document_templates_list(p_entity_type text, p_output_type text)
 RETURNS TABLE(id uuid, entity_type text, output_type text, filename text, description text, email_type text, created_by uuid, created_at_utc timestamp with time zone, updated_at_utc timestamp with time zone)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  select
    dt.id,
    dt.entity_type,
    dt.output_type,
    dt.filename,
    dt.description,
    dt.email_type,
    dt.created_by,
    dt.created_at_utc,
    dt.updated_at_utc
  from public.document_templates dt
  where dt.entity_type = p_entity_type
    and dt.output_type = p_output_type
  order by lower(dt.filename) asc, dt.created_at_utc asc;
$function$;

-- document_templates_upsert(uuid,text,text,text,text,text,text[],jsonb,uuid)
CREATE OR REPLACE FUNCTION public.document_templates_upsert(p_template_id uuid, p_entity_type text, p_output_type text, p_filename text, p_description text, p_email_type text, p_selected_field_keys text[], p_template_content_json jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_now timestamptz := now();
  v_id uuid;
  v_existing public.document_templates%rowtype;
  v_row public.document_templates%rowtype;

  v_entity_type_norm text := nullif(lower(btrim(coalesce(p_entity_type, ''))), '');
  v_output_type_norm text := upper(btrim(coalesce(p_output_type, '')));
  v_filename_norm text := nullif(btrim(coalesce(p_filename, '')), '');
  v_email_type_norm text := nullif(lower(btrim(coalesce(p_email_type, ''))), '');

  v_selected_raw text[] := coalesce(p_selected_field_keys, '{}'::text[]);
  v_selected_accepted text[] := '{}'::text[];
  v_selected_rejected text[] := '{}'::text[];

  v_content jsonb := coalesce(p_template_content_json, '{}'::jsonb);
  v_attachment_cfg jsonb := '{}'::jsonb;
  v_attachment_items_raw jsonb := '[]'::jsonb;
  v_attachment_items_norm jsonb := '[]'::jsonb;
  v_attachment_item jsonb := '{}'::jsonb;
  v_attachment_item_idx integer := 0;
  v_attachment_item_r2_key text;
  v_attachment_item_filename text;
  v_attachment_item_content_type text;
  v_attachment_item_source text;
  v_attachment_item_source_label text;
  v_attachment_item_read_only boolean := false;
  v_attachment_item_size_bytes bigint;
  v_attach_timesheet_pdf boolean := false;
  v_attach_invoice_pdf boolean := false;

  v_whatsapp_provider text;
  v_whatsapp_template_name text;
  v_whatsapp_param_name text;
  v_whatsapp_message_text text;
  v_sms_voice_message_text text;
  v_word_body_html text;
  v_word_body_text text;

  v_rejected_details jsonb := '[]'::jsonb;
  v_warnings jsonb := '[]'::jsonb;

  v_rejected_total int := 0;
  v_reason_field_not_found int := 0;
  v_reason_disabled_globally int := 0;
  v_reason_not_allowed_for_entity int := 0;
  v_reason_not_resolvable_for_entity int := 0;
  v_reason_stale int := 0;
  v_duplicate_count int := 0;
begin
  if v_entity_type_norm is null then
    raise exception 'entity_type required';
  end if;

  if v_output_type_norm is null or v_output_type_norm = '' then
    raise exception 'output_type required';
  end if;

  if v_filename_norm is null then
    raise exception 'filename required';
  end if;

  if p_actor_user_id is null then
    raise exception 'actor_user_id required';
  end if;

  if p_template_content_json is not null and jsonb_typeof(p_template_content_json) <> 'object' then
    raise exception 'template_content_json must be object';
  end if;

  perform 1
  from public.v_mailshot_resolution_graph as rg
  where rg.root_entity_type = v_entity_type_norm
  limit 1;

  if not found then
    raise exception 'invalid entity_type: %', v_entity_type_norm;
  end if;

  if v_output_type_norm not in ('EMAIL','WHATSAPP','SMS','VOICE','WORD','EXCEL') then
    raise exception 'invalid output_type: %', v_output_type_norm;
  end if;

  if v_output_type_norm = 'EMAIL' then
    if v_email_type_norm is not null and v_email_type_norm not in ('plain','html') then
      raise exception 'email_type must be plain or html for EMAIL';
    end if;
  else
    v_email_type_norm := null;
  end if;

  if v_content ? 'mailshot_attachments' then
    if jsonb_typeof(v_content->'mailshot_attachments') <> 'object' then
      raise exception 'mailshot_attachments must be object';
    end if;
    v_attachment_cfg := coalesce(v_content->'mailshot_attachments', '{}'::jsonb);
  else
    v_attachment_cfg := '{}'::jsonb;
  end if;

  if v_attachment_cfg ? 'attach_authoritative_timesheet_pdf'
     and jsonb_typeof(v_attachment_cfg->'attach_authoritative_timesheet_pdf') <> 'boolean' then
    raise exception 'mailshot_attachments.attach_authoritative_timesheet_pdf must be boolean';
  end if;

  if v_attachment_cfg ? 'attach_authoritative_invoice_pdf'
     and jsonb_typeof(v_attachment_cfg->'attach_authoritative_invoice_pdf') <> 'boolean' then
    raise exception 'mailshot_attachments.attach_authoritative_invoice_pdf must be boolean';
  end if;

  if v_attachment_cfg ? 'attachments'
     and jsonb_typeof(v_attachment_cfg->'attachments') <> 'array' then
    raise exception 'mailshot_attachments.attachments must be array';
  end if;

  if v_attachment_cfg ? 'template_files'
     and jsonb_typeof(v_attachment_cfg->'template_files') <> 'array' then
    raise exception 'mailshot_attachments.template_files must be array';
  end if;

  if v_attachment_cfg ? 'files'
     and jsonb_typeof(v_attachment_cfg->'files') <> 'array' then
    raise exception 'mailshot_attachments.files must be array';
  end if;

  v_attachment_items_raw :=
    coalesce(case when v_attachment_cfg ? 'attachments' then v_attachment_cfg->'attachments' else '[]'::jsonb end, '[]'::jsonb)
    ||
    coalesce(case when v_attachment_cfg ? 'template_files' then v_attachment_cfg->'template_files' else '[]'::jsonb end, '[]'::jsonb)
    ||
    coalesce(case when v_attachment_cfg ? 'files' then v_attachment_cfg->'files' else '[]'::jsonb end, '[]'::jsonb);

  v_attachment_items_norm := '[]'::jsonb;
  v_attachment_item_idx := 0;

  for v_attachment_item in
    select jbe.value
    from jsonb_array_elements(v_attachment_items_raw) as jbe(value)
  loop
    v_attachment_item_idx := v_attachment_item_idx + 1;

    if jsonb_typeof(v_attachment_item) <> 'object' then
      raise exception 'mailshot_attachments.attachments[%] must be object', v_attachment_item_idx;
    end if;

    v_attachment_item_r2_key := nullif(btrim(coalesce(v_attachment_item->>'r2_key', '')), '');
    v_attachment_item_filename := coalesce(
      nullif(btrim(coalesce(v_attachment_item->>'filename', '')), ''),
      nullif(btrim(coalesce(v_attachment_item->>'name', '')), '')
    );
    v_attachment_item_content_type := coalesce(
      nullif(btrim(coalesce(v_attachment_item->>'content_type', '')), ''),
      nullif(btrim(coalesce(v_attachment_item->>'contentType', '')), '')
    );
    v_attachment_item_source := nullif(btrim(coalesce(v_attachment_item->>'source', '')), '');
    v_attachment_item_source_label := coalesce(
      nullif(btrim(coalesce(v_attachment_item->>'source_label', '')), ''),
      nullif(btrim(coalesce(v_attachment_item->>'sourceLabel', '')), '')
    );

    if v_attachment_item_r2_key is null then
      raise exception 'mailshot_attachments.attachments[%].r2_key required', v_attachment_item_idx;
    end if;

    if v_attachment_item_filename is null then
      raise exception 'mailshot_attachments.attachments[%].filename required', v_attachment_item_idx;
    end if;

    if v_attachment_item ? 'read_only' then
      if jsonb_typeof(v_attachment_item->'read_only') <> 'boolean' then
        raise exception 'mailshot_attachments.attachments[%].read_only must be boolean', v_attachment_item_idx;
      end if;
      v_attachment_item_read_only := coalesce((v_attachment_item->>'read_only')::boolean, false);
    elsif v_attachment_item ? 'readOnly' then
      if jsonb_typeof(v_attachment_item->'readOnly') <> 'boolean' then
        raise exception 'mailshot_attachments.attachments[%].readOnly must be boolean', v_attachment_item_idx;
      end if;
      v_attachment_item_read_only := coalesce((v_attachment_item->>'readOnly')::boolean, false);
    else
      v_attachment_item_read_only := false;
    end if;

    v_attachment_item_size_bytes := null;
    if nullif(btrim(coalesce(v_attachment_item->>'size_bytes', '')), '') is not null then
      if btrim(coalesce(v_attachment_item->>'size_bytes', '')) !~ '^\d+$' then
        raise exception 'mailshot_attachments.attachments[%].size_bytes must be non-negative integer', v_attachment_item_idx;
      end if;
      v_attachment_item_size_bytes := (v_attachment_item->>'size_bytes')::bigint;
    elsif nullif(btrim(coalesce(v_attachment_item->>'sizeBytes', '')), '') is not null then
      if btrim(coalesce(v_attachment_item->>'sizeBytes', '')) !~ '^\d+$' then
        raise exception 'mailshot_attachments.attachments[%].sizeBytes must be non-negative integer', v_attachment_item_idx;
      end if;
      v_attachment_item_size_bytes := (v_attachment_item->>'sizeBytes')::bigint;
    end if;

    v_attachment_items_norm := v_attachment_items_norm || jsonb_build_array(
      jsonb_strip_nulls(
        jsonb_build_object(
          'r2_key', v_attachment_item_r2_key,
          'filename', v_attachment_item_filename,
          'content_type', v_attachment_item_content_type,
          'source', v_attachment_item_source,
          'source_label', v_attachment_item_source_label,
          'read_only', v_attachment_item_read_only,
          'size_bytes', v_attachment_item_size_bytes
        )
      )
    );
  end loop;

  v_attach_timesheet_pdf := coalesce((v_attachment_cfg->>'attach_authoritative_timesheet_pdf')::boolean, false);
  v_attach_invoice_pdf := coalesce((v_attachment_cfg->>'attach_authoritative_invoice_pdf')::boolean, false);

  if v_output_type_norm <> 'EMAIL' then
    if v_attach_timesheet_pdf or v_attach_invoice_pdf or jsonb_array_length(v_attachment_items_norm) > 0 then
      raise exception 'mailshot attachments are only supported for EMAIL templates';
    end if;

    v_content := v_content - 'mailshot_attachments';
  else
    if v_entity_type_norm = 'timesheet' then
      if v_attach_invoice_pdf then
        raise exception 'invoice PDF attachment preset is not valid for timesheet EMAIL templates';
      end if;
    elsif v_entity_type_norm = 'invoice' then
      if v_attach_timesheet_pdf then
        raise exception 'timesheet PDF attachment preset is not valid for invoice EMAIL templates';
      end if;
    else
      if v_attach_timesheet_pdf or v_attach_invoice_pdf then
        raise exception 'root-document PDF attachment presets are only valid for timesheet or invoice EMAIL templates';
      end if;
    end if;

    v_content := (v_content - 'mailshot_attachments') || jsonb_build_object(
      'mailshot_attachments',
      jsonb_build_object(
        'attach_authoritative_timesheet_pdf',
        case when v_entity_type_norm = 'timesheet' then v_attach_timesheet_pdf else false end,
        'attach_authoritative_invoice_pdf',
        case when v_entity_type_norm = 'invoice' then v_attach_invoice_pdf else false end,
        'attachments', coalesce(v_attachment_items_norm, '[]'::jsonb)
      )
    );
  end if;

  if v_output_type_norm = 'WHATSAPP' then
    v_whatsapp_provider := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'provider_key','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->>'provider','')), ''),
      nullif(btrim(coalesce(v_content->>'provider_key','')), '')
    );

    if v_whatsapp_provider is not null and upper(v_whatsapp_provider) <> 'WATI' then
      raise exception 'whatsapp template_content_json provider must be WATI';
    end if;

    v_whatsapp_template_name := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'templateName','')), ''),
      nullif(btrim(coalesce(v_content->>'wati_template_name','')), ''),
      nullif(btrim(coalesce(v_content->>'watiTemplateName','')), ''),
      nullif(btrim(coalesce(v_content->>'template_name','')), ''),
      nullif(btrim(coalesce(v_content->>'templateName','')), '')
    );

    if v_whatsapp_template_name is null then
      raise exception 'whatsapp template_content_json missing WATI template_name';
    end if;

    v_whatsapp_param_name := coalesce(
      nullif(btrim(coalesce(v_content->'provider_contract'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider_contract'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'wati'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'whatsapp'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->'provider'->>'paramName','')), ''),
      nullif(btrim(coalesce(v_content->>'wati_param_name','')), ''),
      nullif(btrim(coalesce(v_content->>'watiParamName','')), ''),
      nullif(btrim(coalesce(v_content->>'param_name','')), ''),
      nullif(btrim(coalesce(v_content->>'paramName','')), '')
    );

    if v_whatsapp_param_name is null then
      raise exception 'whatsapp template_content_json missing WATI param_name';
    end if;

    v_whatsapp_message_text := nullif(btrim(coalesce(v_content->>'message_text','')), '');

    if v_whatsapp_message_text is null then
      raise exception 'whatsapp template_content_json missing message_text';
    end if;

    v_content := v_content || jsonb_build_object(
      'provider_contract',
      jsonb_build_object(
        'provider', 'WATI',
        'template_name', v_whatsapp_template_name,
        'param_name', v_whatsapp_param_name
      )
    );
  elsif v_output_type_norm in ('SMS','VOICE') then
    v_sms_voice_message_text := nullif(btrim(coalesce(v_content->>'message_text','')), '');

    if v_sms_voice_message_text is null then
      raise exception '% template_content_json missing message_text', v_output_type_norm;
    end if;
  elsif v_output_type_norm = 'WORD' then
    v_word_body_html := nullif(btrim(coalesce(v_content->>'body_html','')), '');
    v_word_body_text := nullif(btrim(coalesce(v_content->>'body_text','')), '');

    if v_word_body_html is null and v_word_body_text is null then
      raise exception 'word template_content_json requires body_html or body_text';
    end if;
  end if;

  drop table if exists tmp_document_template_selected_input;
  create temporary table tmp_document_template_selected_input(
    ord integer not null,
    field_key text not null
  ) on commit drop;

  insert into tmp_document_template_selected_input(
    ord,
    field_key
  )
  select
    u.ord::integer,
    btrim(u.field_key)
  from unnest(v_selected_raw) with ordinality as u(field_key, ord)
  where nullif(btrim(coalesce(u.field_key, '')), '') is not null;

  drop table if exists tmp_document_template_selected_distinct;
  create temporary table tmp_document_template_selected_distinct(
    ord integer not null,
    field_key text not null,
    primary key (field_key)
  ) on commit drop;

  insert into tmp_document_template_selected_distinct(
    ord,
    field_key
  )
  select
    min(tdtsi.ord) as ord,
    tdtsi.field_key
  from tmp_document_template_selected_input as tdtsi
  group by tdtsi.field_key;

  select greatest(
           coalesce((select count(*) from tmp_document_template_selected_input), 0)
           -
           coalesce((select count(*) from tmp_document_template_selected_distinct), 0),
           0
         )
    into v_duplicate_count;

  drop table if exists tmp_document_template_selection_review;
  create temporary table tmp_document_template_selection_review(
    ord integer not null,
    field_key text not null,
    reject_reason text
  ) on commit drop;

  insert into tmp_document_template_selection_review(
    ord,
    field_key,
    reject_reason
  )
  select
    tdsd.ord,
    tdsd.field_key,
    case
      when mf.id is null then 'field_not_found'
      when lower(coalesce(mf.resolver_spec_json ->> 'stale', 'false')) = 'true' then 'stale'
      when mf.enabled_global is distinct from true then 'disabled_globally'
      when not (v_entity_type_norm = any(coalesce(mf.allowed_entity_types, '{}'::text[]))) then 'not_allowed_for_entity'
      when not exists (
        select 1
        from public.v_mailshot_resolution_graph as rg
        where rg.root_entity_type = v_entity_type_norm
          and rg.source_family = coalesce(
            nullif(mf.resolver_spec_json ->> 'source_family', ''),
            split_part(mf.field_key, '.', 1)
          )
      ) then 'not_resolvable_for_entity'
      else null
    end as reject_reason
  from tmp_document_template_selected_distinct as tdsd
  left join public.mailshot_fields as mf
    on mf.field_key = tdsd.field_key;

  select coalesce(
           array_agg(tdsr.field_key order by tdsr.ord),
           '{}'::text[]
         )
    into v_selected_accepted
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason is null;

  select coalesce(
           array_agg(tdsr.field_key order by tdsr.ord),
           '{}'::text[]
         )
    into v_selected_rejected
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason is not null;

  select count(*) into v_rejected_total
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason is not null;

  select count(*) into v_reason_field_not_found
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason = 'field_not_found';

  select count(*) into v_reason_disabled_globally
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason = 'disabled_globally';

  select count(*) into v_reason_not_allowed_for_entity
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason = 'not_allowed_for_entity';

  select count(*) into v_reason_not_resolvable_for_entity
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason = 'not_resolvable_for_entity';

  select count(*) into v_reason_stale
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason = 'stale';

  select coalesce(
           jsonb_agg(
             jsonb_build_object(
               'field_key', tdsr.field_key,
               'reason', tdsr.reject_reason
             )
             order by tdsr.ord
           ),
           '[]'::jsonb
         )
    into v_rejected_details
  from tmp_document_template_selection_review as tdsr
  where tdsr.reject_reason is not null;

  if v_rejected_total > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      'Some selected fields were rejected because they are not eligible for this entity, are stale, or are disabled.'
    );
  end if;

  if v_reason_field_not_found > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s selected field(s) were rejected because they do not exist in mailshot_fields.', v_reason_field_not_found)
    );
  end if;

  if v_reason_disabled_globally > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s selected field(s) were rejected because they are globally disabled.', v_reason_disabled_globally)
    );
  end if;

  if v_reason_not_allowed_for_entity > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s selected field(s) were rejected because they are not visible for entity_type %s.', v_reason_not_allowed_for_entity, v_entity_type_norm)
    );
  end if;

  if v_reason_not_resolvable_for_entity > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s selected field(s) were rejected because they are not resolvable for entity_type %s.', v_reason_not_resolvable_for_entity, v_entity_type_norm)
    );
  end if;

  if v_reason_stale > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s selected field(s) were rejected because they are marked stale.', v_reason_stale)
    );
  end if;

  if v_duplicate_count > 0 then
    v_warnings := v_warnings || jsonb_build_array(
      format('%s duplicate selected field entrie(s) were removed while preserving first occurrence order.', v_duplicate_count)
    );
  end if;

  if p_template_id is null then
    insert into public.document_templates(
      entity_type,
      output_type,
      filename,
      description,
      email_type,
      selected_field_keys,
      template_content_json,
      created_by,
      created_at_utc,
      updated_at_utc
    )
    values (
      v_entity_type_norm,
      v_output_type_norm,
      v_filename_norm,
      p_description,
      v_email_type_norm,
      v_selected_accepted,
      v_content,
      p_actor_user_id,
      v_now,
      v_now
    )
    returning id into v_id;
  else
    select dt.*
      into v_existing
    from public.document_templates as dt
    where dt.id = p_template_id;

    if not found then
      raise exception 'document_template not found: %', p_template_id;
    end if;

    update public.document_templates as dt
    set
      entity_type = v_entity_type_norm,
      output_type = v_output_type_norm,
      filename = v_filename_norm,
      description = p_description,
      email_type = v_email_type_norm,
      selected_field_keys = v_selected_accepted,
      template_content_json = v_content,
      updated_at_utc = v_now
    where dt.id = p_template_id
    returning dt.id into v_id;
  end if;

  select dt.*
    into v_row
  from public.document_templates as dt
  where dt.id = v_id;

  return jsonb_build_object(
    'ok', true,
    'accepted_selected_field_keys', to_jsonb(v_selected_accepted),
    'rejected_selected_field_keys', to_jsonb(v_selected_rejected),
    'rejected_selected_field_details', v_rejected_details,
    'warnings', v_warnings,
    'template', jsonb_build_object(
      'id', v_row.id::text,
      'entity_type', v_row.entity_type,
      'output_type', v_row.output_type,
      'filename', v_row.filename,
      'description', v_row.description,
      'email_type', v_row.email_type,
      'selected_field_keys', to_jsonb(v_row.selected_field_keys),
      'template_content_json', v_row.template_content_json,
      'created_by', case when v_row.created_by is null then null else v_row.created_by::text end,
      'created_at_utc', v_row.created_at_utc::text,
      'updated_at_utc', v_row.updated_at_utc::text
    )
  );
end;
$function$;

-- email_outbox_claim_ready_batch(integer,text,integer)
CREATE OR REPLACE FUNCTION public.email_outbox_claim_ready_batch(p_limit integer, p_attempt_lease_token text, p_lease_minutes integer DEFAULT 5)
 RETURNS SETOF mail_outbox
 LANGUAGE plpgsql
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare
  v_now timestamptz:=now();
  v_limit integer:=greatest(0,least(coalesce(p_limit,0),500));
  v_lease integer:=greatest(1,least(coalesce(p_lease_minutes,5),30));
begin
  if coalesce(btrim(p_attempt_lease_token),'')='' then raise exception 'attempt_lease_token is required'; end if;
  if v_limit=0 then return; end if;

  return query
  with picked as materialized (
    select mo.id
    from public.mail_outbox mo
    where mo.status='QUEUED' and mo.sent_at is null and mo.delivered_at is null and mo.read_at is null
      and coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc)<=v_now
      and (mo.attempt_lease_token is null or mo.attempt_lease_expires_at_utc is null
        or mo.attempt_lease_expires_at_utc<=v_now)
      and (
        not (
          nullif(btrim(coalesce(mo.payment_scope_json->>'candidate_workflow_id','')),'') is not null
          or mo.payment_scope_json ? 'candidate_workflow_generation'
          or mo.payment_scope_json ? 'paper_return_manifest_sha256'
          or mo.payment_scope_json ? 'candidate_paper_pack_ready'
          or mo.payment_scope_json ? 'candidate_complete_pack_storage_key'
          or upper(coalesce(mo.payment_scope_json->>'mail_hold_reason',''))
            ='CANDIDATE_PAPER_PACK_PENDING'
          or upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
        )
        or (
          coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.payment_scope_json->>'paper_return_manifest_sha256',''))
            ~ '^[0-9a-f]{64}$'
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in('true','t','1','yes')
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in('false','f','0','no')
          and lower(coalesce(mo.payment_scope_json->>'mail_held_until_pdf_rendered','true'))
            in('false','f','0','no')
          and nullif(btrim(coalesce(mo.payment_scope_json->>'mail_hold_reason','')),'') is null
          and jsonb_typeof(mo.attachments)='array'
          and jsonb_array_length(mo.attachments)=1
          and nullif(btrim(coalesce(mo.attachments->0->>'r2_key','')),'') is not null
          and lower(coalesce(mo.attachments->0->>'sha256','')) ~ '^[0-9a-f]{64}$'
          and coalesce(mo.attachments->0->>'size_bytes','') ~ '^[1-9][0-9]{0,18}$'
          and coalesce(mo.attachments->0->>'page_count','') ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.attachments->0->>'content_type',''))='application/pdf'
          and mo.attachments->0->>'r2_key'
            =mo.payment_scope_json->>'candidate_complete_pack_storage_key'
          and lower(mo.attachments->0->>'sha256')
            =lower(mo.payment_scope_json->>'candidate_complete_pack_sha256')
          and mo.attachments->0->>'size_bytes'
            =mo.payment_scope_json->>'candidate_complete_pack_size_bytes'
          and mo.attachments->0->>'page_count'
            =mo.payment_scope_json->>'candidate_complete_pack_page_count'
          and lower(coalesce(mo.payment_scope_json->>'candidate_complete_pack_media_type',''))
            ='application/pdf'
          and mo.attachments->0->>'candidate_workflow_id'
            =mo.payment_scope_json->>'candidate_workflow_id'
          and mo.attachments->0->>'candidate_workflow_generation'
            =mo.payment_scope_json->>'candidate_workflow_generation'
          and lower(mo.attachments->0->>'paper_return_manifest_sha256')
            =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
          and exists(
            select 1
            from public.candidate_submission_workflows workflow
            where workflow.id=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                then (mo.payment_scope_json->>'candidate_workflow_id')::uuid
                else null::uuid end
              and workflow.generation=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
                  ~ '^[1-9][0-9]{0,8}$'
                then (mo.payment_scope_json->>'candidate_workflow_generation')::integer
                else null::integer end
              and workflow.route='PAPER'
              and workflow.state='AWAITING_PAPER_RETURN'
              and encode(workflow.paper_return_manifest_sha256,'hex')
                =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
              and coalesce(workflow.target_timesheet_id,workflow.anchor_timesheet_id)=mo.context_id
          )
        )
        or (
          upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
          and upper(coalesce(mo.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL')
          and lower(coalesce(mo.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no')
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and mo.context_kind='CANDIDATE_WORKFLOW'
          and mo.context_id=(mo.payment_scope_json->>'candidate_manager_workflow_id')::uuid
          and exists (
            select 1
            from public.candidate_approval_requests request_row
            join public.candidate_submission_workflows workflow
              on workflow.id=request_row.workflow_id
            where request_row.id=(mo.payment_scope_json->>'candidate_approval_request_id')::uuid
              and request_row.workflow_id=mo.context_id
              and request_row.workflow_generation=
                    (mo.payment_scope_json->>'candidate_manager_workflow_generation')::integer
              and request_row.request_generation=
                    (mo.payment_scope_json->>'candidate_approval_request_generation')::integer
              and request_row.method='EMAIL'
              and request_row.manager_email_normalized=mo."to"
              and (
                (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind')
                    in ('INITIAL','REMINDER','RENEWAL')
                  and request_row.state='PENDING'
                  and request_row.expires_at_utc>v_now
                  and workflow.generation=request_row.workflow_generation
                  and workflow.route='EMAIL'
                  and workflow.state='AWAITING_MANAGER_APPROVAL'
                  and workflow.review_manifest_sha256=request_row.review_manifest_sha256
                )
                or (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind')='WITHDRAWAL'
                  and request_row.state in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED')
                )
              )
          )
        )
      )
      and (
        upper(coalesce(mo.type,''))<>'INVOICE'
        or (
          mo.attachments_ready=true and mo.waiting_invoice_operation_id is null
          and jsonb_typeof(mo.attachments)='array' and jsonb_array_length(mo.attachments)>0
          and not exists (
            select 1 from jsonb_array_elements(mo.attachments) a
            left join lateral (
              select
                case when coalesce(a->>'document_version_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'document_version_id')::uuid end document_version_id,
                case when coalesce(a->>'invoice_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'invoice_id')::uuid end invoice_id,
                case when coalesce(a->>'size_bytes','') ~ '^[0-9]{1,18}$'
                  then(a->>'size_bytes')::bigint end size_bytes,
                case when coalesce(a->>'page_count','') ~ '^[0-9]{1,9}$'
                  then(a->>'page_count')::integer end page_count,
                upper(coalesce(nullif(a->>'delivery_mode',''),
                  'ATTACHMENT')) delivery_mode,
                lower(coalesce(a->>'secure_link_required','false'))
                  in('true','t','1','yes') secure_link_required
            ) parsed on true
            left join public.invoice_document_versions v
              on v.id=parsed.document_version_id
            left join public.invoices i on i.id=parsed.invoice_id
            where parsed.document_version_id is null or parsed.invoice_id is null
              or parsed.delivery_mode not in('ATTACHMENT','SECURE_LINK')
              or nullif(a->>'sha256','') is null
              or nullif(a->>'filename','') is null
              or coalesce(parsed.size_bytes,0)<=0 or coalesce(parsed.page_count,0)<=0
              or v.id is null or v.status<>'READY' or v.superseded_at_utc is not null
              or v.purpose<>'FINAL_ISSUE' or v.entity_type<>'INVOICE'
              or v.entity_id is distinct from parsed.invoice_id
              or i.id is null or i.status<>'ISSUED'
              or i.issued_document_version_id is distinct from v.id
              or v.sha256 is distinct from a->>'sha256'
              or v.size_bytes is distinct from parsed.size_bytes
              or v.page_count is distinct from parsed.page_count
              or(parsed.delivery_mode='ATTACHMENT' and(
                nullif(a->>'r2_key','') is null
                or nullif(a->>'mime_type','') is null
                or v.r2_key is distinct from a->>'r2_key'))
              or(parsed.delivery_mode='SECURE_LINK' and(
                parsed.secure_link_required is not true
                or nullif(a->>'r2_key','') is not null))
          )
        )
      )
    order by coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc),
      mo.created_at_utc,mo.id
    for update skip locked limit v_limit
  ),
  updated as (
    update public.mail_outbox mo set attempt_lease_token=p_attempt_lease_token,
      attempt_leased_at_utc=v_now,attempt_lease_expires_at_utc=v_now+make_interval(mins=>v_lease)
    from picked p where mo.id=p.id returning mo.*
  )
  select u.* from updated u
  order by coalesce(u.next_attempt_at_utc,u.scheduled_for_utc,u.created_at_utc),u.created_at_utc,u.id;
end;
$function$;

-- enqueue_ts_financials_priority(uuid[],ts_fin_reason_enum)
CREATE OR REPLACE FUNCTION public.enqueue_ts_financials_priority(_timesheet_ids uuid[], _reason ts_fin_reason_enum)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_count int := 0;
  v_priority_ts timestamptz := now() - interval '100 years';
begin
  if _timesheet_ids is null or array_length(_timesheet_ids, 1) is null then
    return 0;
  end if;

  if cardinality(_timesheet_ids) > 5000 then
    raise exception 'TSFIN_PRIORITY_ENQUEUE_TARGET_LIMIT_EXCEEDED'
      using errcode = '22023';
  end if;

  with requested as (
    select distinct t as timesheet_id
    from unnest(_timesheet_ids) as t
    where t is not null
  ), expanded as (
    select r.timesheet_id
    from requested r

    union

    -- A TSFIN write for an import-authoritative correction is atomic at the
    -- correction-unit boundary. If either reversal/replacement member is
    -- requested, enqueue every current member of that same unit. This does
    -- not alter either leg's economics; it completes the guarded batch.
    select partner.timesheet_id
    from requested r
    join public.timesheets seed
      on seed.timesheet_id = r.timesheet_id
     and seed.is_current = true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin, ''))) in (
       'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id = seed.correction_id
     and partner.is_current = true
     and upper(btrim(coalesce(partner.adjustment_origin, ''))) in (
       'IMPORT_CORRECTION', 'IMPORT_CANCELLATION',
       'HEALTHROSTER_CHANGED_HOURS', 'NHSP_CHANGED_HOURS',
       'HEALTHROSTER_CANCELLATION', 'NHSP_CANCELLATION'
     )
  ), ids as (
    select distinct timesheet_id from expanded
  )

  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select timesheet_id, _reason, 0, null, null, v_priority_ts
  from ids
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = v_priority_ts;

  get diagnostics v_count = row_count;
  return v_count;
end;
$function$;

-- enqueue_ts_financials(uuid,ts_fin_reason_enum)
CREATE OR REPLACE FUNCTION public.enqueue_ts_financials(_timesheet_id uuid, _reason ts_fin_reason_enum)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  INSERT INTO public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  VALUES (_timesheet_id, _reason, 0, NULL, NULL, now())
  ON CONFLICT (timesheet_id, reason)
  DO UPDATE SET
      attempt_count   = 0,
      next_attempt_at = NULL,
      last_error      = NULL,
      created_at      = now();
END;
$function$;

-- enqueue_tsfin_for_authorised_range(date,date,ts_fin_reason_enum,integer)
CREATE OR REPLACE FUNCTION public.enqueue_tsfin_for_authorised_range(p_from date, p_to date DEFAULT NULL::date, p_reason ts_fin_reason_enum DEFAULT 'CONTEXT_CHANGED'::ts_fin_reason_enum, p_limit integer DEFAULT 20000)
 RETURNS integer
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_cnt integer := 0;
  v_now timestamptz := now();
begin
  with picked as (
    select ts.timesheet_id
    from public.timesheets ts
    join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null
      and ts.authorised_at_server is not null
      and (ts.authorised_at_server at time zone 'Europe/London')::date >= p_from
      and (p_to is null or (ts.authorised_at_server at time zone 'Europe/London')::date <= p_to)

      -- ✅ PAYE only (pay_method is a text field in timesheets_financials)
      and tf.pay_method = 'PAYE'

      -- safety: don't enqueue if current TSFIN is locked/paid
      and tf.locked_by_invoice_id is null
      and tf.paid_at_utc is null

    limit p_limit
  )
  insert into public.ts_financials_outbox
    (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select
    p.timesheet_id, p_reason, 0, null, null, v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  get diagnostics v_cnt = row_count;
  return v_cnt;
end;
$function$;

-- enqueue_tsfin_for_hospital_norm(text,ts_fin_reason_enum,boolean,integer)
CREATE OR REPLACE FUNCTION public.enqueue_tsfin_for_hospital_norm(p_hospital_norm text, p_reason ts_fin_reason_enum DEFAULT 'CONTEXT_CHANGED'::ts_fin_reason_enum, p_priority boolean DEFAULT true, p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_norm text := lower(btrim(coalesce(p_hospital_norm, '')));
  v_now  timestamptz := now();
  v_cnt  integer := 0;
begin
  if v_norm = '' then
    return 0;
  end if;

  with picked as (
    select ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null
      and ts.hospital_norm = v_norm
    limit p_limit
  )
  select
    case
      when p_priority then public.enqueue_ts_financials_priority(array_agg(timesheet_id), p_reason)
      else null
    end
  into v_cnt
  from picked;

  if p_priority then
    return coalesce(v_cnt, 0);
  end if;

  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select p.timesheet_id, p_reason, 0, null, null, v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  get diagnostics v_cnt = row_count;
  return v_cnt;
end;
$function$;

-- enqueue_tsfin_for_occ_key(text,ts_fin_reason_enum,boolean,integer)
CREATE OR REPLACE FUNCTION public.enqueue_tsfin_for_occ_key(p_occ_key_norm text, p_reason ts_fin_reason_enum DEFAULT 'CONTEXT_CHANGED'::ts_fin_reason_enum, p_priority boolean DEFAULT true, p_limit integer DEFAULT 500)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
  v_norm text := lower(btrim(coalesce(p_occ_key_norm, '')));
  v_now  timestamptz := now();
  v_cnt  integer := 0;
begin
  if v_norm = '' then
    return 0;
  end if;

  with picked as (
    select ts.timesheet_id
    from public.timesheets ts
    left join public.timesheets_financials tf
      on tf.timesheet_id = ts.timesheet_id
     and tf.is_current = true
    where ts.is_current = true
      and ts.revoked_at is null
      and ts.occupant_key_norm = v_norm
    limit p_limit
  )
  select
    case
      when p_priority then public.enqueue_ts_financials_priority(array_agg(timesheet_id), p_reason)
      else null
    end
  into v_cnt
  from picked;

  if p_priority then
    return coalesce(v_cnt, 0);
  end if;

  insert into public.ts_financials_outbox (timesheet_id, reason, attempt_count, next_attempt_at, last_error, created_at)
  select p.timesheet_id, p_reason, 0, null, null, v_now
  from picked p
  on conflict (timesheet_id, reason)
  do update set
    attempt_count   = 0,
    next_attempt_at = null,
    last_error      = null,
    created_at      = excluded.created_at;

  get diagnostics v_cnt = row_count;
  return v_cnt;
end;
$function$;

-- expense_carrier_resolve_or_create_atomic_v1(uuid,text,uuid,text,text,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.expense_carrier_resolve_or_create_atomic_v1(p_candidate_id uuid, p_environment text, p_anchor_timesheet_id uuid, p_expected_row_signature text, p_idempotency_key text, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_placement jsonb;
  v_signature jsonb;
  v_new_week public.contract_weeks%rowtype;
  v_next_seq integer;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or p_anchor_timesheet_id is null or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null then
    raise exception 'EXPENSE_CARRIER_PAYLOAD_INVALID' using errcode='22023';
  end if;
  select * into v_anchor_week from public.contract_weeks where timesheet_id=p_anchor_timesheet_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id for update;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  if nullif(btrim(coalesce(p_expected_row_signature,'')),'') is not null then
    v_signature:=public.timesheet_lifecycle_guard_signature_v1(p_anchor_timesheet_id,v_anchor_week.id,false);
    if coalesce(v_signature->>'row_signature',v_signature->>'backend_row_signature','')<>p_expected_row_signature then
      raise exception 'ROW_SIGNATURE_MISMATCH'
        using errcode='40001',detail=jsonb_build_object('code','ROW_SIGNATURE_MISMATCH')::text;
    end if;
  end if;
  perform pg_advisory_xact_lock(hashtext(v_contract.id::text||'|'||v_anchor_week.week_ending_date::text||'|EXPENSE_CARRIER'));
  perform 1 from public.contract_weeks cw
  where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
  order by cw.additional_seq,cw.id for update;
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='BLOCKED' then
    raise exception '%',v_placement->>'reason_code' using errcode='55000',detail=v_placement::text;
  elsif v_placement->>'placement' in ('SAME_RECORD','REUSE_CARRIER') then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  select coalesce(max(additional_seq),0)+1 into v_next_seq from public.contract_weeks
  where contract_id=v_contract.id and week_ending_date=v_anchor_week.week_ending_date;
  insert into public.contract_weeks(
    contract_id,week_ending_date,additional_seq,status,submission_mode_snapshot,
    day_entries_json,totals_json,planned_schedule_json,is_adjustment,
    enforce_day_partition,allowed_days_mask,split_boundary_date,split_group_key,
    created_at,updated_at
  ) values (
    v_contract.id,v_anchor_week.week_ending_date,v_next_seq,'OPEN','MANUAL',
    '[]'::jsonb,
    jsonb_build_object(
      'hours',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),
      'additional_units_week','{}'::jsonb,
      'additional_units_per_day','{}'::jsonb,
      'expenses_draft',jsonb_build_object(
        'mileage_units',0,'travel_pay',0,'travel_charge',0,
        'accommodation_pay',0,'accommodation_charge',0,
        'other_pay',0,'other_charge',0,'note',''
      )
    ),
    '[]'::jsonb,true,
    v_anchor_week.enforce_day_partition,v_anchor_week.allowed_days_mask,
    v_anchor_week.split_boundary_date,v_anchor_week.split_group_key,
    p_now_utc,p_now_utc
  ) returning * into v_new_week;
  perform private._candidate_audit_v1('contract_week',v_new_week.id::text,'CANDIDATE_EXPENSE_CARRIER_CREATED',null,
    jsonb_build_object('contract_id',v_contract.id,'week_ending_date',v_new_week.week_ending_date,'additional_seq',v_new_week.additional_seq),
    null,null,p_idempotency_key,p_now_utc);
  return jsonb_build_object(
    'ok',true,'placement','CREATE_CARRIER','reason_code','CARRIER_CREATED',
    'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',null,'target_contract_week_id',v_new_week.id,
    'target_record_role','FLEXIBLE','idempotent_replay',false,'idempotency_key',p_idempotency_key
  );
exception when unique_violation then
  v_placement:=public.expense_placement_resolve_v1(p_candidate_id,v_environment,p_anchor_timesheet_id,v_anchor_week.id,'{}'::jsonb,p_now_utc);
  if v_placement->>'placement'='REUSE_CARRIER' then
    return v_placement||jsonb_build_object('idempotent_replay',true,'idempotency_key',p_idempotency_key);
  end if;
  raise;
end;
$function$;

-- expense_placement_resolve_v1(uuid,text,uuid,uuid,jsonb,timestamp with time zone)
CREATE OR REPLACE FUNCTION public.expense_placement_resolve_v1(p_candidate_id uuid, p_environment text, p_anchor_timesheet_id uuid, p_contract_week_id uuid DEFAULT NULL::uuid, p_proposed_claim jsonb DEFAULT '{}'::jsonb, p_now_utc timestamp with time zone DEFAULT now())
 RETURNS jsonb
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'public', 'private', 'pg_temp'
AS $function$
declare
  v_environment text;
  v_anchor_week public.contract_weeks%rowtype;
  v_contract public.contracts%rowtype;
  v_anchor_capabilities jsonb;
  v_positive_work boolean:=false;
  v_candidate_count integer:=0;
  v_candidate_week_id uuid;
  v_candidate_timesheet_id uuid;
  v_candidate_role text;
  v_result text;
  v_reason text;
begin
  v_environment:=private._candidate_assert_environment(p_environment);
  perform private._candidate_require_feature_v1(v_environment,'candidate_expense_atomic_placement');
  if p_candidate_id is null or (p_anchor_timesheet_id is null and p_contract_week_id is null) then
    raise exception 'EXPENSE_PLACEMENT_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  if p_contract_week_id is not null then
    select * into v_anchor_week from public.contract_weeks where id=p_contract_week_id;
  else
    select * into v_anchor_week from public.contract_weeks
    where timesheet_id=p_anchor_timesheet_id order by updated_at desc,id desc limit 1;
  end if;
  if not found then raise exception 'EXPENSE_PLACEMENT_ANCHOR_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_contract from public.contracts where id=v_anchor_week.contract_id and candidate_id=p_candidate_id;
  if not found then raise exception 'EXPENSE_PLACEMENT_CANDIDATE_MISMATCH' using errcode='28000'; end if;
  v_anchor_capabilities:=private._candidate_record_capabilities_v1(
    coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),v_anchor_week.id,coalesce(p_proposed_claim,'{}'::jsonb)
  );
  if not coalesce((v_anchor_capabilities->>'candidate_expenses_allowed')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','CANDIDATE_RECORD_VIEW_ONLY',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  select exists(
    select 1
    from public.contract_weeks cw
    join public.timesheets t on t.timesheet_id=cw.timesheet_id and t.is_current=true and t.archived_at_utc is null
    join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current=true
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and (
        coalesce(tf.total_hours,0)>0
        or private._candidate_json_numeric_sum(coalesce(tf.additional_units_json,'{}'::jsonb))>0
        or private._candidate_json_numeric_sum(coalesce(t.additional_units_week,'{}'::jsonb))
          +private._candidate_json_numeric_sum(coalesce(t.additional_units_per_day,'{}'::jsonb))>0
      )
  ) into v_positive_work;
  if not v_positive_work then
    return jsonb_build_object(
      'ok',true,'placement','BLOCKED','reason_code','NO_POSITIVE_WORKED_TIME',
      'anchor_timesheet_id',p_anchor_timesheet_id,'anchor_contract_week_id',v_anchor_week.id,
      'capabilities',v_anchor_capabilities
    );
  end if;

  if coalesce((v_anchor_capabilities->>'effective_separation')::boolean,false)=false
     and v_anchor_capabilities->>'record_role' in ('COMBINED_ALLOWED','EXPENSE_ONLY','FLEXIBLE')
     and coalesce((v_anchor_capabilities->>'protected')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'candidate_mutation_locked')::boolean,false)=false
     and coalesce((v_anchor_capabilities->>'can_edit_expenses')::boolean,false) then
    return jsonb_build_object(
      'ok',true,'placement','SAME_RECORD','reason_code','COMBINED_ALLOWED',
      'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
      'target_contract_week_id',v_anchor_week.id,'capabilities',v_anchor_capabilities
    );
  end if;

  with candidate_carriers as (
    select cw.id as contract_week_id,cw.timesheet_id,
      private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
    from public.contract_weeks cw
    left join public.timesheets t on t.timesheet_id=cw.timesheet_id
    where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
      and cw.additional_seq>0
      and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
      and not exists(
        select 1 from public.candidate_submission_workflows w
        where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
          and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
      )
  ), safe as (
    select * from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
  )
  select count(*)::integer into v_candidate_count from safe;

  if v_candidate_count=1 then
    with candidate_carriers as (
      select cw.id as contract_week_id,cw.timesheet_id,
        private._candidate_record_capabilities_v1(cw.timesheet_id,cw.id,coalesce(p_proposed_claim,'{}'::jsonb)) as capabilities
      from public.contract_weeks cw
      left join public.timesheets t on t.timesheet_id=cw.timesheet_id
      where cw.contract_id=v_contract.id and cw.week_ending_date=v_anchor_week.week_ending_date
        and cw.additional_seq>0
        and (t.timesheet_id is null or (t.is_current=true and t.archived_at_utc is null))
        and not exists(
          select 1 from public.candidate_submission_workflows w
          where w.contract_week_id=cw.id and w.candidate_id<>p_candidate_id
            and w.state in ('CREATED','WORKER_SUBMITTED','AWAITING_MANAGER_APPROVAL','MANAGER_APPROVED','AWAITING_PAPER_RETURN','RECEIVED')
        )
    )
    select contract_week_id,timesheet_id,capabilities->>'record_role'
    into v_candidate_week_id,v_candidate_timesheet_id,v_candidate_role
    from candidate_carriers
    where capabilities->>'record_role' in ('EXPENSE_ONLY','FLEXIBLE')
      and coalesce((capabilities->>'protected')::boolean,false)=false
      and coalesce((capabilities->>'candidate_mutation_locked')::boolean,false)=false
      and coalesce((capabilities->>'import_authoritative')::boolean,false)=false
    order by contract_week_id
    limit 1;
  end if;

  if v_candidate_count>1 then v_result:='BLOCKED';v_reason:='EXPENSE_CARRIER_AMBIGUOUS';
  elsif v_candidate_count=1 then v_result:='REUSE_CARRIER';v_reason:='SAFE_EXISTING_CARRIER';
  else v_result:='CREATE_CARRIER';v_reason:='NO_SAFE_CARRIER'; end if;

  return jsonb_build_object(
    'ok',true,'placement',v_result,'reason_code',v_reason,
    'anchor_timesheet_id',coalesce(p_anchor_timesheet_id,v_anchor_week.timesheet_id),
    'anchor_contract_week_id',v_anchor_week.id,
    'target_timesheet_id',v_candidate_timesheet_id,
    'target_contract_week_id',v_candidate_week_id,
    'target_record_role',v_candidate_role,
    'capabilities',v_anchor_capabilities
  );
end;
$function$;

-- hr_autoprocess_apply_phase1(uuid,text[],text[],text[])
CREATE OR REPLACE FUNCTION public.hr_autoprocess_apply_phase1(import_id uuid, selected_group_ids text[] DEFAULT NULL::text[], p_skip_external_row_keys text[] DEFAULT NULL::text[], p_force_overwrite_external_row_keys text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
declare
  v_created           int := 0;
  v_updated           int := 0;
  v_mapped_candidates int := 0;

  -- ✅ NEW: count of rows we refused to touch because the matched shift is invoice/paid locked
  v_skipped_locked    int := 0;
begin
  ----------------------------------------------------------------
  -- Sanity: import must exist and be HEALTHROSTER
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = hr_autoprocess_apply_phase1.import_id
    and hi.source_system = 'HEALTHROSTER'::public.hr_source_enum;

  if not found then
    raise exception 'hr_autoprocess_apply_phase1: import % not found or not HEALTHROSTER', import_id;
  end if;

  with src as (
    select
      r.id          as hr_row_id,
      r.external_row_key,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,

      -- ✅ Break minutes priority:
      --  1) payload actual_break_mins / actual_break_minutes
      --  2) payload break_mins / break_minutes
      --  3) 0
      greatest(
        0,
        coalesce(
          case
            when (r.payload_json ? 'actual_break_mins')
             and nullif(btrim(coalesce(r.payload_json ->> 'actual_break_mins','')), '') is not null
             and (r.payload_json ->> 'actual_break_mins') ~ '^[0-9]+$'
            then (r.payload_json ->> 'actual_break_mins')::int
            else null
          end,
          case
            when (r.payload_json ? 'actual_break_minutes')
             and nullif(btrim(coalesce(r.payload_json ->> 'actual_break_minutes','')), '') is not null
             and (r.payload_json ->> 'actual_break_minutes') ~ '^[0-9]+$'
            then (r.payload_json ->> 'actual_break_minutes')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json ->> 'break_mins','')), '') is not null
             and (r.payload_json ->> 'break_mins') ~ '^[0-9]+$'
            then (r.payload_json ->> 'break_mins')::int
            else null
          end,
          case
            when nullif(btrim(coalesce(r.payload_json ->> 'break_minutes','')), '') is not null
             and (r.payload_json ->> 'break_minutes') ~ '^[0-9]+$'
            then (r.payload_json ->> 'break_minutes')::int
            else null
          end,
          0
        )
      ) as break_mins,

      coalesce(
        nullif((r.payload_json ->> 'finalized_date'), ''),
        nullif((r.payload_json ->> 'finalised_date'), '')
      ) as finalized_raw,

      -- ✅ Request id priority (trimmed + blank->NULL):
      --  1) hr_rows.hr_request_id
      --  2) payload_json.request_id
      coalesce(
        nullif(btrim(coalesce(r.hr_request_id,'')), ''),
        nullif(btrim(coalesce(r.payload_json ->> 'request_id','')), '')
      ) as request_id
    from public.hr_rows r
    join public.hr_imports hi
      on hi.id = r.import_id
    where r.import_id = hr_autoprocess_apply_phase1.import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),

  normed as (
    select
      s.hr_row_id,
      s.client_id,
      s.work_date,
      s.staff_name,
      lower(trim(s.staff_name)) as staff_norm,
      s.ward,
      lower(trim(s.ward))       as ward_norm,
      s.start_utc,
      s.end_utc,
      s.break_mins,

      -- ✅ ensure request_id is always trimmed and blank->NULL
      nullif(btrim(coalesce(s.request_id,'')), '') as request_id,

      case
        when coalesce(trim(s.finalized_raw), '') = '' then 'NO_FINALISED_DATE'
        else null
      end as held_back_reason,

      coalesce(
        s.external_row_key,
        case
          when s.work_date is null or s.start_utc is null or s.end_utc is null then null
          else array_to_string(ARRAY[
            regexp_replace(trim(s.work_date::text),               '\|', ' ', 'g'),
            regexp_replace(coalesce(lower(trim(s.staff_name)),''), '\|',' ','g'),
            regexp_replace(coalesce(lower(trim(s.ward)),''),       '\|',' ','g'),
            regexp_replace(coalesce(s.client_id::text,''),         '\|',' ','g'),
            regexp_replace(coalesce(trim(s.request_id),''),        '\|',' ','g')
          ], '|')
        end
      ) as external_row_key
    from src s
  ),

  -- Enforce row-level selection:
  -- - Unticked rows must be in SKIP list → not present here
  -- - Ticked rows must be in FORCE list → only rows in FORCE are processed when FORCE list provided
  normed_filtered as (
    select *
    from normed n
    where
      (
        p_skip_external_row_keys is null
        or array_length(p_skip_external_row_keys, 1) is null
        or n.external_row_key is null
        or n.external_row_key <> all(p_skip_external_row_keys)
      )
      and (
        p_force_overwrite_external_row_keys is null
        or array_length(p_force_overwrite_external_row_keys, 1) is null
        or (n.external_row_key is not null and n.external_row_key = any(p_force_overwrite_external_row_keys))
      )
  ),

  resolved_base as (
    select
      n.*,
      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id
    from normed_filtered n

    cross join lateral (
      select
        nullif(lower(trim(coalesce(n.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(n.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2
    ) nx

    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (nx.staff_lc    is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_lc]::text[]))
          or
          (nx.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (nx.staff_lc    is not null and hm.hr_name_norm = nx.staff_lc)
          or
          (nx.staff_norm2 is not null and hm.hr_name_norm = nx.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    left join lateral (
      with matches as (
        select c.id as candidate_id
        from public.candidates c
        where c.active = true
          and nx.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(candidate_id order by candidate_id::text))[1]
        end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  ),

  -- ✅ NEW: if Request ID changes but the shift overlaps the existing one, reuse the existing external_row_key
  match_existing as (
    select
      x.hr_row_id,
      x.external_row_key as existing_external_row_key
    from (
      select
        rb.hr_row_id,
        s2.external_row_key,
        row_number() over (
          partition by rb.hr_row_id
          order by s2.updated_at desc nulls last, s2.created_at desc nulls last, s2.id desc
        ) as rn
      from resolved_base rb
      join public.nhsp_shifts s2
        on s2.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and s2.client_id = rb.client_id
       and rb.candidate_id is not null
       and s2.candidate_id = rb.candidate_id
       and s2.work_date = rb.work_date
       and rb.start_utc is not null
       and rb.end_utc is not null
       and s2.start_utc is not null
       and s2.end_utc is not null
       and (least(s2.end_utc, rb.end_utc) - greatest(s2.start_utc, rb.start_utc)) >= interval '1 minute'
    ) as x
    where x.rn = 1
  ),

  resolved as (
    select
      rb.*,
      coalesce(me.existing_external_row_key, rb.external_row_key) as external_row_key_eff
    from resolved_base rb
    left join match_existing me
      on me.hr_row_id = rb.hr_row_id
  ),

  fin_current as (
    select distinct on (tf.timesheet_id)
      tf.timesheet_id,
      tf.locked_by_invoice_id,
      tf.paid_at_utc,
      tf.invoice_breakdown_json
    from public.timesheets_financials tf
    where tf.is_current = true
    order by tf.timesheet_id, tf.created_at desc
  ),

  ext_update as (
    update public.hr_rows r
    set external_row_key = res.external_row_key_eff
    from resolved res
    where r.id = res.hr_row_id
      and res.external_row_key_eff is not null
      and r.external_row_key is distinct from res.external_row_key_eff
    returning 1
  ),

  ins as (
    insert into public.nhsp_shifts (
      external_row_key,
      latest_import_id,
      source_system,
      work_date,
      ward,
      start_utc,
      end_utc,
      break_mins,
      pay_minutes,
      client_id,
      hr_request_id,
      ref_num,
      held_back_reason,
      candidate_id,
      created_at,
      updated_at
    )
    select
      r.external_row_key_eff,
      hr_autoprocess_apply_phase1.import_id,
      'HEALTHROSTER'::public.hr_source_enum,
      r.work_date,
      nullif(r.ward, ''),
      r.start_utc,
      r.end_utc,
      coalesce(r.break_mins, 0),
      greatest(
        0,
        (extract(epoch from (r.end_utc - r.start_utc)) / 60)::int
        - coalesce(r.break_mins, 0)
      ) as pay_minutes,
      r.client_id,
      r.request_id,
      r.request_id,
      r.held_back_reason,
      r.candidate_id,
      now(),
      now()
    from resolved r
    where r.external_row_key_eff is not null
      and not exists (
        select 1
        from public.nhsp_shifts s
        where s.external_row_key = r.external_row_key_eff
      )
    returning
      (candidate_id is not null) as mapped_candidate
  ),

  upd_src as (
    select
      s.external_row_key,
      s.id as nhsp_shift_id,
      s.invoice_id as shift_invoice_id,
      s.timesheet_id,

      s.candidate_id as old_candidate_id,
      s.client_id    as old_client_id,

      r.work_date,
      r.ward,
      r.start_utc,
      r.end_utc,
      r.break_mins,
      r.request_id,
      r.held_back_reason,
      r.client_id     as new_client_id,
      r.candidate_id  as new_candidate_id,

      fc.locked_by_invoice_id,
      fc.paid_at_utc,
      fc.invoice_breakdown_json,

      (
        fc.invoice_breakdown_json is not null
        and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
        and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
        and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
        and exists (
          select 1
          from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg(seg_obj)
          where nullif(btrim(coalesce(seg.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
            and nullif(btrim(coalesce(seg.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
        )
      ) as is_segment_locked,

      (
        s.invoice_id is not null
        or fc.locked_by_invoice_id is not null
        or fc.paid_at_utc is not null
        or (
          fc.invoice_breakdown_json is not null
          and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
          and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
          and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
          and exists (
            select 1
            from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg2(seg_obj)
            where nullif(btrim(coalesce(seg2.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
              and nullif(btrim(coalesce(seg2.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
          )
        )
      ) as is_invoice_locked,

      (
        not (
          s.invoice_id is not null
          or fc.locked_by_invoice_id is not null
          or fc.paid_at_utc is not null
          or (
            fc.invoice_breakdown_json is not null
            and jsonb_typeof(fc.invoice_breakdown_json) = 'object'
            and upper(coalesce(fc.invoice_breakdown_json->>'mode','')) = 'SEGMENTS'
            and jsonb_typeof(fc.invoice_breakdown_json->'segments') = 'array'
            and exists (
              select 1
              from jsonb_array_elements(fc.invoice_breakdown_json->'segments') as seg3(seg_obj)
              where nullif(btrim(coalesce(seg3.seg_obj->>'nhsp_shift_id','')), '') = s.id::text
                and nullif(btrim(coalesce(seg3.seg_obj->>'invoice_locked_invoice_id','')), '') is not null
            )
          )
        )
      ) as safe_to_overwrite

    from public.nhsp_shifts s
    join resolved r
      on r.external_row_key_eff = s.external_row_key
    left join fin_current fc
      on fc.timesheet_id = s.timesheet_id
    where s.source_system = 'HEALTHROSTER'::public.hr_source_enum
  ),

  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = hr_autoprocess_apply_phase1.import_id,
      source_system    = 'HEALTHROSTER'::public.hr_source_enum,

      work_date        = u.work_date,
      ward             = nullif(u.ward, ''),

      -- ✅ Do not clear request/ref if the new row has no request_id (preserve existing truth)
      hr_request_id    = case when u.request_id is not null then u.request_id else s.hr_request_id end,
      ref_num          = case when u.request_id is not null then u.request_id else s.ref_num end,

      held_back_reason = u.held_back_reason,
      updated_at       = now(),

      cancelled_at_utc = null,
      cancelled_by_import_id = null,
      cancelled_reason = null,

      start_utc        = u.start_utc,
      end_utc          = u.end_utc,
      break_mins       = coalesce(u.break_mins, 0),
      pay_minutes      = greatest(
                           0,
                           (extract(epoch from (u.end_utc - u.start_utc)) / 60)::int
                           - coalesce(u.break_mins, 0)
                         ),

      client_id        = case
                           when u.new_client_id is not null then u.new_client_id
                           else s.client_id
                         end,

      candidate_id     = case
                           when u.new_candidate_id is not null then u.new_candidate_id
                           else s.candidate_id
                         end
    from upd_src u
    where s.external_row_key = u.external_row_key
      and u.safe_to_overwrite is true
    returning
      (u.old_candidate_id is null and u.new_candidate_id is not null and u.safe_to_overwrite) as mapped_candidate
  )

  select
    coalesce((select count(*) from ins), 0),
    coalesce((select count(*) from upd), 0),
    coalesce((select count(*) from ins where mapped_candidate), 0)
      + coalesce((select count(*) from upd where mapped_candidate), 0),
    coalesce((select count(*) from upd_src where is_invoice_locked is true), 0)
  into
    v_created,
    v_updated,
    v_mapped_candidates,
    v_skipped_locked;

  return jsonb_build_object(
    'import_id',             import_id,
    'shifts_created',        v_created,
    'shifts_updated',        v_updated,
    'mapped_candidates',     v_mapped_candidates,
    'skipped_locked_shifts', v_skipped_locked
  );
end;
$function$;

-- hr_autoprocess_apply_phase1(uuid,text[])
CREATE OR REPLACE FUNCTION public.hr_autoprocess_apply_phase1(import_id uuid, selected_group_ids text[] DEFAULT NULL::text[])
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_created           int := 0;
  v_updated           int := 0;
  v_mapped_candidates int := 0;
begin
  ----------------------------------------------------------------
  -- Sanity: import must exist and be HEALTHROSTER
  ----------------------------------------------------------------
  perform 1
  from public.hr_imports hi
  where hi.id = hr_autoprocess_apply_phase1.import_id
    and hi.source_system = 'HEALTHROSTER'::hr_source_enum;

  if not found then
    raise exception 'hr_autoprocess_apply_phase1: import % not found or not HEALTHROSTER', import_id;
  end if;

  ----------------------------------------------------------------
  -- src: normalise hr_rows + payload, join to hr_imports for client_id
  ----------------------------------------------------------------
  with src as (
    select
      r.id          as hr_row_id,
      r.external_row_key,
      hi.client_id  as client_id,
      r.date_local  as work_date,

      /* Staff name: payload.staff_name, else staff_raw / staff_norm */
      coalesce(
        nullif((r.payload_json ->> 'staff_name'), ''),
        nullif(r.staff_raw, ''),
        nullif(r.staff_norm, '')
      ) as staff_name,

      /* Ward: payload.ward, else hints from hr_rows */
      coalesce(
        nullif((r.payload_json ->> 'ward'), ''),
        nullif(r.unit_hint, ''),
        nullif(r.unit_raw, '')
      ) as ward,

      (r.payload_json ->> 'start_utc')::timestamptz as start_utc,
      (r.payload_json ->> 'end_utc')::timestamptz   as end_utc,
      coalesce((r.payload_json ->> 'break_mins')::int, 0) as break_mins,

      /* Finalised flags & HR request id */
      coalesce(
        nullif((r.payload_json ->> 'finalized_date'), ''),
        nullif((r.payload_json ->> 'finalised_date'), '')
      ) as finalized_raw,
      nullif((r.payload_json ->> 'request_id'), '') as request_id
    from public.hr_rows r
    join public.hr_imports hi
      on hi.id = r.import_id
    where r.import_id = hr_autoprocess_apply_phase1.import_id
      and r.date_local is not null
      and (r.payload_json ->> 'start_utc') is not null
      and (r.payload_json ->> 'end_utc')   is not null
  ),
  normed as (
    select
      s.hr_row_id,
      s.client_id,
      s.work_date,
      s.staff_name,

      -- keep existing behaviour for staff_norm output (lower+trim)
      lower(trim(s.staff_name)) as staff_norm,

      s.ward,
      lower(trim(s.ward))       as ward_norm,
      s.start_utc,
      s.end_utc,
      s.break_mins,
      s.request_id,

      case when coalesce(trim(s.finalized_raw), '') = '' then 'NO_FINALISED_DATE'
           else null
      end as held_back_reason,

      -- Compute or reuse external_row_key (same recipe as JS):
      coalesce(
        s.external_row_key,
        case
          when s.work_date is null or s.start_utc is null or s.end_utc is null then null
          else array_to_string(ARRAY[
            regexp_replace(trim(s.work_date::text),               '\|', ' ', 'g'),
            regexp_replace(coalesce(lower(trim(s.staff_name)),''), '\|',' ','g'),
            regexp_replace(coalesce(lower(trim(s.ward)),''),       '\|',' ','g'),
            regexp_replace(coalesce(s.client_id::text,''),         '\|',' ','g'),
            regexp_replace(coalesce(trim(s.request_id),''),        '\|',' ','g')
          ], '|')
        end
      ) as external_row_key
    from src s
  ),

  ----------------------------------------------------------------
  -- Candidate auto-mapping:
  --  1) aliases (legacy lower/trim OR symbol/space stripped)
  --  2) hr_name_mappings (legacy OR stripped)
  --  3) UNIQUE exact match on candidates (first+last OR last+first), stripped
  ----------------------------------------------------------------
  resolved as (
    select
      n.*,

      coalesce(
        cand_alias.id,
        cand_map.candidate_id,
        cand_exact_unique.candidate_id
      ) as candidate_id

    from normed n

    -- normalisations used for matching
    cross join lateral (
      select
        nullif(lower(trim(coalesce(n.staff_name,''))), '') as staff_lc,
        nullif(regexp_replace(lower(coalesce(n.staff_name,'')), '[^a-z0-9]+', '', 'g'), '') as staff_norm2
    ) nx

    -- 1) candidate aliases via nhsp_hr_name_aliases (support legacy + stripped)
    left join lateral (
      select c.id
      from public.candidates c
      where c.nhsp_hr_name_aliases is not null
        and (
          (nx.staff_lc    is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_lc]::text[]))
          or
          (nx.staff_norm2 is not null and c.nhsp_hr_name_aliases @> to_jsonb(array[nx.staff_norm2]::text[]))
        )
      limit 1
    ) cand_alias on true

    -- 2) fallback via hr_name_mappings.hr_name_norm (support legacy + stripped)
    left join lateral (
      select hm.candidate_id
      from public.hr_name_mappings hm
      where hm.active = true
        and (
          (nx.staff_lc    is not null and hm.hr_name_norm = nx.staff_lc)
          or
          (nx.staff_norm2 is not null and hm.hr_name_norm = nx.staff_norm2)
        )
      order by hm.created_at desc
      limit 1
    ) cand_map on (cand_alias.id is null)

    -- 3) UNIQUE exact candidate fallback (first+last OR last+first), symbols/spaces removed
    left join lateral (
      with matches as (
        select c.id as candidate_id
        from public.candidates c
        where c.active = true
          and nx.staff_norm2 is not null
          and (
            regexp_replace(lower(coalesce(c.first_name,'') || coalesce(c.last_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
            or
            regexp_replace(lower(coalesce(c.last_name,'')  || coalesce(c.first_name,'')), '[^a-z0-9]+', '', 'g') = nx.staff_norm2
          )
      )
      select
        case
          when count(*) = 1
            then (array_agg(candidate_id order by candidate_id::text))[1]
        end as candidate_id
      from matches
    ) cand_exact_unique on (cand_alias.id is null and cand_map.candidate_id is null)
  ),

  ----------------------------------------------------------------
  -- Current TSFIN state per timesheet (keyed by timesheet_id)
  ----------------------------------------------------------------
  fin_current as (
    select distinct on (tf.timesheet_id)
      tf.timesheet_id,
      tf.locked_by_invoice_id,
      tf.paid_at_utc
    from public.timesheets_financials tf
    where tf.is_current = true
    order by tf.timesheet_id, tf.created_at desc
  ),

  ----------------------------------------------------------------
  -- Update hr_rows.external_row_key where it was previously null/different
  ----------------------------------------------------------------
  ext_update as (
    update public.hr_rows r
    set external_row_key = res.external_row_key
    from resolved res
    where r.id = res.hr_row_id
      and res.external_row_key is not null
      and r.external_row_key is distinct from res.external_row_key
    returning 1
  ),

  ----------------------------------------------------------------
  -- Insert new HEALTHROSTER shifts
  ----------------------------------------------------------------
  ins as (
    insert into public.nhsp_shifts (
      external_row_key,
      latest_import_id,
      source_system,
      work_date,
      ward,
      start_utc,
      end_utc,
      break_mins,
      pay_minutes,
      client_id,
      hr_request_id,
      held_back_reason,
      candidate_id,
      created_at,
      updated_at
    )
    select
      r.external_row_key,
      hr_autoprocess_apply_phase1.import_id,
      'HEALTHROSTER'::hr_source_enum,
      r.work_date,
      nullif(r.ward, ''),
      r.start_utc,
      r.end_utc,
      coalesce(r.break_mins, 0),
      greatest(
        0,
        (extract(epoch from (r.end_utc - r.start_utc)) / 60)::int
        - coalesce(r.break_mins, 0)
      ) as pay_minutes,
      r.client_id,
      r.request_id,
      r.held_back_reason,
      r.candidate_id,
      now(),
      now()
    from resolved r
    where r.external_row_key is not null
      and not exists (
        select 1
        from public.nhsp_shifts s
        where s.external_row_key = r.external_row_key
      )
    returning
      (candidate_id is not null) as mapped_candidate
  ),

  ----------------------------------------------------------------
  -- Build update source rows + SAFE overwrite decision (no illegal LATERAL ref)
  ----------------------------------------------------------------
  upd_src as (
    select
      s.external_row_key,
      s.timesheet_id,

      s.candidate_id as old_candidate_id,
      s.client_id    as old_client_id,

      r.work_date,
      r.ward,
      r.start_utc,
      r.end_utc,
      r.break_mins,
      r.request_id,
      r.held_back_reason,
      r.client_id     as new_client_id,
      r.candidate_id  as new_candidate_id,

      fc.locked_by_invoice_id,
      fc.paid_at_utc,
      (fc.timesheet_id is null) as tsfin_missing,

      (
        s.timesheet_id is null
        or fc.timesheet_id is null
        or (fc.locked_by_invoice_id is null and fc.paid_at_utc is null)
      ) as safe_to_overwrite
    from public.nhsp_shifts s
    join resolved r
      on r.external_row_key = s.external_row_key
    left join fin_current fc
      on fc.timesheet_id = s.timesheet_id
  ),

  ----------------------------------------------------------------
  -- Update existing shifts with latest HR data (+ SAFE overwrite of ids)
  ----------------------------------------------------------------
  upd as (
    update public.nhsp_shifts s
    set
      latest_import_id = hr_autoprocess_apply_phase1.import_id,
      source_system    = 'HEALTHROSTER'::hr_source_enum,
      work_date        = u.work_date,
      ward             = nullif(u.ward, ''),
      start_utc        = u.start_utc,
      end_utc          = u.end_utc,
      break_mins       = coalesce(u.break_mins, 0),
      pay_minutes      = greatest(
                           0,
                           (extract(epoch from (u.end_utc - u.start_utc)) / 60)::int
                           - coalesce(u.break_mins, 0)
                         ),
      client_id        = case
                           when u.new_client_id is not null and u.safe_to_overwrite
                             then u.new_client_id
                           else s.client_id
                         end,
      hr_request_id    = u.request_id,
      held_back_reason = u.held_back_reason,
      updated_at       = now(),

      -- ✅ UPDATED FIX (same as NHSP apply logic):
      -- Allow corrected candidate_id to overwrite when SAFE:
      --   - shift not linked to a timesheet yet, OR
      --   - linked timesheet has no current TSFIN row, OR
      --   - linked timesheet TSFIN exists and is not paid and not invoice-locked.
      candidate_id     = case
                           when u.new_candidate_id is not null and u.safe_to_overwrite
                             then u.new_candidate_id
                           else s.candidate_id
                         end
    from upd_src u
    where s.external_row_key = u.external_row_key
    returning
      (u.old_candidate_id is null and u.new_candidate_id is not null and u.safe_to_overwrite) as mapped_candidate
  )

  ----------------------------------------------------------------
  -- Aggregate counts
  ----------------------------------------------------------------
  select
    coalesce((select count(*) from ins), 0),
    coalesce((select count(*) from upd), 0),
    coalesce((select count(*) from ins where mapped_candidate), 0)
      + coalesce((select count(*) from upd where mapped_candidate), 0)
  into
    v_created,
    v_updated,
    v_mapped_candidates;

  return jsonb_build_object(
    'import_id',         import_id,
    'shifts_created',    v_created,
    'shifts_updated',    v_updated,
    'mapped_candidates', v_mapped_candidates
  );
end;
$function$;

-- hr_daily_apply_transactional(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.hr_daily_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  v_src public.hr_source_enum;
  v_import_client_id uuid;
  v_validation_policy jsonb := '{}'::jsonb;
  v_validation_eligible_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];

  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_validation_rows_json jsonb := '[]'::jsonb;

  -- Expanded only from persisted, selected review decisions.
  v_invalidation_actions_json jsonb := '[]'::jsonb;
  v_invalidation_actions_count int := 0;

  v_validations_upserted int := 0;
  v_timesheets_ref_updated int := 0;
  v_timesheets_ref_cleared int := 0;

  v_email_logs_upserted int := 0;
  v_email_jobs jsonb := '[]'::jsonb;

  v_email_selected_count int := 0;

  -- ✅ TSFIN recompute support (validation changes + reference updates)
  v_daily_validation_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_updated_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_cleared_timesheet_ids uuid[] := array[]::uuid[];
  v_affected_timesheet_ids uuid[] := array[]::uuid[];

  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;
  v_post_commit_email_action_ids jsonb := '[]'::jsonb;

begin
  -- 0) Validate import exists + is HEALTHROSTER_DAILY
  select hi.source_system, hi.client_id
    into v_src, v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_src is null then
    raise exception 'hr_daily_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_src <> 'HEALTHROSTER_DAILY'::public.hr_source_enum then
    raise exception 'hr_daily_apply_transactional: import % source_system=%; expected HEALTHROSTER_DAILY.', p_import_id, v_src::text;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
     or not (v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;
  select coalesce(jsonb_agg(v.row_json order by v.sort_key),'[]'::jsonb) into v_validation_rows_json
  from (
    select 'row:'||r.hr_row_id::text sort_key,jsonb_build_object('timesheet_id',r.resolved_timesheet_id,
      'status',case when issue.reason_code is null then 'VALIDATION_OK' else 'VALIDATION_ERROR' end,
      'reason_code',coalesce(issue.reason_code,'HEALTHROSTER_DAILY'),'hr_request_id',h.hr_request_id) row_json
    from public.import_review_daily_timesheet_resolutions r
    join public.hr_rows h on h.id=r.hr_row_id and h.import_id=p_import_id
    left join lateral (select d.summary_json->>'reason_code' reason_code from public.import_review_decisions d
      where d.import_id=p_import_id and d.hr_row_id=r.hr_row_id and d.is_current
        and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER') limit 1) issue on true
    where r.import_id=p_import_id and r.status='CURRENT' and exists(
      select 1 from public.import_review_decisions include_decision
      where include_decision.import_id=p_import_id and include_decision.hr_row_id=r.hr_row_id
        and include_decision.is_current and include_decision.action_kind='NO_ACTION' and include_decision.selected
        and include_decision.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids')))
    union all
    select 'missing:'||d.action_id,jsonb_build_object('timesheet_id',d.timesheet_id,
      'status','VALIDATION_ERROR','reason_code','MISSING_FROM_IMPORT','hr_request_id',null)
    from public.import_review_decisions d
    where d.import_id=p_import_id and d.is_current and d.selected
      and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
      and d.action_kind='MARK_VALIDATION_ERROR'
  ) v;
  select coalesce(jsonb_agg(jsonb_build_object('timesheet_id',d.timesheet_id,'comparison_key',d.source_identity,'invalidate',true) order by d.action_id),'[]'::jsonb)
  into v_invalidation_actions_json from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(to_jsonb(d.action_id) order by d.action_id),'[]'::jsonb) into v_post_commit_email_action_ids
  from public.import_review_decisions d where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER');
  v_email_selected_count:=jsonb_array_length(v_post_commit_email_action_ids);

  -- 1) Validate database-built payload shapes. Invalidation is represented by
  -- explicit selected action IDs and expanded above from persisted decisions.
  if jsonb_typeof(v_validation_rows_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: validation_rows must be a JSON array.';
  end if;

  if jsonb_typeof(v_invalidation_actions_json) <> 'array' then
    raise exception 'hr_daily_apply_transactional: invalidation_actions must be a JSON array.';
  end if;

  v_invalidation_actions_count := jsonb_array_length(v_invalidation_actions_json);

  -- 2) Normalise validation rows (dedupe per timesheet_id and keep worst-case)
  create temporary table tmp_val_raw(
    timesheet_id uuid not null,
    status_text text null,
    reason_code text null,
    hr_request_id text null
  ) on commit drop;

  insert into tmp_val_raw(timesheet_id, status_text, reason_code, hr_request_id)
  select
    nullif(btrim(j.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(j.value->>'status'), '') as status_text,
    nullif(btrim(j.value->>'reason_code'), '') as reason_code,
    nullif(btrim(j.value->>'hr_request_id'), '') as hr_request_id
  from jsonb_array_elements(v_validation_rows_json) as j(value)
  where nullif(btrim(j.value->>'timesheet_id'), '') is not null;

  create temporary table tmp_val_by_ts(
    timesheet_id uuid primary key,
    has_error boolean not null,
    chosen_reason_code text null,
    chosen_hr_request_id text null
  ) on commit drop;

  insert into tmp_val_by_ts(timesheet_id, has_error, chosen_reason_code, chosen_hr_request_id)
  select
    vr.timesheet_id,
    bool_or(
      not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
    ) as has_error,
    case
      when bool_or(
        not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
      )
      then
        min(vr.reason_code) filter (
          where not (upper(coalesce(vr.status_text, '')) in ('VALIDATION_OK','OK','PASS','VALID'))
        )
      else
        'HEALTHROSTER_DAILY'
    end as chosen_reason_code,
    max(vr.hr_request_id) as chosen_hr_request_id
  from tmp_val_raw vr
  group by vr.timesheet_id;

  -- ✅ compute which timesheets will have a meaningful validation row change (before upsert)
  -- ✅ UPDATED: compute new_pre_validated (true only when OK and timesheet not authorised yet)
  create temporary table tmp_val_upsert on commit drop as
  select
    vt.timesheet_id,
    case
      when vt.has_error
        then 'VALIDATION_ERROR'::public.validation_status_enum
      else 'VALIDATION_OK'::public.validation_status_enum
    end as new_status,
    vt.chosen_reason_code as new_reason_code,
    case when vt.has_error then null else v_now end as new_validated_at_utc,
    p_import_id as new_last_source,
    vt.chosen_hr_request_id as new_hr_request_id,
    case
      when vt.has_error is false
       and tsu.timesheet_id is not null
       and tsu.authorised_at_server is null
      then true
      else false
    end as new_pre_validated
  from tmp_val_by_ts vt
  left join public.timesheets tsu
    on tsu.timesheet_id = vt.timesheet_id
   and tsu.is_current = true;

  select coalesce(array_agg(distinct x.timesheet_id order by x.timesheet_id), array[]::uuid[])
  into v_daily_validation_changed_timesheet_ids
  from (
    select u.timesheet_id
    from tmp_val_upsert u
    left join public.timesheet_validations tv
      on tv.timesheet_id = u.timesheet_id
    where tv.timesheet_id is null
       or tv.status is distinct from u.new_status
       or tv.validated_at_utc is distinct from u.new_validated_at_utc
       or tv.last_source is distinct from u.new_last_source
       or tv.reason_code is distinct from u.new_reason_code
       or tv.hr_request_id is distinct from u.new_hr_request_id
       or tv.pre_validated is distinct from u.new_pre_validated
  ) as x;

  -- 3) Upsert timesheet_validations (required + transactional)
  -- ✅ UPDATED: include pre_validated
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    pre_validated,
    updated_at,
    hr_request_id,
    hr_request_source,
    hr_request_set_by,
    hr_request_set_at_utc
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    u.new_pre_validated,
    v_now,
    u.new_hr_request_id,
    case when u.new_hr_request_id is null then null else 'IMPORTED'::public.reference_source_enum end,
    case when u.new_hr_request_id is null then null else p_actor_user_id end,
    case when u.new_hr_request_id is null then null else v_now end
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status               = excluded.status,
        reason_code           = excluded.reason_code,
        validated_at_utc      = excluded.validated_at_utc,
        last_source           = excluded.last_source,
        pre_validated         = excluded.pre_validated,
        updated_at            = excluded.updated_at,
        hr_request_id         = excluded.hr_request_id,
        hr_request_source     = excluded.hr_request_source,
        hr_request_set_by     = excluded.hr_request_set_by,
        hr_request_set_at_utc = excluded.hr_request_set_at_utc;

  get diagnostics v_validations_upserted = row_count;

  -- 4) DAILY reference truth:
  -- When VALIDATION_OK and chosen_hr_request_id present, set timesheets.reference_number
  -- ✅ Must NOT update invoiced/paid/locked timesheets
  -- ✅ Must only touch DAILY timesheets
  create temporary table tmp_ref_updated_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  insert into tmp_ref_updated_ids(timesheet_id)
  select distinct ts2.timesheet_id
  from public.timesheets ts2
  join tmp_val_by_ts vt2
    on vt2.timesheet_id = ts2.timesheet_id
  left join public.timesheets_financials tf2
    on tf2.timesheet_id = ts2.timesheet_id
   and tf2.is_current = true
  where ts2.is_current = true
    and upper(coalesce(ts2.sheet_scope::text, '')) = 'DAILY'
    and vt2.has_error is false
    and vt2.chosen_hr_request_id is not null
    and (ts2.reference_number is distinct from vt2.chosen_hr_request_id)
    and (tf2.timesheet_id is null or (tf2.locked_by_invoice_id is null and tf2.paid_at_utc is null));

  update public.timesheets tsu
     set reference_number = vt3.chosen_hr_request_id,
         updated_at = v_now
    from tmp_val_by_ts vt3
    join tmp_ref_updated_ids tri
      on tri.timesheet_id = vt3.timesheet_id
   where tsu.timesheet_id = vt3.timesheet_id
     and tsu.is_current = true
     and upper(coalesce(tsu.sheet_scope::text, '')) = 'DAILY';

  get diagnostics v_timesheets_ref_updated = row_count;

  select coalesce(array_agg(tri.timesheet_id order by tri.timesheet_id), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from tmp_ref_updated_ids tri;

  -- 4.1) ✅ MODE_A parity: destructive invalidation actions (clear refs)
  -- Schema aligns to weekly: {timesheet_id, comparison_key, invalidate:true|false}
  create temporary table tmp_invalidation_actions(
    timesheet_id uuid not null,
    comparison_key text not null,
    invalidate boolean not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  if v_invalidation_actions_count > 0 then
    insert into tmp_invalidation_actions(timesheet_id, comparison_key, invalidate)
    select
      nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
      nullif(btrim(a.value->>'comparison_key'), '') as comparison_key,
      (lower(coalesce(a.value->>'invalidate','true')) in ('true','1')) as invalidate
    from jsonb_array_elements(v_invalidation_actions_json) as a(value)
    where nullif(btrim(a.value->>'timesheet_id'), '') is not null
      and nullif(btrim(a.value->>'comparison_key'), '') is not null
    on conflict (timesheet_id, comparison_key) do update
      set invalidate = excluded.invalidate;
  end if;

  create temporary table tmp_ref_cleared_ids(
    timesheet_id uuid primary key
  ) on commit drop;

  insert into tmp_ref_cleared_ids(timesheet_id)
  select distinct ia.timesheet_id
  from tmp_invalidation_actions ia
  where ia.invalidate is true
    and ia.timesheet_id is not null
  on conflict do nothing;

  -- Clear timesheets.reference_number (DAILY only; never when locked/paid)
  with upd as (
    update public.timesheets tsclr
       set reference_number = null,
           updated_at = v_now
      from tmp_ref_cleared_ids rc
      left join public.timesheets_financials tfc
        on tfc.timesheet_id = rc.timesheet_id
       and tfc.is_current = true
     where tsclr.is_current = true
       and upper(coalesce(tsclr.sheet_scope::text, '')) = 'DAILY'
       and tsclr.timesheet_id = rc.timesheet_id
       and tsclr.reference_number is not null
       and (tfc.timesheet_id is null or (tfc.locked_by_invoice_id is null and tfc.paid_at_utc is null))
    returning tsclr.timesheet_id
  )
  select coalesce(array_agg(upd.timesheet_id order by upd.timesheet_id), array[]::uuid[])
  into v_ref_cleared_timesheet_ids
  from upd;

  v_timesheets_ref_cleared := coalesce(array_length(v_ref_cleared_timesheet_ids, 1), 0);

  -- For parity, also clear validation HR-request fields and force re-validation (PENDING)
  -- (This ensures the final outcome is not "validated OK" after a destructive clear.)
  create temporary table tmp_val_invalidation_upd(
    timesheet_id uuid primary key
  ) on commit drop;

  with upd_tv as (
    update public.timesheet_validations tvc
       set status = 'PENDING'::public.validation_status_enum,
           pre_validated = false,
           validated_at_utc = null,
           reason_code = 'TIMESHEET_CHANGED',
           last_source = p_import_id,
           updated_at = v_now,
           hr_request_id = null,
           hr_request_source = null,
           hr_request_set_by = null,
           hr_request_set_at_utc = null
      from tmp_ref_cleared_ids rc2
     where tvc.timesheet_id = rc2.timesheet_id
    returning tvc.timesheet_id
  )
  insert into tmp_val_invalidation_upd(timesheet_id)
  select upd_tv.timesheet_id
  from upd_tv
  where upd_tv.timesheet_id is not null
  on conflict do nothing;

  -- Ensure validation_affected includes invalidation-updated rows
  select coalesce(array_agg(distinct x.tsid order by x.tsid), array[]::uuid[])
  into v_daily_validation_changed_timesheet_ids
  from (
    select unnest(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[])) as tsid
    union
    select u.timesheet_id as tsid
    from tmp_val_invalidation_upd u
  ) as x
  where x.tsid is not null;

  -- Ensure ref_updated_timesheet_ids includes both ref-set and ref-cleared
  select coalesce(array_agg(distinct x.tsid order by x.tsid), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from (
    select unnest(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])) as tsid
    union
    select unnest(coalesce(v_ref_cleared_timesheet_ids, array[]::uuid[])) as tsid
  ) as x
  where x.tsid is not null;

  -- Exact-match Daily validation can authorise only after the imported
  -- reference is durably present.  Every selected row for the timesheet must
  -- be validation-OK, have one unambiguous HealthRoster reference, and leave
  -- the current DAILY record carrying that same reference.  The shared helper
  -- then resolves contract override -> client setting -> global default for
  -- each timesheet and applies the frozen-artifact safety gates.
  select coalesce(array_agg(distinct u.timesheet_id order by u.timesheet_id),array[]::uuid[])
  into v_validation_eligible_timesheet_ids
  from tmp_val_upsert u
  join public.timesheets t
    on t.timesheet_id=u.timesheet_id
   and t.is_current=true
   and t.revoked_at is null
  join public.hr_imports hi
    on hi.id=p_import_id
  cross join lateral (
    select case
      when jsonb_typeof(t.actual_schedule_json)='array'
       and jsonb_array_length(t.actual_schedule_json)>0
        then jsonb_array_length(t.actual_schedule_json)
      else 1
    end as segment_count
  ) whole_timesheet
  where u.new_status='VALIDATION_OK'::public.validation_status_enum
    and u.new_pre_validated=true
    and u.new_hr_request_id is not null
    and upper(coalesce(t.sheet_scope::text,''))='DAILY'
    and t.reference_number=u.new_hr_request_id
    and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
    and (
      hi.coverage_mode='COMPLETE_ALL'
      or exists (
        select 1
        from public.import_review_scope_candidates scoped_candidate
        join public.timesheets_financials scoped_financial
          on scoped_financial.timesheet_id=t.timesheet_id
         and scoped_financial.is_current=true
         and scoped_financial.candidate_id=scoped_candidate.candidate_id
        where scoped_candidate.import_id=p_import_id
      )
    )
    and whole_timesheet.segment_count=(
      select count(*)::integer
      from tmp_val_raw covered_row
      where covered_row.timesheet_id=u.timesheet_id
    )
    and not exists (
      select 1
      from tmp_val_raw raw_row
      where raw_row.timesheet_id=u.timesheet_id
        and (
          upper(coalesce(raw_row.status_text,'')) not in ('VALIDATION_OK','OK','PASS','VALID')
          or raw_row.hr_request_id is null
        )
    )
    and 1=(
      select count(distinct raw_ref.hr_request_id)
      from tmp_val_raw raw_ref
      where raw_ref.timesheet_id=u.timesheet_id
        and raw_ref.hr_request_id is not null
    );

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_validation_eligible_timesheet_ids,'HEALTHROSTER_DAILY'::public.hr_source_enum,true
  );

  -- Each target is resolved independently inside the owner-only helper so a
  -- contract override cannot be hidden by a client-only summary.  Return only
  -- bounded, non-authoritative diagnostics here.
  v_validation_policy:=jsonb_build_object(
    'source_system','HEALTHROSTER_DAILY',
    'validation_context',true,
    'hierarchy','CONTRACT_OVERRIDE_CLIENT_GLOBAL',
    'whole_timesheet_eligible_count',cardinality(v_validation_eligible_timesheet_ids),
    'auto_authorise_target_count',cardinality(v_auto_authorise_timesheet_ids)
  );

  -- Query emails are intentionally outside the source transaction. The
  -- database returns selected action IDs; the Worker later calls the
  -- idempotent outbox-backed enqueue RPC after source commit.

  -- ✅ build affected_timesheet_ids = (validation changed) ∪ (reference updated/cleared)
  select coalesce(array_agg(distinct a.tsid order by a.tsid), array[]::uuid[])
  into v_affected_timesheet_ids
  from (
    select unnest(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[])) as tsid
    union
    select unnest(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])) as tsid
  ) as a
  where a.tsid is not null;

  -- ✅ enqueue TSFIN priority for affected timesheets (if any)
  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(
      v_affected_timesheet_ids,
      'CONTEXT_CHANGED'::public.ts_fin_reason_enum
    );
  end if;

  -- 6) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 only after no work remains.
  update public.hr_imports hi2
     set import_scope = 'HR_DAILY'
   where hi2.id = p_import_id;

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'source_system', v_src::text,
    'validations_upserted', v_validations_upserted,

    -- Existing key kept; includes "set" updates only (as before)
    'timesheets_reference_updated', v_timesheets_ref_updated,

    -- ✅ NEW: explicit cleared count (MODE_A parity)
    'timesheets_reference_cleared', v_timesheets_ref_cleared,

    -- Existing key kept; now includes both set + cleared ids
    'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
    'ref_cleared_timesheet_ids', to_jsonb(coalesce(v_ref_cleared_timesheet_ids, array[]::uuid[])),

    'email_actions_received', v_email_selected_count,
    'email_logs_upserted', v_email_logs_upserted,
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_policy', v_validation_policy,
    'auto_authorise_timesheet_ids', to_jsonb(coalesce(v_auto_authorise_timesheet_ids, array[]::uuid[])),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_daily_validation_changed_timesheet_ids, array[]::uuid[])),
    'post_commit_email_action_ids',v_post_commit_email_action_ids,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    jsonb_array_length(v_post_commit_email_action_ids)>0 or cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;
end;
$function$;

-- hr_daily_timesheet_resolution_save_v1(uuid,uuid,uuid,bigint,integer,text,uuid,uuid)
CREATE OR REPLACE FUNCTION public.hr_daily_timesheet_resolution_save_v1(p_import_id uuid, p_hr_row_id uuid, p_timesheet_id uuid, p_expected_state_version bigint, p_expected_preview_generation integer, p_expected_evidence_fingerprint text, p_actor_user_id uuid DEFAULT NULL::uuid, p_request_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype; v_action public.import_review_decisions%rowtype;
  v_hr public.hr_rows%rowtype; v_ts public.v_timesheets_daily_match%rowtype;
  v_ts_row public.timesheets%rowtype;
  v_existing public.import_review_daily_timesheet_resolutions%rowtype;
  v_mapping public.hr_daily_grade_role_mappings%rowtype; v_mapping_count integer; v_timesheet_evidence jsonb;
  v_refresh jsonb; v_hash text; v_prior jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_request_id is null or p_import_id is null or p_hr_row_id is null then
    raise exception 'HR_DAILY_RESOLUTION_INPUT_INVALID' using errcode='22023';
  end if;
  v_hash:=public._import_review_hash_v1(concat_ws('|','daily-resolution-v1',p_import_id,p_hr_row_id,p_timesheet_id,
    p_expected_state_version,p_expected_preview_generation,p_expected_evidence_fingerprint));
  select event_context_json into v_prior from public.import_review_events
  where import_id=p_import_id and operation_id=p_request_id and event_code='DAILY_TIMESHEET_RESOLUTION_SAVED'
  order by id desc limit 1;
  if found then
    if v_prior->>'request_hash'<>v_hash then raise exception 'HR_DAILY_RESOLUTION_REQUEST_CONFLICT' using errcode='23505'; end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,'hr_row_id',p_hr_row_id,
      'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status');
  end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('BLOCKED','READY','IN_REVIEW') or v_state.state_version<>p_expected_state_version
    or v_state.preview_generation<>p_expected_preview_generation then
    raise exception 'HR_DAILY_RESOLUTION_REVIEW_STALE' using errcode='40001';
  end if;
  select * into v_action from public.import_review_decisions
  where import_id=p_import_id and hr_row_id=p_hr_row_id and is_current
    and action_kind in ('DAILY_TIMESHEET_RESOLUTION','ADVISORY','NO_ACTION')
  order by case action_kind when 'DAILY_TIMESHEET_RESOLUTION' then 0 when 'ADVISORY' then 1 else 2 end limit 1 for update;
  if not found or v_action.evidence_fingerprint<>p_expected_evidence_fingerprint then
    raise exception 'HR_DAILY_RESOLUTION_EVIDENCE_STALE' using errcode='40001';
  end if;
  select * into v_existing from public.import_review_daily_timesheet_resolutions
  where import_id=p_import_id and hr_row_id=p_hr_row_id for update;
  if found and v_existing.status='APPLIED' then
    if v_existing.resolved_timesheet_id is not distinct from p_timesheet_id then
      return jsonb_build_object('ok',true,'replay',true,'immutable',true,'import_id',p_import_id,'hr_row_id',p_hr_row_id,
        'timesheet_id',p_timesheet_id,'state_version',v_state.state_version,'status',v_state.status);
    end if;
    raise exception 'HR_DAILY_RESOLUTION_APPLIED_IMMUTABLE' using errcode='55000';
  end if;
  select * into v_hr from public.hr_rows where id=p_hr_row_id and import_id=p_import_id;
  if not found then raise exception 'HR_DAILY_RESOLUTION_ROW_NOT_FOUND' using errcode='P0002'; end if;

  if p_timesheet_id is null then
    insert into public.import_review_daily_timesheet_resolutions(import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,
      evidence_fingerprint,preview_generation,state_version,selected_by_user_id,stale_at_utc,stale_reason_code)
    values(p_import_id,p_hr_row_id,null,'USER_SELECTED','CLEARED',v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,
      p_actor_user_id,now(),'USER_CLEARED')
    on conflict(import_id,hr_row_id) do update set resolved_timesheet_id=null,resolution_method='USER_SELECTED',status='CLEARED',
      evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,state_version=excluded.state_version,
      selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,stale_at_utc=now(),stale_reason_code='USER_CLEARED',updated_at_utc=now();
  else
    select count(*) into v_mapping_count
    from public.hr_daily_grade_role_mappings gm
    where gm.client_id=v_action.client_id and gm.active
      and gm.incoming_grade_norm=lower(btrim(coalesce(nullif(v_hr.assignment_grade_norm,''),
        v_hr.payload_json->>'grade_raw',v_hr.payload_json->>'Request_Grade','')));
    if v_mapping_count<>1 then
      raise exception 'HR_DAILY_RESOLUTION_GRADE_MAPPING_STALE' using errcode='40001';
    end if;
    select * into v_mapping from public.hr_daily_grade_role_mappings gm
    where gm.client_id=v_action.client_id and gm.active
      and gm.incoming_grade_norm=lower(btrim(coalesce(nullif(v_hr.assignment_grade_norm,''),
        v_hr.payload_json->>'grade_raw',v_hr.payload_json->>'Request_Grade','')))
    order by gm.updated_at desc,gm.id limit 1 for update;
    select * into v_ts from public.v_timesheets_daily_match t where t.timesheet_id=p_timesheet_id;
    if not found or v_ts.candidate_id is distinct from v_action.candidate_id or v_ts.client_id is distinct from v_action.client_id
      or v_ts.sheet_scope::text<>'DAILY'
      or (v_ts.worked_start_iso at time zone 'Europe/London')::date<>v_hr.date_local then
      raise exception 'HR_DAILY_RESOLUTION_TARGET_OUTSIDE_ROW_SCOPE' using errcode='22023';
    end if;
    if lower(btrim(coalesce(v_ts.tsfin_role,''))) is distinct from lower(btrim(coalesce(v_mapping.role_code,'')))
      or (nullif(btrim(coalesce(v_mapping.band_norm,'')),'') is not null
        and lower(btrim(coalesce(v_ts.tsfin_band,''))) is distinct from lower(btrim(v_mapping.band_norm))) then
      raise exception 'HR_DAILY_RESOLUTION_GRADE_ROLE_MISMATCH' using errcode='22023';
    end if;
    select * into v_ts_row from public.timesheets t
    where t.timesheet_id=p_timesheet_id and t.is_current and t.revoked_at is null
    order by t.updated_at desc limit 1 for update;
    if not found then
      raise exception 'HR_DAILY_RESOLUTION_TIMESHEET_STALE' using errcode='40001';
    end if;
    select jsonb_strip_nulls(jsonb_build_object('timesheet_id',t.timesheet_id,'updated_at',t.updated_at,
      'contract_id',t.contract_id,
      'candidate_id',v_ts.candidate_id,'client_id',v_ts.client_id,
      'worked_start_iso',v_ts.worked_start_iso,'worked_end_iso',v_ts.worked_end_iso,
      'worked_minutes',v_ts.worked_minutes,'break_minutes',v_ts.break_minutes,
      'tsfin_role',v_ts.tsfin_role,'tsfin_band',v_ts.tsfin_band))
      into v_timesheet_evidence from public.timesheets t where t.timesheet_id=p_timesheet_id and t.is_current;
    insert into public.import_review_daily_timesheet_resolutions(import_id,hr_row_id,resolved_timesheet_id,resolution_method,status,
      evidence_fingerprint,preview_generation,state_version,selected_by_user_id)
    values(p_import_id,p_hr_row_id,p_timesheet_id,'USER_SELECTED','CURRENT',v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,p_actor_user_id)
    on conflict(import_id,hr_row_id) do update set resolved_timesheet_id=excluded.resolved_timesheet_id,resolution_method='USER_SELECTED',status='CURRENT',
      evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,state_version=excluded.state_version,
      selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,stale_at_utc=null,stale_reason_code=null,updated_at_utc=now();
  end if;
  update public.import_review_states set state_version=state_version+1,status='IN_REVIEW',updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;
  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'DAILY_TIMESHEET_RESOLUTION_SAVED',p_actor_user_id,jsonb_build_object(
    'request_hash',v_hash,'hr_row_id',p_hr_row_id,'timesheet_id',p_timesheet_id,
    'mapping_evidence',case when p_timesheet_id is null then null else jsonb_strip_nulls(jsonb_build_object(
      'mapping_id',v_mapping.id,'mapping_updated_at',v_mapping.updated_at,'mapped_role',v_mapping.role_code,'mapped_band',v_mapping.band_norm)) end,
    'timesheet_evidence',case when p_timesheet_id is null then null else v_timesheet_evidence end,
    'resulting_state_version',v_state.state_version,'status',v_state.status));
  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v_state.state_version,p_actor_user_id,500);
  return v_refresh||jsonb_build_object('replay',false,'hr_row_id',p_hr_row_id,'timesheet_id',p_timesheet_id,
    'resolution_method',case when p_timesheet_id is null then 'CLEARED' else 'USER_SELECTED' end);
end $function$;

-- hr_daily_validation_preview_v1(uuid,text,integer)
CREATE OR REPLACE FUNCTION public.hr_daily_validation_preview_v1(p_import_id uuid, p_after_action_id text DEFAULT NULL::text, p_limit integer DEFAULT 100)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'pg_temp'
AS $function$
declare v_limit integer:=least(greatest(coalesce(p_limit,100),1),200); v_state public.import_review_states%rowtype; v_actions jsonb; v_last text;
begin
  select * into v_state from public.import_review_states where import_id=p_import_id;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  with p as (select * from public.import_review_decisions d where d.import_id=p_import_id and d.is_current
    and (d.hr_row_id is not null or d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER','INVALIDATE_REFERENCE','MARK_VALIDATION_ERROR'))
    and (p_after_action_id is null or d.action_id>p_after_action_id) order by d.action_id limit v_limit+1),
  l as (select * from p order by action_id limit v_limit)
  select coalesce(jsonb_agg(jsonb_build_object('action_id',action_id,'action_kind',action_kind,'category',action_category,
    'hr_row_id',hr_row_id,'timesheet_id',timesheet_id,'evidence_fingerprint',evidence_fingerprint,'selectable',selectable,
    'default_selected',default_selected,'selected',selected,'blocking',blocking,'summary',summary_json,
    'resolution',(select jsonb_build_object('timesheet_id',r.resolved_timesheet_id,'method',r.resolution_method,'status',r.status,
      'evidence_fingerprint',r.evidence_fingerprint) from public.import_review_daily_timesheet_resolutions r
      where r.import_id=p_import_id and r.hr_row_id=l.hr_row_id)) order by action_id),'[]'),max(action_id) into v_actions,v_last from l;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'state_version',v_state.state_version,
    'preview_generation',v_state.preview_generation,'preview_fingerprint',v_state.preview_fingerprint,'actions',v_actions,
    'next_cursor',case when jsonb_array_length(v_actions)=v_limit then v_last end);
end $function$;

-- hr_weekly_apply_transactional(uuid,jsonb,uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_apply_transactional(p_import_id uuid, p_payload jsonb, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
 SET "plpgsql_check.mode" TO 'disabled'
AS $function$
declare
  v_now timestamptz := now();

  -- import header
  v_import_source_system text;
  v_import_client_id uuid;

  -- payload parts
  v_payload jsonb := coalesce(p_payload, '{}'::jsonb);
  v_actions_json jsonb := '[]'::jsonb;

  -- Expanded only from persisted, selected review decisions.
  v_invalidation_actions jsonb := '[]'::jsonb;
  v_invalidation_actions_count int := 0;

  -- normalized selections
  v_selected_action_ids text[] := array[]::text[];
  v_selected_truth_keys text[] := array[]::text[];
  v_selected_cancel_shift_ids uuid[] := array[]::uuid[];

  -- derived mode key sets
  v_mode_a_external_keys text[] := array[]::text[];
  v_mode_b_external_keys text[] := array[]::text[];

  -- selected truth keys constrained to MODE_B
  v_selected_truth_keys_mode_b text[] := array[]::text[];

  -- Mode B tick-only enforced lists
  v_force_keys_final text[] := array[]::text[];
  v_skip_keys_final text[] := array[]::text[];

  -- changed-hours partition (selected keys only, MODE_B)
  v_invoiced_changed_keys text[] := array[]::text[];
  v_not_invoiced_changed_keys text[] := array[]::text[];
  v_protected_source_timesheet_ids uuid[] := array[]::uuid[];
  v_force_keys_non_invoiced text[] := array[]::text[];

  v_phase3_result jsonb := null;
  v_changed_preflight jsonb := null;
  v_changed_timesheet_ids uuid[] := array[]::uuid[];
  v_reauthorise_timesheet_ids uuid[] := array[]::uuid[];
  v_lifecycle_items jsonb := '[]'::jsonb;
  v_unauthorise_result jsonb := null;
  v_reconciliation_action_ids text[] := array[]::text[];
  v_operation_bound_correction_action_ids text[] := array[]::text[];
  v_operation_bound_correction_keys text[] := array[]::text[];
  v_operation_bound_correction_timesheet_ids uuid[] := array[]::uuid[];
  v_general_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_reconciliation_transition jsonb := null;
  v_reconciliation_units jsonb := '[]'::jsonb;
  v_paid_unit jsonb;
  v_paid_timesheet_id uuid;
  v_paid_intent text;
  v_paid_current_count integer := 0;
  v_paid_current_tf public.timesheets_financials%rowtype;
  v_paid_preflight jsonb;
  v_paid_rollover jsonb;
  v_paid_mode text;
  v_paid_historical_id uuid;
  v_paid_shell_id uuid;
  v_paid_applied jsonb;
  v_paid_applied_timesheet_ids uuid[] := array[]::uuid[];
  v_paid_current_contract jsonb;
  v_paid_current_contract_fingerprint text;
  v_paid_current_policy_unit jsonb;
  v_paid_current_policy_count integer := 0;
  v_paid_current_policy jsonb;
  v_paid_origin_operation public.import_apply_operations%rowtype;
  v_paid_origin_contract jsonb;
  v_paid_origin_contract_fingerprint text;
  v_paid_origin_policy_unit jsonb;
  v_paid_origin_policy_count integer := 0;
  v_paid_historical_tf public.timesheets_financials%rowtype;
  v_paid_digest text;

  -- Phase 1 / 1.5 (MODE_B)
  v_phase1_result jsonb := null;
  v_phase15_ok int := 0;
  v_phase15_updated int := 0;

  -- cancellations (MODE_B)
  v_cancel_actions jsonb := '[]'::jsonb;
  v_cancellations_result jsonb := null;

  -- mirror (MODE_A)
  v_mirror_result jsonb := null;

  -- validation (MODE_A)
  v_weekly_val_payload jsonb := null;
  v_validations_upserted int := 0;
  v_mismatched_tsids uuid[] := array[]::uuid[];

  -- ✅ NEW: validation-changed timesheets (MODE_A) that must trigger TSFIN recompute
  v_validation_changed_timesheet_ids uuid[] := array[]::uuid[];

  -- ✅ NEW: count of ref clears due to missing shifts (MODE_A)
  v_mode_a_ref_cleared_count int := 0;

  -- ✅ NEW: count of ref sets due to matched shifts (MODE_A)
  v_mode_a_ref_set_count int := 0;

  -- ✅ NEW: timesheets whose reference truth changed (for post-apply QR reissue + regen)
  v_ref_updated_timesheet_ids uuid[] := array[]::uuid[];
  v_ref_updated_timesheet_ids_count int := 0;

  -- ✅ NEW: consolidated email jobs + items
  v_email_jobs jsonb := '[]'::jsonb;

  -- affected timesheets for TSFIN drain (MODE_B + MODE_A validation changes)
  v_affected_timesheet_ids uuid[] := array[]::uuid[];
  -- MODE_B targets are ordinary authoritative work.  MODE_A targets are kept
  -- in a separate array and enter auto-authorisation only after the stricter
  -- complete whole-timesheet validation gate below.
  v_authoritative_affected_timesheet_ids uuid[] := array[]::uuid[];
  v_validation_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];
  v_auto_authorise_timesheet_ids uuid[] := array[]::uuid[];

  -- policy A replacement-day
  v_selected_cancel_shift_id_set text[] := array[]::text[];

  -- debug counts
  v_steps jsonb := '[]'::jsonb;

  v_selected_action_ids_count int := 0;
  v_selected_row_keys_count int := 0;
  v_selected_cancel_shift_ids_count int := 0;

  v_mode_a_ok_keys_total int := 0;
  v_mode_b_ok_keys_total int := 0;

  v_force_keys_count int := 0;
  v_skip_keys_count int := 0;

  v_invoiced_changed_keys_count int := 0;
  v_not_invoiced_changed_keys_count int := 0;

  v_cancellations_count int := 0;

  v_phase3_created_count int := 0;
  v_phase3_updated_count int := 0;
  v_cancel_adjustment_count int := 0;
  v_correction_timesheets_created_count int := 0;

  v_val_rows_count int := 0;
  v_email_actions_count int := 0;
  v_email_jobs_count int := 0;

  v_sample_force_keys jsonb := '[]'::jsonb;
  v_sample_cancel_shift_ids jsonb := '[]'::jsonb;
  v_selected_action_ids_sample jsonb := '[]'::jsonb;

  v_mode_b_phase1_called boolean := false;
  v_mode_b_phase15_called boolean := false;
  v_mode_b_cancellations_called boolean := false;
  v_mode_b_phase3_called boolean := false;

  v_mode_b_should_run_phase1 boolean := false;
  v_mode_b_should_run_phase15 boolean := false;
  v_mode_b_should_run_cancellations boolean := false;
  v_mode_b_should_run_phase3 boolean := false;

  -- Server-owned review contract. A review state is mandatory for every call.
  v_review_contract jsonb := coalesce(v_payload->'review_contract','{}'::jsonb);
  v_review_selected_ids jsonb := coalesce(v_payload->'review_selected_action_ids','[]'::jsonb);
  v_review_operation_id uuid;
  v_review_guard jsonb;
  v_review_result jsonb;
  v_post_commit_email_action_ids jsonb := '[]'::jsonb;

  v_phase1_shifts_created int := null;
  v_phase1_shifts_updated int := null;

  v_last_shift_id uuid := null;

  -- ─────────────────────────────────────────────
  -- ✅ ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER SHIFTS (invariant)
  -- ─────────────────────────────────────────────
  v_ensure_pairs_count int := 0;
  v_ensure_pairs_skipped_no_active int := 0;

  v_ensure_base_week_created_count int := 0;
  v_ensure_base_week_existing_count int := 0;

  v_ensure_timesheet_created_count int := 0;
  v_ensure_timesheet_reused_count int := 0;
  v_ensure_timesheet_missing_reference_count int := 0;

  v_ensure_shifts_attached_count int := 0;
  v_ensure_shifts_relinked_invalid_ts_count int := 0;
  v_ensure_remaining_active_detached_count int := 0;

  -- ✅ NEW: MODE_A shift→timesheet linking (fix evidence + HR crosscheck + ref propagation)
  v_mode_a_shifts_attached_count int := 0;
  v_mode_a_ts_linked_count int := 0;

  v_ensure_sample_pairs jsonb := '[]'::jsonb;
  v_ensure_sample_created_ts_ids jsonb := '[]'::jsonb;

  -- loop vars for ensure
  v_pair_contract_id uuid;
  v_pair_candidate_id uuid;
  v_pair_client_id uuid;
  v_pair_week_ending_date date;

  v_active_count int := 0;

  v_base_week_id uuid := null;
  v_base_week_ts_id uuid := null;

  v_ts_exists boolean := false;

  v_candidate_display_name text := null;
  v_candidate_tms_ref text := null;
  v_client_name text := null;
  v_contract_display_site text := null;
  v_contract_ward_hint text := null;
  v_contract_role text := null;

  v_occupant_norm text := null;
  v_hospital_norm text := null;
  v_ward_norm text := null;
  v_role_norm text := null;

  v_booking_base text := null;
  v_hash_hex text := null;
  v_booking_id text := null;
  v_shift_label_norm text := null;

  v_new_ts_id uuid := null;

  v_attached_null_count int := 0;
  v_relinked_invalid_count int := 0;

  v_sqlstate text;
  v_err text;
begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));

  -- ─────────────────────────────────────────────
  -- 0) Validate import + header fields
  -- ─────────────────────────────────────────────
  select
    upper(coalesce(hi.source_system::text, '')),
    hi.client_id
  into
    v_import_source_system,
    v_import_client_id
  from public.hr_imports hi
  where hi.id = p_import_id
  limit 1;

  if v_import_source_system is null or v_import_source_system = '' then
    raise exception 'hr_weekly_apply_transactional: import % not found in hr_imports.', p_import_id;
  end if;

  if v_import_source_system <> 'HEALTHROSTER' then
    raise exception 'hr_weekly_apply_transactional: import % source_system=%; expected HEALTHROSTER.', p_import_id, v_import_source_system;
  end if;

  if v_import_client_id is null then
    raise exception 'hr_weekly_apply_transactional: import % missing client_id.', p_import_id;
  end if;

  if not exists(select 1 from public.import_review_states where import_id=p_import_id) then
    raise exception 'IMPORT_REVIEW_REQUIRED' using errcode='55000';
  end if;
  if jsonb_typeof(v_payload)<>'object' then
    raise exception 'IMPORT_REVIEW_APPLY_PAYLOAD_INVALID' using errcode='22023';
  end if;
  if exists(select 1 from jsonb_object_keys(v_payload) as keys(key_name)
    where keys.key_name not in ('review_contract','review_selected_action_ids','invalidation_action_ids')) then
    raise exception 'IMPORT_REVIEW_BROWSER_AUTHORITY_REJECTED' using errcode='22023';
  end if;
  if jsonb_typeof(v_review_contract)<>'object' or jsonb_typeof(v_review_selected_ids)<>'array'
    or not(v_payload?'invalidation_action_ids') or jsonb_typeof(v_payload->'invalidation_action_ids')<>'array' then
    raise exception 'IMPORT_REVIEW_APPLY_CONTRACT_REQUIRED' using errcode='22023';
  end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;
  v_review_guard:=public.import_review_apply_guard_v1(p_import_id,(v_review_contract->>'state_version')::bigint,
    v_review_contract->>'coverage_fingerprint',v_review_contract->>'preview_fingerprint',v_review_operation_id,
    v_review_contract->>'request_hash',v_review_selected_ids,v_payload->'invalidation_action_ids',p_actor_user_id);
  if coalesce((v_review_guard->>'replay')::boolean,false) then return v_review_guard->'stored_response'; end if;

  -- The server guard has reduced the request to complete, ready
  -- candidate/client units.  Keep that boundary available to every MODE_A
  -- validation/mirror step; otherwise a partial batch could validate or link
  -- rows belonging to a candidate that the operator deliberately left
  -- pending.
  drop table if exists pg_temp.tmp_review_batch_units;
  create temporary table tmp_review_batch_units(
    candidate_id uuid not null,
    client_id uuid not null,
    primary key(candidate_id,client_id)
  ) on commit drop;
  insert into tmp_review_batch_units(candidate_id,client_id)
  select distinct d.candidate_id,d.client_id
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.candidate_id is not null and d.client_id is not null
  on conflict do nothing;
  if not exists(select 1 from tmp_review_batch_units) then
    raise exception 'IMPORT_REVIEW_BATCH_SCOPE_EMPTY' using errcode='55000';
  end if;
  select coalesce(jsonb_agg(to_jsonb(case when d.action_kind='APPLY_CANCELLATION' then 'CANCEL:'||d.shift_id::text else 'ROW:'||d.source_identity end) order by d.action_id),'[]'::jsonb)
    into v_actions_json
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('INCLUDE_SHIFT','APPLY_AMENDMENT','APPLY_CANCELLATION');
  select coalesce(jsonb_agg(jsonb_build_object('timesheet_id',d.timesheet_id,'comparison_key',d.source_identity,'invalidate',true) order by d.action_id),'[]'::jsonb)
    into v_invalidation_actions
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind='INVALIDATE_REFERENCE';
  select coalesce(jsonb_agg(to_jsonb(d.action_id) order by d.action_id),'[]'::jsonb)
    into v_post_commit_email_action_ids
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.is_current and d.selected
    and d.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    and d.action_kind in ('EMAIL_ISSUE','EMAIL_REMINDER');
  v_email_actions_count:=jsonb_array_length(v_post_commit_email_action_ids);

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_OK','client_id',v_import_client_id::text));

  -- ─────────────────────────────────────────────
  -- 1) Parse and normalize selection payload (ROW:/CANCEL:)
  -- ─────────────────────────────────────────────
  if jsonb_typeof(v_actions_json) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: selected_action_ids must be a JSON array.';
  end if;

  if jsonb_typeof(v_invalidation_actions) <> 'array' then
    raise exception 'hr_weekly_apply_transactional: invalidation_actions must be a JSON array.';
  end if;

  v_invalidation_actions_count := jsonb_array_length(v_invalidation_actions);

  create temporary table tmp_sel_ids(
    action_id text primary key
  ) on commit drop;

  insert into tmp_sel_ids(action_id)
  select distinct nullif(btrim(x.value), '')
  from jsonb_array_elements_text(v_actions_json) as x(value)
  where nullif(btrim(x.value), '') is not null
  on conflict do nothing;

  if exists (
    select 1
    from tmp_sel_ids s
    where s.action_id !~ '^(ROW|CANCEL):'
  ) then
    raise exception 'hr_weekly_apply_transactional: invalid action_id in selection (expected ROW:<external_row_key> or CANCEL:<shift_id>).';
  end if;

  select coalesce(array_agg(s.action_id order by s.action_id), array[]::text[])
  into v_selected_action_ids
  from tmp_sel_ids s;

  select coalesce(array_agg(distinct substring(s.action_id from 5) order by substring(s.action_id from 5)), array[]::text[])
  into v_selected_truth_keys
  from tmp_sel_ids s
  where s.action_id like 'ROW:%';

  select coalesce(array_agg(distinct (substring(s.action_id from 8))::uuid order by (substring(s.action_id from 8))::uuid), array[]::uuid[])
  into v_selected_cancel_shift_ids
  from tmp_sel_ids s
  where s.action_id like 'CANCEL:%';

  v_selected_action_ids_count := coalesce(array_length(v_selected_action_ids, 1), 0);
  v_selected_row_keys_count := coalesce(array_length(v_selected_truth_keys, 1), 0);
  v_selected_cancel_shift_ids_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  select to_jsonb(coalesce(array_agg(x.a), array[]::text[]))
  into v_selected_action_ids_sample
  from (
    select a as a
    from unnest(coalesce(v_selected_action_ids, array[]::text[])) as a
    order by a
    limit 20
  ) as x;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','SELECTION_PARSED',
      'selected_action_ids_count', v_selected_action_ids_count,
      'selected_row_keys_count', v_selected_row_keys_count,
      'selected_cancel_shift_ids_count', v_selected_cancel_shift_ids_count,
      'email_actions_count', v_email_actions_count,
      'invalidation_actions_count', v_invalidation_actions_count,
      'selected_action_ids_sample', v_selected_action_ids_sample
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ tmp_aff_ts must exist early (PK + ON CONFLICT supported)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_aff_ts;
  create temporary table tmp_aff_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ✅ NEW: reference-updated timesheets (used for QR reissue + regen decisions)
  drop table if exists pg_temp.tmp_ref_updated_ts;
  create temporary table tmp_ref_updated_ts(
    timesheet_id uuid primary key
  ) on commit drop;

  -- ─────────────────────────────────────────────
  -- 2) Load weekly_import_phase2 + compute per-group authority through the
  --    shared current-setting core used by staging and catalogue generation.
  -- ─────────────────────────────────────────────
  create temporary table tmp_p2_all on commit drop as
  select *
  from public.weekly_import_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

  create temporary table tmp_p2_ok on commit drop as
  select
    p2.external_row_key,
    p2.candidate_id,
    p2.client_id,
    p2.contract_id,
    p2.week_ending_date,
    p2.work_date,
    upper(coalesce(p2.action::text,'')) as action
  from tmp_p2_all p2
  where upper(coalesce(p2.action::text,'')) = 'OK'
    and p2.external_row_key is not null
    and p2.contract_id is not null
    and p2.candidate_id is not null
    and p2.client_id is not null
    and p2.week_ending_date is not null;

  create temporary table tmp_group_mode on commit drop as
  select distinct
    t.contract_id,
    t.candidate_id,
    t.client_id,
    t.week_ending_date,
    ('grp:' || t.contract_id::text || ':' || t.week_ending_date::text || ':' || t.candidate_id::text) as group_id,
    case a.authority_mode when 'AUTHORITATIVE' then 'MODE_B'
      when 'VALIDATION_ONLY' then 'MODE_A' else 'OUT_OF_SCOPE' end as mode
  from (
    select distinct p2ok.contract_id, p2ok.candidate_id, p2ok.client_id, p2ok.week_ending_date
    from tmp_p2_ok p2ok
  ) as t
  join public.contracts c
    on c.id = t.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',c.id,c.client_id,t.week_ending_date) a;

  if exists(select 1 from tmp_group_mode gm join tmp_review_batch_units bu
      on bu.candidate_id=gm.candidate_id and bu.client_id=gm.client_id
      where gm.mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_IMPORT_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_p2_ok_mode on commit drop as
  select
    p2ok.external_row_key,
    p2ok.candidate_id,
    p2ok.client_id,
    p2ok.contract_id,
    p2ok.week_ending_date,
    p2ok.work_date,
    gm.group_id,
    gm.mode
  from tmp_p2_ok p2ok
  join tmp_group_mode gm
    on gm.contract_id = p2ok.contract_id
   and gm.candidate_id = p2ok.candidate_id
   and gm.week_ending_date = p2ok.week_ending_date;

  -- Validation-only Weekly work is atomic at the real timesheet boundary.
  -- A selected email by itself must never cause matching sibling shifts to be
  -- validated or sent to TSFIN.  The complete group is eligible only when at
  -- least one server-selected validation row exists, no unresolved action is
  -- present, and every selectable validation row in the group was selected.
  create temporary table tmp_mode_a_eligible_groups on commit drop as
  select gm.contract_id,gm.candidate_id,gm.client_id,gm.week_ending_date,gm.group_id
  from tmp_group_mode gm
  join tmp_review_batch_units bu
    on bu.candidate_id=gm.candidate_id and bu.client_id=gm.client_id
  where gm.mode='MODE_A'
    and exists (
      select 1
      from public.import_review_decisions selected_row
      where selected_row.import_id=p_import_id
        and selected_row.is_current
        and selected_row.action_id in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
        and selected_row.action_kind='NO_ACTION'
        and selected_row.candidate_id=gm.candidate_id
        and selected_row.client_id=gm.client_id
        and selected_row.contract_id is not distinct from gm.contract_id
        and nullif(selected_row.summary_json->>'week_ending_date','')::date=gm.week_ending_date
        and selected_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
    )
    and not exists (
      select 1
      from public.import_review_decisions hold
      where hold.import_id=p_import_id
        and hold.is_current
        and hold.candidate_id=gm.candidate_id
        and hold.client_id=gm.client_id
        and hold.contract_id is not distinct from gm.contract_id
        and nullif(hold.summary_json->>'week_ending_date','')::date=gm.week_ending_date
        and hold.summary_json->>'source_route' not like '%DAILY%'
        and (hold.blocking or hold.action_category in ('EMAIL','PENDING','BLOCKED'))
    )
    and not exists (
      select 1
      from public.import_review_decisions deferred_row
      where deferred_row.import_id=p_import_id
        and deferred_row.is_current
        and deferred_row.action_kind='NO_ACTION'
        and deferred_row.selectable
        and deferred_row.candidate_id=gm.candidate_id
        and deferred_row.client_id=gm.client_id
        and deferred_row.contract_id is not distinct from gm.contract_id
        and nullif(deferred_row.summary_json->>'week_ending_date','')::date=gm.week_ending_date
        and deferred_row.summary_json->>'authority_mode'='VALIDATION_ONLY'
        and deferred_row.action_id not in (select jsonb_array_elements_text(v_review_guard->'selected_action_ids'))
    );

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_a_external_keys
  from tmp_p2_ok_mode m
  join tmp_mode_a_eligible_groups eligible
    on eligible.contract_id=m.contract_id
   and eligible.candidate_id=m.candidate_id
   and eligible.client_id=m.client_id
   and eligible.week_ending_date=m.week_ending_date
  where m.mode = 'MODE_A';

  select coalesce(array_agg(distinct m.external_row_key order by m.external_row_key), array[]::text[])
  into v_mode_b_external_keys
  from tmp_p2_ok_mode m
  where m.mode = 'MODE_B';

  v_mode_a_ok_keys_total := coalesce(array_length(v_mode_a_external_keys, 1), 0);
  v_mode_b_ok_keys_total := coalesce(array_length(v_mode_b_external_keys, 1), 0);

  if exists (
    select 1
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    left join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
    where mbok.external_row_key is null
  ) then
    raise exception 'hr_weekly_apply_transactional: selection includes ROW:<external_row_key> that is not MODE_B (timesheet required).';
  end if;

  select coalesce(array_agg(k.external_row_key order by k.external_row_key), array[]::text[])
  into v_selected_truth_keys_mode_b
  from (
    select distinct k.external_row_key
    from unnest(coalesce(v_selected_truth_keys, array[]::text[])) as k(external_row_key)
    join (select distinct mb.external_row_key from tmp_p2_ok_mode mb where mb.mode = 'MODE_B') as mbok
      on mbok.external_row_key = k.external_row_key
  ) as k;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','PHASE2_OK_LOADED',
      'mode_a_ok_keys_total', v_mode_a_ok_keys_total,
      'mode_b_ok_keys_total', v_mode_b_ok_keys_total
    )
  );

  -- ─────────────────────────────────────────────
  -- 3) MODE_B tick = PROCEED (no decisions)
  -- ─────────────────────────────────────────────
  v_force_keys_final := coalesce(v_selected_truth_keys_mode_b, array[]::text[]);

  select coalesce(array_agg(x.external_row_key order by x.external_row_key), array[]::text[])
  into v_skip_keys_final
  from (
    select distinct okk.external_row_key
    from unnest(coalesce(v_mode_b_external_keys, array[]::text[])) as okk(external_row_key)
    left join unnest(coalesce(v_force_keys_final, array[]::text[])) as fk(external_row_key)
      on fk.external_row_key = okk.external_row_key
    where fk.external_row_key is null
  ) as x;

  v_force_keys_count := coalesce(array_length(v_force_keys_final, 1), 0);
  v_skip_keys_count := coalesce(array_length(v_skip_keys_final, 1), 0);
  v_cancellations_count := coalesce(array_length(v_selected_cancel_shift_ids, 1), 0);

  -- Only MODE_B is import-authoritative.  Ignore unrelated expense-only
  -- timesheets, but refuse to reuse a base contract-week timesheet occupied by
  -- calculated expenses or to amend/reverse an imported shift whose own linked
  -- timesheet contains them.  This runs before any source mutation.
  if exists (
    select 1
    from (
      select cw.timesheet_id
      from tmp_p2_ok_mode p2
      join public.contract_weeks cw
        on cw.contract_id=p2.contract_id
       and cw.week_ending_date=p2.week_ending_date
       and cw.is_adjustment=false
       and coalesce(cw.additional_seq,0)=0
      where p2.mode='MODE_B'
        and p2.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
        and cw.timesheet_id is not null
        and not exists (
          select 1
          from public.nhsp_shifts existing_import_shift
          where existing_import_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and existing_import_shift.client_id=v_import_client_id
            and existing_import_shift.external_row_key=p2.external_row_key
            and existing_import_shift.cancelled_at_utc is null
        )
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      where ns.source_system='HEALTHROSTER'::public.hr_source_enum
        and ns.client_id=v_import_client_id
        and ns.timesheet_id is not null
        and (
          ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
          or ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        )
    ) expense_target
    where public._import_review_timesheet_has_calculated_expenses_core_v1(expense_target.timesheet_id)
  ) then
    raise exception using
      message='IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
      errcode='P0001',
      detail=jsonb_build_object(
        'code','IMPORT_AUTHORITATIVE_EXPENSE_SEPARATION_REQUIRED',
        'message','Timesheet occupied by expenses. Remove the expenses from this timesheet, save or recalculate it, then choose Recheck. Expenses must be invoiced on a separate timesheet for import-authoritative work; no import mutation was applied.'
      )::text;
  end if;

  v_mode_b_should_run_phase1 := (v_force_keys_count > 0);
  v_mode_b_should_run_phase15 := (v_force_keys_count > 0);
  v_mode_b_should_run_cancellations := (v_cancellations_count > 0);

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','TICK_PROCEED_KEYS_READY',
      'mode_b_force_keys_count', v_force_keys_count,
      'mode_b_skip_keys_count', v_skip_keys_count,
      'mode_b_cancellations_count', v_cancellations_count
    )
  );

  -- ─────────────────────────────────────────────
  -- 4) MODE_B: do NOT run truth mutation work when there is nothing to apply
  -- ─────────────────────────────────────────────
  if (v_mode_b_should_run_phase1 is false) and (v_mode_b_should_run_cancellations is false) then
    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','MODE_B_NOOP_GUARD',
        'reason','NO_SELECTION_NO_CANCELLATION => SKIP_MODE_B_TRUTH_MUTATION',
        'should_run_phase1', false,
        'should_run_phase15', false,
        'should_run_phase3', false,
        'should_run_cancellations', false
      )
    );
  else
    -- MODE_B PHASE3 / PHASE1 / PHASE1.5 / CANCELLATIONS BLOCKS
    create temporary table tmp_changed_sel on commit drop as
    select
      ch.external_row_key,
      ch.timesheet_id,
      ch.is_paid,
      ch.is_invoiced
    from public.weekly_import_changed_hours_phase3(p_import_id := p_import_id, p_system_type := 'HEALTHROSTER') as ch
    where ch.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]));

    if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
      if exists(select 1 from pg_temp.import_review_reconciliation_units_v1 u
        where u.unit_json->>'source_system'<>'HEALTHROSTER'
           or u.unit_json->>'schema_version'<>'IMPORT_AUTHORITATIVE_RECONCILIATION_V1'
           or u.route not in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')) then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;
      select coalesce(array_agg(u.action_id order by u.action_id),array[]::text[]),
        coalesce(array_agg(u.action_id order by u.action_id) filter(where u.route in ('AMEND_PAID_UNINVOICED_SOURCE','AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
        coalesce(array_agg(u.source_identity order by u.source_identity) filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[])
      into v_reconciliation_action_ids,v_operation_bound_correction_action_ids,v_operation_bound_correction_keys
      from pg_temp.import_review_reconciliation_units_v1 u;
    end if;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is true
       or cs.is_paid is true;

    select coalesce(array_agg(cs.external_row_key order by cs.external_row_key), array[]::text[])
    into v_not_invoiced_changed_keys
    from tmp_changed_sel cs
    where cs.is_invoiced is false
      and cs.is_paid is false;

    if cardinality(v_reconciliation_action_ids)>0 then
      select coalesce(array_agg(u.source_identity order by u.source_identity)
        filter(where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')),array[]::text[]),
        coalesce(array_agg(u.source_identity order by u.source_identity)
        filter(where u.route in ('AMEND_SOURCE','AMEND_PAID_UNINVOICED_SOURCE')),array[]::text[])
      into v_invoiced_changed_keys,v_not_invoiced_changed_keys
      from pg_temp.import_review_reconciliation_units_v1 u;
      if cardinality(v_operation_bound_correction_action_ids)>0 then
        v_reconciliation_transition:=public.import_review_correction_generation_transition_v1(
          p_import_id,v_review_operation_id,v_review_contract->>'request_hash','PREPARE',p_actor_user_id,
          v_operation_bound_correction_action_ids,v_now);
      end if;
    end if;

    v_invoiced_changed_keys_count := coalesce(array_length(v_invoiced_changed_keys, 1), 0);
    v_not_invoiced_changed_keys_count := coalesce(array_length(v_not_invoiced_changed_keys, 1), 0);

    select coalesce(array_agg(distinct cs.timesheet_id order by cs.timesheet_id), array[]::uuid[])
      into v_changed_timesheet_ids
    from tmp_changed_sel cs
    where cs.timesheet_id is not null;

    select coalesce(array_agg(distinct protected.timesheet_id order by protected.timesheet_id),array[]::uuid[])
      into v_protected_source_timesheet_ids
    from (
      select cs.timesheet_id
      from tmp_changed_sel cs
      where cs.timesheet_id is not null
        and (cs.is_invoiced is true or cs.is_paid is true)
      union all
      -- A financial-position-only action may have no legacy changed-hours row.
      -- Its reviewed reconciliation unit is then the durable authority proving
      -- that the ordinary source is protected history and only the mutable
      -- correction members may enter the post-commit TSFIN target set.
      select nullif(u.unit_json->>'source_timesheet_id','')::uuid
      from pg_temp.import_review_reconciliation_units_v1 u
      where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT')
        and nullif(u.unit_json->>'source_timesheet_id','') is not null
    ) protected
    where protected.timesheet_id is not null;

    if coalesce(array_length(v_changed_timesheet_ids, 1), 0) > 0 then
      select public.import_timesheet_financial_preflight_v1(
        p_timesheet_ids := v_changed_timesheet_ids,
        p_action := 'IMPORT_CHANGED_HOURS',
        p_actor_user_id := p_actor_user_id,
        p_expected_state_json := '{}'::jsonb,
        p_lock_rows := true,
        p_max_scope := 100
      ) into v_changed_preflight;

      if coalesce((v_changed_preflight->>'allowed')::boolean, false) is not true then
        raise exception using
          message = 'IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',
          errcode = 'P0001',
          detail = v_changed_preflight::text;
      end if;

    end if;

    -- Mode B only: preserve the lifecycle state of authorised, mutable source
    -- timesheets.  Reauthorisation is deliberately deferred until the Worker
    -- has completed the bounded TSFIN refresh for this committed operation.
    select coalesce(array_agg(distinct lifecycle_scope.timesheet_id order by lifecycle_scope.timesheet_id),array[]::uuid[])
    into v_reauthorise_timesheet_ids
    from (
      select cs.timesheet_id
      from tmp_changed_sel cs
      join public.timesheets ts on ts.timesheet_id=cs.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where cs.timesheet_id is not null
        and not (cs.external_row_key=any(coalesce(v_operation_bound_correction_keys,array[]::text[])))
        and cs.is_invoiced is false
        and (cs.is_paid is false or exists(select 1 from pg_temp.import_review_reconciliation_units_v1 paid_unit
          where paid_unit.source_identity=cs.external_row_key
            and paid_unit.route='AMEND_PAID_UNINVOICED_SOURCE'
            and paid_unit.unit_json->>'intended_authorisation_action'='REAUTHORISE'))
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
      union
      select ns.timesheet_id
      from public.nhsp_shifts ns
      join public.timesheets ts on ts.timesheet_id=ns.timesheet_id and ts.is_current=true
      left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
      left join public.contract_weeks cw on cw.timesheet_id=ts.timesheet_id
      where ns.id=any(coalesce(v_selected_cancel_shift_ids,array[]::uuid[]))
        and ns.timesheet_id is not null
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'paid')::boolean,false)=false
        and coalesce((public._import_review_timesheet_protection_core_v1(ns.timesheet_id)->>'invoice_locked')::boolean,false)=false
        and (ts.authorised_at_server is not null or tf.authorised_at_utc is not null
          or cw.status='AUTHORISED'::public.contract_week_status_enum)
    ) lifecycle_scope;

    if cardinality(v_reauthorise_timesheet_ids)>100 then
      raise exception 'IMPORT_REVIEW_REAUTHORISE_SCOPE_TOO_LARGE' using errcode='54000';
    end if;
    if cardinality(v_reauthorise_timesheet_ids)>0 then
      select coalesce(jsonb_agg(jsonb_build_object(
        'timesheet_id',target_id::text,
        'expected_timesheet_id',target_id::text
      ) order by target_id),'[]'::jsonb)
      into v_lifecycle_items
      from unnest(v_reauthorise_timesheet_ids) as lifecycle_target(target_id);

      select public.timesheet_unauthorise_bulk_atomic(v_lifecycle_items,p_actor_user_id,v_now)
      into v_unauthorise_result;
      if coalesce((v_unauthorise_result->>'ok')::boolean,false) is not true
        or coalesce((v_unauthorise_result->>'all_success')::boolean,false) is not true then
        raise exception using message='IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',errcode='P0001',
          detail=jsonb_build_object(
            'code','IMPORT_REVIEW_CANONICAL_UNAUTHORISE_FAILED',
            'timesheet_ids',to_jsonb(v_reauthorise_timesheet_ids),
            'failure_count',coalesce((v_unauthorise_result->>'failure_count')::int,cardinality(v_reauthorise_timesheet_ids))
          )::text;
      end if;
      v_steps:=v_steps||jsonb_build_array(jsonb_build_object(
        'step','CANONICAL_UNAUTHORISE_COMPLETE',
        'reauthorise_timesheet_count',cardinality(v_reauthorise_timesheet_ids)
      ));
    end if;

    -- Execute the reviewed ordinary paid-but-uninvoiced route before source
    -- truth is amended. Paid status alone never routes a source through phase 3.
    for v_paid_unit in
      select u.unit_json from pg_temp.import_review_reconciliation_units_v1 u
      where u.route='AMEND_PAID_UNINVOICED_SOURCE'
      order by u.source_timesheet_id
    loop
      v_paid_timesheet_id:=nullif(v_paid_unit->>'source_timesheet_id','')::uuid;
      v_paid_intent:=v_paid_unit->>'intended_authorisation_action';
      if v_paid_timesheet_id is null
         or v_paid_intent not in ('REAUTHORISE','AUTHORISE','LEAVE_UNAUTHORISED') then
        raise exception 'IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID' using errcode='22023';
      end if;

      select operation_row.response_json#>'{correction_operation_contract}'
      into v_paid_current_contract from public.import_apply_operations operation_row
      where operation_row.id=v_review_operation_id;
      v_paid_current_contract_fingerprint:=encode(extensions.digest(convert_to(
        (v_paid_current_contract-'operation_contract_fingerprint')::text,'UTF8'),'sha256'),'hex');
      if jsonb_typeof(v_paid_current_contract)<>'object'
         or v_paid_current_contract->>'operation_contract_fingerprint' is distinct from v_paid_current_contract_fingerprint then
        raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
      end if;
      select count(*)::integer,min(policy_unit::text)::jsonb
      into v_paid_current_policy_count,v_paid_current_policy_unit
      from jsonb_array_elements(coalesce(v_paid_current_contract->'correction_units','[]'::jsonb)) policy_unit
      where policy_unit->>'action_id'=v_paid_unit->>'action_id'
        and policy_unit->>'root_timesheet_id'=v_paid_timesheet_id::text
        and policy_unit->>'source_row_key'=v_paid_unit->>'source_identity';
      if v_paid_current_policy_count<>1
         or jsonb_typeof(v_paid_current_policy_unit->'policy_envelope')<>'object' then
        raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID' using errcode='P0001';
      end if;
      v_paid_current_policy:=v_paid_current_policy_unit->'policy_envelope';

      perform 1 from public.timesheets exact_source
      where exact_source.timesheet_id=v_paid_timesheet_id
        and exact_source.is_current and exact_source.archived_at_utc is null for update;
      if not found then
        raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
      end if;
      perform 1 from public.timesheets_financials current_lock
      where current_lock.timesheet_id=v_paid_timesheet_id and current_lock.is_current
      order by current_lock.id for update;
      select count(*)::integer into v_paid_current_count from public.timesheets_financials current_tf
      where current_tf.timesheet_id=v_paid_timesheet_id and current_tf.is_current;
      if v_paid_current_count<>1 then
        raise exception 'IMPORT_REVIEW_SELECTED_ACTION_STALE' using errcode='40001';
      end if;
      select current_tf.* into v_paid_current_tf from public.timesheets_financials current_tf
      where current_tf.timesheet_id=v_paid_timesheet_id and current_tf.is_current;

      if v_paid_current_tf.paid_at_utc is not null then
        v_paid_preflight:=public.import_timesheet_financial_preflight_v1(
          array[v_paid_timesheet_id]::uuid[],'PAID_UNINVOICED_ROLLOVER',p_actor_user_id,
          '{}'::jsonb,false,100);
        if coalesce((v_paid_preflight->>'allowed')::boolean,false) is not true
           or v_paid_preflight->>'required_path' is distinct from 'PAID_UNINVOICED_ROLLOVER'
           or coalesce((v_paid_preflight->>'input_count')::integer,0)<>1
           or coalesce((v_paid_preflight->>'member_count')::integer,0)<>1
           or coalesce((v_paid_preflight->>'paid_count')::integer,0)<>1
           or coalesce((v_paid_preflight->>'invoice_lined_count')::integer,0)<>0
           or coalesce((v_paid_preflight->>'blocking_batch_count')::integer,0)<>0
           or coalesce((v_paid_preflight->>'stale_tsfin_count')::integer,0)<>0
           or jsonb_array_length(coalesce(v_paid_preflight->'errors','[]'::jsonb))<>0 then
          raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
        end if;
        v_paid_rollover:=public.timesheet_paid_uninvoiced_rollover_v1(
          v_paid_timesheet_id,p_actor_user_id,v_review_operation_id,v_paid_current_tf.id,
          v_paid_preflight->>'preflight_fingerprint',v_now);
        v_paid_historical_id:=nullif(v_paid_rollover->>'historical_paid_tsfin_id','')::uuid;
        v_paid_shell_id:=nullif(v_paid_rollover->>'new_current_tsfin_id','')::uuid;
        v_paid_mode:='CREATED_CURRENT_OPERATION_SHELL';
        if coalesce((v_paid_rollover->>'ok')::boolean,false) is not true
           or v_paid_historical_id is distinct from v_paid_current_tf.id
           or v_paid_shell_id is null
           or not exists(select 1 from public.timesheets_financials shell
             where shell.id=v_paid_shell_id and shell.timesheet_id=v_paid_timesheet_id and shell.is_current
               and shell.paid_at_utc is null and shell.processing_status='PENDING_AUTH'::public.ts_fin_processing_status_enum
               and shell.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
               and coalesce((shell.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)
               and shell.policy_snapshot_json->>'import_apply_operation_id'=v_review_operation_id::text
               and shell.policy_snapshot_json->>'rollover_source_tsfin_id'=v_paid_historical_id::text)
           or (select count(*) from public.timesheets_financials shell
             where shell.timesheet_id=v_paid_timesheet_id and shell.is_current)<>1 then
          raise exception 'PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID' using errcode='P0001';
        end if;
      else
        -- Reuse only a settled shell from a completed prior operation whose own
        -- frozen contract and paid lineage prove it, then compare only stable
        -- policy facts with this operation.
        if v_paid_current_tf.locked_by_invoice_id is not null
           or exists(select 1 from public.invoice_lines il where il.timesheet_id=v_paid_timesheet_id)
           or v_paid_current_tf.authorised_at_utc is not null
           or v_paid_current_tf.processing_status<>'PENDING_AUTH'::public.ts_fin_processing_status_enum
           or coalesce(v_paid_current_tf.is_stale,false)
           or v_paid_current_tf.stale_reason is not null
           or not coalesce((v_paid_current_tf.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)
         or coalesce(v_paid_current_tf.policy_snapshot_json->>'import_apply_operation_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
         or coalesce(v_paid_current_tf.policy_snapshot_json->>'rollover_source_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           or nullif(v_paid_current_tf.policy_snapshot_json->>'rollover_source_paid_digest','') is null then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
        end if;
        select origin.* into v_paid_origin_operation from public.import_apply_operations origin
        where origin.id=(v_paid_current_tf.policy_snapshot_json->>'import_apply_operation_id')::uuid for update;
        if not found or v_paid_origin_operation.state<>'COMPLETE' then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_ORIGIN_INCOMPLETE' using errcode='P0001';
        end if;
        v_paid_origin_contract:=v_paid_origin_operation.response_json#>'{correction_operation_contract}';
        v_paid_origin_contract_fingerprint:=encode(extensions.digest(convert_to(
          (v_paid_origin_contract-'operation_contract_fingerprint')::text,'UTF8'),'sha256'),'hex');
        if jsonb_typeof(v_paid_origin_contract)<>'object'
           or v_paid_origin_contract->>'operation_contract_fingerprint' is distinct from v_paid_origin_contract_fingerprint then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
        end if;
        select count(*)::integer,min(origin_unit::text)::jsonb
        into v_paid_origin_policy_count,v_paid_origin_policy_unit
        from jsonb_array_elements(coalesce(v_paid_origin_contract->'correction_units','[]'::jsonb)) origin_unit
        where origin_unit->>'root_timesheet_id'=v_paid_timesheet_id::text
          and origin_unit->>'source_row_key'=v_paid_unit->>'source_identity';
        if v_paid_origin_policy_count<>1
           or v_paid_origin_policy_unit->'policy_envelope' is distinct from
             v_paid_current_tf.policy_snapshot_json->'correction_financials_policy_envelope'
           or v_paid_origin_policy_unit->>'policy_envelope_fingerprint' is distinct from
             v_paid_current_tf.policy_snapshot_json->>'correction_financials_policy_envelope_fingerprint' then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
        end if;
        select historical.* into v_paid_historical_tf from public.timesheets_financials historical
        where historical.id=(v_paid_current_tf.policy_snapshot_json->>'rollover_source_tsfin_id')::uuid
          and historical.timesheet_id=v_paid_timesheet_id and not historical.is_current
          and historical.paid_at_utc is not null and historical.locked_by_invoice_id is null;
        if not found then raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001'; end if;
        v_paid_digest:=encode(extensions.digest(convert_to(jsonb_build_object(
          'id',v_paid_historical_tf.id::text,'timesheet_id',v_paid_historical_tf.timesheet_id::text,
          'timesheet_version',v_paid_historical_tf.timesheet_version,'paid_at_utc',v_paid_historical_tf.paid_at_utc,
          'paid_by_user_id',case when v_paid_historical_tf.paid_by_user_id is null then null else v_paid_historical_tf.paid_by_user_id::text end,
          'payment_reference',v_paid_historical_tf.payment_reference,'total_hours',v_paid_historical_tf.total_hours,
          'total_pay_ex_vat',v_paid_historical_tf.total_pay_ex_vat,'total_charge_ex_vat',v_paid_historical_tf.total_charge_ex_vat,
          'pay_vat_rate_pct_snapshot',v_paid_historical_tf.pay_vat_rate_pct_snapshot,
          'pay_vat_amount_snapshot',v_paid_historical_tf.pay_vat_amount_snapshot,
          'pay_total_inc_vat_snapshot',v_paid_historical_tf.pay_total_inc_vat_snapshot,
          'policy_snapshot_json',v_paid_historical_tf.policy_snapshot_json,'rate_source_refs_json',v_paid_historical_tf.rate_source_refs_json,
          'actual_schedule_json',v_paid_historical_tf.actual_schedule_json)::text,'UTF8'),'sha256'),'hex');
        if v_paid_digest is distinct from v_paid_current_tf.policy_snapshot_json->>'rollover_source_paid_digest' then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_INVALID' using errcode='P0001';
        end if;
        if v_paid_origin_contract->>'source_system' is distinct from v_paid_current_contract->>'source_system'
           or v_paid_origin_policy_unit#>>'{policy_envelope,classification,source_shift_id}' is distinct from v_paid_current_policy#>>'{classification,source_shift_id}'
           or v_paid_origin_policy_unit#>>'{policy_envelope,classification,source_row_key}' is distinct from v_paid_current_policy#>>'{classification,source_row_key}'
           or v_paid_origin_policy_unit#>>'{policy_envelope,root_timesheet_id}' is distinct from v_paid_current_policy->>'root_timesheet_id'
           or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,leg_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,leg_fingerprint}'
           or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,tsfin_policy,tsfin_policy_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,tsfin_policy,tsfin_policy_fingerprint}'
           or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,invoice_policy,invoice_policy_fingerprint}' is distinct from v_paid_current_policy#>>'{replacement,invoice_policy,invoice_policy_fingerprint}'
           or v_paid_origin_policy_unit#>>'{policy_envelope,replacement,invoice_policy,invoice_stream}' is distinct from v_paid_current_policy#>>'{replacement,invoice_policy,invoice_stream}' then
          raise exception 'IMPORT_REVIEW_PAID_ROLLOVER_SHELL_POLICY_CHANGED' using errcode='P0001';
        end if;
        v_paid_historical_id:=v_paid_historical_tf.id;
        v_paid_shell_id:=v_paid_current_tf.id;
        v_paid_mode:='REUSED_COMPLETED_OPERATION_SHELL';
      end if;

      v_paid_applied:=jsonb_build_object(
        'applied_timesheet_id',v_paid_timesheet_id,'rollover_mode',v_paid_mode,
        'historical_paid_tsfin_id',v_paid_historical_id,'current_shell_tsfin_id',v_paid_shell_id,
        'intended_authorisation_action',v_paid_intent,
        'reviewed_unit_fingerprint',v_paid_unit->>'unit_fingerprint',
        'reconciliation_fingerprint',v_paid_unit->>'reconciliation_fingerprint');
      update pg_temp.import_review_reconciliation_units_v1 applied_unit
      set unit_json=applied_unit.unit_json||v_paid_applied||jsonb_build_object(
        'applied_result_fingerprint',encode(extensions.digest(convert_to(v_paid_applied::text,'UTF8'),'sha256'),'hex'))
      where applied_unit.action_id=v_paid_unit->>'action_id';
      v_paid_applied_timesheet_ids:=array_append(v_paid_applied_timesheet_ids,v_paid_timesheet_id);
    end loop;

    v_mode_b_should_run_phase3 := (v_invoiced_changed_keys_count > 0);

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','CHANGED_HOURS_PARTITIONED',
        'invoiced_changed_keys_count', v_invoiced_changed_keys_count,
        'not_invoiced_changed_keys_count', v_not_invoiced_changed_keys_count
      )
    );

    create temporary table tmp_selected_replacement_keys(
      candidate_id uuid,
      client_id uuid,
      old_work_date date,
      replacement_day_key text
    ) on commit drop;

    if array_length(v_force_keys_final, 1) is not null then
      create temporary table tmp_sel_truth_p2 on commit drop as
      select
        m.external_row_key,
        m.candidate_id,
        m.client_id,
        m.work_date as import_work_date
      from tmp_p2_ok_mode m
      where m.mode = 'MODE_B'
        and m.external_row_key = any(v_force_keys_final);

      create temporary table tmp_existing_by_key on commit drop as
      select distinct on (ns.external_row_key)
        ns.external_row_key,
        ns.id as shift_id,
        ns.candidate_id as candidate_id,
        ns.client_id as client_id,
        ns.work_date as old_work_date
      from public.nhsp_shifts ns
      where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.cancelled_at_utc is null
        and ns.external_row_key = any(v_force_keys_final)
        and ns.work_date is not null
      order by ns.external_row_key, ns.updated_at desc nulls last, ns.created_at desc nulls last;

      insert into tmp_selected_replacement_keys(candidate_id, client_id, old_work_date, replacement_day_key)
      select distinct
        (coalesce(ex.candidate_id, st.candidate_id))::uuid as candidate_id,
        (coalesce(ex.client_id, st.client_id))::uuid as client_id,
        ex.old_work_date as old_work_date,
        ((coalesce(ex.candidate_id, st.candidate_id))::text || '|' ||
         (coalesce(ex.client_id, st.client_id))::text || '|' ||
         (ex.old_work_date)::text) as replacement_day_key
      from tmp_sel_truth_p2 st
      join tmp_existing_by_key ex
        on ex.external_row_key = st.external_row_key
      where ex.old_work_date is not null
        and st.import_work_date is not null
        and ex.old_work_date <> st.import_work_date;

      select coalesce(array_agg(x::text), array[]::text[])
      into v_selected_cancel_shift_id_set
      from unnest(coalesce(v_selected_cancel_shift_ids, array[]::uuid[])) as x;

      if exists (select 1 from tmp_selected_replacement_keys) then
        create temporary table tmp_required_cancel_ids on commit drop as
        select distinct
          rk.replacement_day_key,
          ns2.id as shift_id
        from tmp_selected_replacement_keys rk
        join public.nhsp_shifts ns2
          on ns2.source_system = 'HEALTHROSTER'::public.hr_source_enum
         and ns2.client_id = v_import_client_id
         and ns2.cancelled_at_utc is null
         and ns2.candidate_id = rk.candidate_id
         and ns2.client_id = rk.client_id
         and ns2.work_date = rk.old_work_date;

        if exists (
          select 1
          from tmp_required_cancel_ids rc
          left join unnest(coalesce(v_selected_cancel_shift_id_set, array[]::text[])) as sel(shift_id_text)
            on sel.shift_id_text = rc.shift_id::text
          where sel.shift_id_text is null
        ) then
          raise exception 'hr_weekly_apply_transactional: Policy A violation (replacement-day selected without selecting all required cancellations).';
        end if;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','POLICY_A_OK'));

    if v_mode_b_should_run_phase3 then
      select public.hr_weekly_phase3_apply_adjustment_truth(
        p_import_id := p_import_id,
        p_selected_external_row_keys := v_invoiced_changed_keys,
        p_actor_user_id := p_actor_user_id
      )
      into v_phase3_result;

      v_mode_b_phase3_called := true;
    end if;

    v_phase3_created_count := jsonb_array_length(coalesce(v_phase3_result->'created_timesheet_ids', '[]'::jsonb));
    v_phase3_updated_count := jsonb_array_length(coalesce(v_phase3_result->'updated_timesheet_ids', '[]'::jsonb));

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE3_CORRECTIONS_DONE',
        'phase3_called', v_mode_b_phase3_called,
        'phase3_created_count', v_phase3_created_count,
        'phase3_updated_count', v_phase3_updated_count
      )
    );

    if v_mode_b_should_run_phase1 then
      select public.hr_autoprocess_apply_phase1(
        import_id := p_import_id,
        selected_group_ids := array[]::text[],
        p_skip_external_row_keys := v_skip_keys_final,
        p_force_overwrite_external_row_keys := v_force_keys_final
      )
      into v_phase1_result;

      v_mode_b_phase1_called := true;

      v_phase1_shifts_created :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_created')
           and (v_phase1_result->>'shifts_created') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_created')::int
          else null
        end;

      v_phase1_shifts_updated :=
        case
          when v_phase1_result is not null
           and jsonb_typeof(v_phase1_result) = 'object'
           and (v_phase1_result ? 'shifts_updated')
           and (v_phase1_result->>'shifts_updated') ~ '^[0-9]+$'
          then (v_phase1_result->>'shifts_updated')::int
          else null
        end;

      if v_mode_b_should_run_phase15 then
        create temporary table tmp_phase15_rows on commit drop as
        select *
        from public.weekly_import_apply_phase2(p_import_id := p_import_id, p_system_type := 'HR_WEEKLY');

        select count(*)::int
        into v_phase15_ok
        from tmp_phase15_rows r
        where upper(coalesce(r.action::text,'')) = 'OK';

        select count(*)::int
        into v_phase15_updated
        from tmp_phase15_rows r
        where coalesce(r.shift_updated,false) is true;

        v_mode_b_phase15_called := true;
      end if;
    end if;

    v_steps := v_steps || jsonb_build_array(
      jsonb_build_object(
        'step','PHASE1_PHASE15_DONE',
        'phase1_called', v_mode_b_phase1_called,
        'phase15_called', v_mode_b_phase15_called,
        'phase1_shifts_created', v_phase1_shifts_created,
        'phase1_shifts_updated', v_phase1_shifts_updated,
        'phase15_ok_rows', v_phase15_ok,
        'phase15_shift_updated_rows', v_phase15_updated
      )
    );

    if v_mode_b_should_run_cancellations then
      create temporary table tmp_cancel_meta on commit drop as
      select
        ns.id as shift_id,
        ns.candidate_id,
        ns.client_id,
        ns.work_date
      from public.nhsp_shifts ns
      where ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]));

      create temporary table tmp_selected_rep_keys_text on commit drop as
      select distinct rk.replacement_day_key
      from tmp_selected_replacement_keys rk;

      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'shift_id', cm.shift_id::text,
            'reason',
              case
                when exists (
                  select 1
                  from tmp_selected_rep_keys_text sr
                  where sr.replacement_day_key = (cm.candidate_id::text || '|' || cm.client_id::text || '|' || cm.work_date::text)
                ) then 'REPLACEMENT_DAY'
                else 'MISSING_FROM_IMPORT'
              end
          )
        ),
        '[]'::jsonb
      )
      into v_cancel_actions
      from tmp_cancel_meta cm;

      select public.weekly_import_apply_cancellations(
        p_import_id := p_import_id,
        p_actions := v_cancel_actions,
        p_actor_user_id := p_actor_user_id
      )
      into v_cancellations_result;

      v_mode_b_cancellations_called := true;
    end if;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','CANCELLATIONS_DONE'));

    -- ─────────────────────────────────────────────
    -- 8.5) ENSURE BASE WEEKLY TIMESHEET + ATTACH ACTIVE HEALTHROSTER MODE_B SHIFTS
    --
    -- Policy X guardrail:
    --   * This block runs only for HEALTHROSTER MODE_B / import-authoritative groups.
    --   * It creates/reuses the canonical weekly timesheet container and links active
    --     imported shifts to that container.
    --   * It does not use imported shift identifiers as Banking Pay economic keys.
    --   * It refuses to silently relink protected detached rows.
    -- ─────────────────────────────────────────────
    v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','HR_MODE_B_ENSURE_BASE_WEEKLY_START'));

    drop table if exists pg_temp.tmp_hr_mode_b_groups;
    create temporary table tmp_hr_mode_b_groups(
      contract_id uuid not null,
      candidate_id uuid not null,
      client_id uuid not null,
      week_ending_date date not null,
      primary key (contract_id, candidate_id, client_id, week_ending_date)
    ) on commit drop;

    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      p2ok.contract_id,
      p2ok.candidate_id,
      p2ok.client_id,
      p2ok.week_ending_date
    from tmp_p2_ok_mode p2ok
    where p2ok.mode = 'MODE_B'
      and p2ok.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
      and p2ok.contract_id is not null
      and p2ok.candidate_id is not null
      and p2ok.client_id is not null
      and p2ok.week_ending_date is not null
    on conflict do nothing;

    if array_length(v_selected_cancel_shift_ids, 1) is not null then
      insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
      select distinct
        ns.contract_id,
        ns.candidate_id,
        ns.client_id,
        ns.week_ending_date
      from public.nhsp_shifts ns
      join public.contracts c_cancel on c_cancel.id = ns.contract_id
      cross join lateral public._import_review_effective_authority_core_v1(
        'HR_WEEKLY',c_cancel.id,c_cancel.client_id,ns.week_ending_date) a_cancel
      where ns.id = any(v_selected_cancel_shift_ids)
        and ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns.client_id = v_import_client_id
        and ns.contract_id is not null
        and ns.candidate_id is not null
        and ns.client_id is not null
        and ns.week_ending_date is not null
        and a_cancel.import_authoritative
      on conflict do nothing;
    end if;

    -- Include all active HealthRoster shifts in the affected MODE_B groups.  This
    -- makes the transaction repair the whole canonical week bucket, not just the
    -- single selected row, while staying inside the MODE_B classification.
    insert into tmp_hr_mode_b_groups(contract_id, candidate_id, client_id, week_ending_date)
    select distinct
      ns.contract_id,
      ns.candidate_id,
      ns.client_id,
      ns.week_ending_date
    from public.nhsp_shifts ns
    join tmp_group_mode gm2
      on gm2.contract_id = ns.contract_id
     and gm2.candidate_id = ns.candidate_id
     and gm2.client_id = ns.client_id
     and gm2.week_ending_date = ns.week_ending_date
     and gm2.mode = 'MODE_B'
    where ns.source_system = 'HEALTHROSTER'::public.hr_source_enum
      and ns.cancelled_at_utc is null
      and ns.contract_id is not null
      and ns.candidate_id is not null
      and ns.client_id is not null
      and ns.week_ending_date is not null
      and (
        ns.external_row_key = any(coalesce(v_force_keys_final, array[]::text[]))
        or ns.id = any(coalesce(v_selected_cancel_shift_ids, array[]::uuid[]))
        or exists (
          select 1
          from tmp_hr_mode_b_groups g0
          where g0.contract_id = ns.contract_id
            and g0.candidate_id = ns.candidate_id
            and g0.client_id = ns.client_id
            and g0.week_ending_date = ns.week_ending_date
        )
      )
    on conflict do nothing;

    select count(*)::int
    into v_ensure_pairs_count
    from tmp_hr_mode_b_groups g;

    select coalesce(jsonb_agg(jsonb_build_object(
      'contract_id', g.contract_id::text,
      'candidate_id', g.candidate_id::text,
      'client_id', g.client_id::text,
      'week_ending_date', g.week_ending_date::text
    )), '[]'::jsonb)
    into v_ensure_sample_pairs
    from (
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
      limit 20
    ) as g;

    drop table if exists pg_temp.tmp_hr_mode_b_created_ts_ids;
    create temporary table tmp_hr_mode_b_created_ts_ids(
      timesheet_id uuid primary key
    ) on commit drop;

    drop table if exists pg_temp.tmp_hr_mode_b_protected_shift_ids;
    create temporary table tmp_hr_mode_b_protected_shift_ids(
      shift_id uuid primary key,
      reason text not null
    ) on commit drop;

    for v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date in
      select g.contract_id, g.candidate_id, g.client_id, g.week_ending_date
      from tmp_hr_mode_b_groups g
      order by g.contract_id::text, g.candidate_id::text, g.client_id::text, g.week_ending_date::text
    loop
      select count(*)::int
      into v_active_count
      from public.nhsp_shifts ns_active
      where ns_active.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_active.cancelled_at_utc is null
        and ns_active.contract_id = v_pair_contract_id
        and ns_active.candidate_id = v_pair_candidate_id
        and ns_active.client_id = v_pair_client_id
        and ns_active.week_ending_date = v_pair_week_ending_date;

      if coalesce(v_active_count, 0) <= 0 then
        v_ensure_pairs_skipped_no_active := v_ensure_pairs_skipped_no_active + 1;
        continue;
      end if;

      v_base_week_id := null;
      v_base_week_ts_id := null;

      select cw0.id, cw0.timesheet_id
      into v_base_week_id, v_base_week_ts_id
      from public.contract_weeks cw0
      where cw0.contract_id = v_pair_contract_id
        and cw0.week_ending_date = v_pair_week_ending_date
        and cw0.is_adjustment is false
        and coalesce(cw0.additional_seq, 0) = 0
      limit 1
      for update;

      if v_base_week_id is null then
        insert into public.contract_weeks(
          contract_id,
          week_ending_date,
          additional_seq,
          status,
          submission_mode_snapshot,
          timesheet_id,
          planned_schedule_json,
          created_at,
          updated_at,
          is_adjustment
        )
        values (
          v_pair_contract_id,
          v_pair_week_ending_date,
          0,
          'SUBMITTED'::public.contract_week_status_enum,
          'MANUAL'::public.submission_mode_enum,
          null,
          null,
          v_now,
          v_now,
          false
        )
        returning id into v_base_week_id;

        v_ensure_base_week_created_count := v_ensure_base_week_created_count + 1;
        v_base_week_ts_id := null;
      else
        v_ensure_base_week_existing_count := v_ensure_base_week_existing_count + 1;
      end if;

      if v_base_week_ts_id is not null then
        select exists(
          select 1
          from public.timesheets tchk
          where tchk.timesheet_id = v_base_week_ts_id
            and tchk.is_current is true
            and tchk.revoked_at is null
          limit 1
        )
        into v_ts_exists;

        if v_ts_exists is not true then
          update public.contract_weeks cw0u
          set
            timesheet_id = null,
            updated_at = v_now
          where cw0u.id = v_base_week_id;

          v_ensure_timesheet_missing_reference_count := v_ensure_timesheet_missing_reference_count + 1;
          v_base_week_ts_id := null;
        end if;
      end if;

      select ct.display_site, ct.ward_hint, ct.role
      into v_contract_display_site, v_contract_ward_hint, v_contract_role
      from public.contracts ct
      where ct.id = v_pair_contract_id
      limit 1;

      select cand.display_name, cand.tms_ref
      into v_candidate_display_name, v_candidate_tms_ref
      from public.candidates cand
      where cand.id = v_pair_candidate_id
      limit 1;

      select cli.name
      into v_client_name
      from public.clients cli
      where cli.id = v_pair_client_id
      limit 1;

      v_occupant_norm := lower(coalesce(v_candidate_tms_ref, v_candidate_display_name, v_pair_candidate_id::text));
      v_hospital_norm := lower(coalesce(v_contract_display_site, v_client_name, v_pair_client_id::text));
      v_ward_norm := lower(coalesce(v_contract_ward_hint, 'contract'));
      v_role_norm := lower(coalesce(v_contract_role, 'weekly'));

      v_shift_label_norm := 'weekly-0';

      v_booking_base :=
        v_occupant_norm || '|' ||
        v_pair_week_ending_date::text || '|' ||
        v_hospital_norm || '|' ||
        v_ward_norm || '|' ||
        v_role_norm || '|' ||
        v_shift_label_norm;

      v_hash_hex := encode(extensions.digest(convert_to(v_booking_base, 'utf8'), 'sha256'::text), 'hex');
      v_booking_id := 'bk_' || substr(v_hash_hex, 1, 16);

      if v_base_week_ts_id is null then
        v_new_ts_id := null;

        insert into public.timesheets(
          booking_id,
          version,
          is_current,
          status,

          sheet_scope,
          submission_mode,
          line_type,
          authorised_at_server,

          occupant_key_norm,
          hospital_norm,
          ward_norm,
          job_title_norm,
          shift_label_norm,

          week_ending_date,
          contract_id,

          manual_pdf_r2_key,
          actual_schedule_json,

          qr_payload_json,
          candidate_hint_text,

          is_adjustment,
          parent_timesheet_id,
          correction_id,
          correction_kind,
          adjustment_origin,

          created_at,
          updated_at
        )
        values (
          v_booking_id,
          1,
          true,
          'RECEIVED'::public.timesheet_status_enum,

          'WEEKLY'::public.timesheet_scope_enum,
          'MANUAL'::public.submission_mode_enum,
          'HOURS'::public.timesheet_line_type_enum,
          null,

          v_occupant_norm,
          v_hospital_norm,
          v_ward_norm,
          v_role_norm,
          v_shift_label_norm,

          v_pair_week_ending_date,
          v_pair_contract_id,

          null,
          '[]'::jsonb,

          '{}'::jsonb,
          null,

          false,
          null,
          null,
          null,
          null,

          v_now,
          v_now
        )
        returning timesheet_id into v_new_ts_id;

        v_ensure_timesheet_created_count := v_ensure_timesheet_created_count + 1;
        v_base_week_ts_id := v_new_ts_id;

        insert into tmp_hr_mode_b_created_ts_ids(timesheet_id)
        values (v_new_ts_id)
        on conflict do nothing;

        update public.contract_weeks cw0link
        set
          timesheet_id = v_new_ts_id,
          status = case
            when cw0link.status = 'AUTHORISED'::public.contract_week_status_enum then cw0link.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0link.id = v_base_week_id;

        perform public._audit_insert(
          'timesheets',
          v_new_ts_id::text,
          'HEALTHROSTER_IMPORT_TIMESHEET_CREATED',
          null,
          jsonb_build_object(
            'import_id', p_import_id::text,
            'source_system', 'HEALTHROSTER',
            'mode', 'MODE_B',
            'kind', 'BASE_WEEKLY',
            'contract_id', v_pair_contract_id::text,
            'contract_week_id', v_base_week_id::text,
            'candidate_id', v_pair_candidate_id::text,
            'client_id', v_pair_client_id::text,
            'week_ending_date', v_pair_week_ending_date::text,
            'booking_id', v_booking_id,
            'active_shifts_count', v_active_count
          ),
          'IMPORT_BIRTH',
          p_actor_user_id
        );
      else
        v_ensure_timesheet_reused_count := v_ensure_timesheet_reused_count + 1;

        update public.contract_weeks cw0keep
        set
          status = case
            when cw0keep.status = 'AUTHORISED'::public.contract_week_status_enum then cw0keep.status
            else 'SUBMITTED'::public.contract_week_status_enum
          end,
          submission_mode_snapshot = 'MANUAL'::public.submission_mode_enum,
          updated_at = v_now
        where cw0keep.id = v_base_week_id;

        if exists (
          select 1
          from public.timesheets identity_target
          where identity_target.timesheet_id=v_base_week_ts_id
            and (
              identity_target.week_ending_date is distinct from v_pair_week_ending_date
              or identity_target.contract_id is distinct from v_pair_contract_id
              or identity_target.occupant_key_norm is distinct from v_occupant_norm
              or identity_target.hospital_norm is distinct from v_hospital_norm
              or identity_target.ward_norm is distinct from v_ward_norm
              or identity_target.job_title_norm is distinct from v_role_norm
            )
        ) then
          select public.import_timesheet_financial_preflight_v1(
            p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
            p_action := 'IMPORT_FINANCIAL_IDENTITY_CHANGE',
            p_actor_user_id := p_actor_user_id,
            p_expected_state_json := '{}'::jsonb,
            p_lock_rows := true,
            p_max_scope := 100
          ) into v_changed_preflight;

          if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
            raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
          end if;

          if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
            raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
              )::text;
          elsif v_changed_preflight->>'required_path'='UNAUTHORISE_AMEND_RECALCULATE_REAUTHORISE' then
            raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
                'paid_uninvoiced_rollover_required',false
              )::text;
          elsif v_changed_preflight->>'required_path'='PAID_UNINVOICED_ROLLOVER'
            and not exists (
              select 1 from public.timesheets_financials rollover_identity
              where rollover_identity.timesheet_id=v_base_week_ts_id
                and rollover_identity.is_current=true
                and rollover_identity.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
                and coalesce((rollover_identity.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
            ) then
            raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
              detail=jsonb_build_object(
                'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
                'reason','FINANCIAL_IDENTITY_CHANGE',
                'required_path',jsonb_build_array(
                  'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
                ),
                'invoice_policy_without_history','NOW'
              )::text;
          end if;
        end if;

        update public.timesheets tnorm
        set
          is_current = true,
          status = 'RECEIVED'::public.timesheet_status_enum,
          sheet_scope = 'WEEKLY'::public.timesheet_scope_enum,
          submission_mode = 'MANUAL'::public.submission_mode_enum,
          line_type = 'HOURS'::public.timesheet_line_type_enum,
          week_ending_date = v_pair_week_ending_date,
          contract_id = v_pair_contract_id,
          occupant_key_norm = v_occupant_norm,
          hospital_norm = v_hospital_norm,
          ward_norm = v_ward_norm,
          job_title_norm = v_role_norm,
          shift_label_norm = v_shift_label_norm,
          updated_at = v_now
        where tnorm.timesheet_id = v_base_week_ts_id;
      end if;

      truncate table tmp_hr_mode_b_protected_shift_ids;

      insert into tmp_hr_mode_b_protected_shift_ids(shift_id, reason)
      select distinct
        ns_lock.id,
        case
          when ns_lock.invoice_id is not null then 'SHIFT_INVOICED'
          when tf_lock.timesheet_id is not null then 'TIMESHEET_FINANCIALS_LOCKED_OR_PAID'
          when pbi_lock.timesheet_id is not null then 'PAY_BATCH_ITEM_EXISTS'
          when t_lock.timesheet_id is not null then 'CORRECTION_OR_ADJUSTMENT_OWNED_TIMESHEET'
          else 'PROTECTED_DETACHED_OR_INVALID_LINK'
        end as reason
      from public.nhsp_shifts ns_lock
      left join public.timesheets_financials tf_lock
        on tf_lock.timesheet_id = ns_lock.timesheet_id
       and tf_lock.is_current is true
       and (
         tf_lock.locked_by_invoice_id is not null
         or tf_lock.paid_at_utc is not null
       )
      left join public.pay_batch_items pbi_lock
        on pbi_lock.timesheet_id = ns_lock.timesheet_id
       and coalesce(pbi_lock.is_voided, false) is false
      left join public.timesheets t_lock
        on t_lock.timesheet_id = ns_lock.timesheet_id
       and (
         coalesce(t_lock.is_adjustment, false) is true
         or t_lock.parent_timesheet_id is not null
         or t_lock.correction_id is not null
         or t_lock.correction_kind is not null
         or t_lock.adjustment_origin is not null
       )
      where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and ns_lock.cancelled_at_utc is null
        and ns_lock.contract_id = v_pair_contract_id
        and ns_lock.candidate_id = v_pair_candidate_id
        and ns_lock.client_id = v_pair_client_id
        and ns_lock.week_ending_date = v_pair_week_ending_date
        and (
          ns_lock.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tvalid
            where tvalid.timesheet_id = ns_lock.timesheet_id
              and tvalid.is_current is true
              and tvalid.revoked_at is null
            limit 1
          )
        )
        and (
          ns_lock.invoice_id is not null
          or tf_lock.timesheet_id is not null
          or pbi_lock.timesheet_id is not null
          or t_lock.timesheet_id is not null
        )
      on conflict do nothing;

      select count(*)::int
      into v_active_count
      from tmp_hr_mode_b_protected_shift_ids p;

      if coalesce(v_active_count, 0) > 0 then
        raise exception
          'hr_weekly_apply_transactional: protected active HEALTHROSTER MODE_B shifts are detached or linked to an invalid timesheet; refusing silent relink. contract_id=% candidate_id=% client_id=% week_ending_date=% protected_count=% sample=%',
          v_pair_contract_id,
          v_pair_candidate_id,
          v_pair_client_id,
          v_pair_week_ending_date,
          v_active_count,
          (
            select coalesce(jsonb_agg(jsonb_build_object('shift_id', p2.shift_id::text, 'reason', p2.reason)), '[]'::jsonb)
            from (
              select p.shift_id, p.reason
              from tmp_hr_mode_b_protected_shift_ids p
              order by p.shift_id::text
              limit 10
            ) p2
          );
      end if;

      if exists (
        select 1 from public.nhsp_shifts ns_scope
        where ns_scope.source_system = 'HEALTHROSTER'::public.hr_source_enum
            and ns_scope.cancelled_at_utc is null
            and ns_scope.contract_id = v_pair_contract_id
            and ns_scope.candidate_id = v_pair_candidate_id
            and ns_scope.client_id = v_pair_client_id
            and ns_scope.week_ending_date = v_pair_week_ending_date
            and (
              ns_scope.timesheet_id is null
              or not exists (
                select 1 from public.timesheets existing_link
                where existing_link.timesheet_id=ns_scope.timesheet_id
                  and existing_link.is_current=true
                  and existing_link.revoked_at is null
              )
            )
            and not exists (
              select 1 from tmp_hr_mode_b_protected_shift_ids protected
              where protected.shift_id=ns_scope.id
            )
      ) then
        select public.import_timesheet_financial_preflight_v1(
          p_timesheet_ids := array[v_base_week_ts_id]::uuid[],
          p_action := 'IMPORT_SOURCE_ASSIGNMENT',
          p_actor_user_id := p_actor_user_id,
          p_expected_state_json := '{}'::jsonb,
          p_lock_rows := true,
          p_max_scope := 100
        ) into v_changed_preflight;

        if coalesce((v_changed_preflight->>'allowed')::boolean,false) is not true then
          raise exception using message='IMPORT_FINANCIAL_PREFLIGHT_BLOCKED',errcode='P0001',detail=v_changed_preflight::text;
        end if;

        if v_changed_preflight->>'required_path'='CREATE_OR_UPDATE_CORRECTION_CHAIN' then
          raise exception using message='IMPORT_INVOICED_CORRECTION_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','IMPORT_INVOICED_CORRECTION_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path','CREATE_OR_UPDATE_CORRECTION_CHAIN'
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets source_target
          left join public.timesheets_financials source_target_tf
            on source_target_tf.timesheet_id=source_target.timesheet_id and source_target_tf.is_current=true
          where source_target.timesheet_id=v_base_week_ts_id
            and (source_target.authorised_at_server is not null or source_target_tf.authorised_at_utc is not null)
            and not exists (
              select 1 from public.timesheets_financials paid_target
              where paid_target.timesheet_id=source_target.timesheet_id
                and paid_target.paid_at_utc is not null
            )
        ) then
          raise exception using message='CANONICAL_UNAUTHORISE_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','CANONICAL_UNAUTHORISE_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array('UNAUTHORISE','AMEND','RECALCULATE','REAUTHORISE'),
              'paid_uninvoiced_rollover_required',false
            )::text;
        end if;

        if exists (
          select 1 from public.timesheets_financials paid_source
          where paid_source.timesheet_id=v_base_week_ts_id and paid_source.paid_at_utc is not null
        ) and not exists (
          select 1 from public.timesheets_financials rollover_source
          where rollover_source.timesheet_id=v_base_week_ts_id and rollover_source.is_current=true
            and rollover_source.stale_reason='IMPORT_PAID_TSFIN_ROLLOVER_PENDING_CALCULATION'
            and coalesce((rollover_source.policy_snapshot_json->>'requires_frozen_correction_policy')::boolean,false)=true
        ) then
          raise exception using message='PAID_UNINVOICED_ROLLOVER_REQUIRED',errcode='P0001',
            detail=jsonb_build_object(
              'code','PAID_UNINVOICED_ROLLOVER_REQUIRED','timesheet_id',v_base_week_ts_id,
              'reason','FINANCIAL_SOURCE_ASSIGNMENT_CHANGE',
              'required_path',jsonb_build_array(
                'UNAUTHORISE','PAID_UNINVOICED_ROLLOVER','AMEND','RECALCULATE','REAUTHORISE'
              ),
              'invoice_policy_without_history','NOW'
            )::text;
        end if;
      end if;

      update public.nhsp_shifts nsu0
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu0.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu0.cancelled_at_utc is null
        and nsu0.contract_id = v_pair_contract_id
        and nsu0.candidate_id = v_pair_candidate_id
        and nsu0.client_id = v_pair_client_id
        and nsu0.week_ending_date = v_pair_week_ending_date
        and nsu0.timesheet_id is null
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p
          where p.shift_id = nsu0.id
        );

      get diagnostics v_attached_null_count = row_count;
      v_ensure_shifts_attached_count := v_ensure_shifts_attached_count + coalesce(v_attached_null_count, 0);

      update public.nhsp_shifts nsu1
      set
        timesheet_id = v_base_week_ts_id,
        updated_at = v_now
      where nsu1.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nsu1.cancelled_at_utc is null
        and nsu1.contract_id = v_pair_contract_id
        and nsu1.candidate_id = v_pair_candidate_id
        and nsu1.client_id = v_pair_client_id
        and nsu1.week_ending_date = v_pair_week_ending_date
        and nsu1.timesheet_id is not null
        and not exists (
          select 1
          from public.timesheets tvalid2
          where tvalid2.timesheet_id = nsu1.timesheet_id
            and tvalid2.is_current is true
            and tvalid2.revoked_at is null
          limit 1
        )
        and not exists (
          select 1
          from tmp_hr_mode_b_protected_shift_ids p2
          where p2.shift_id = nsu1.id
        );

      get diagnostics v_relinked_invalid_count = row_count;
      v_ensure_shifts_relinked_invalid_ts_count := v_ensure_shifts_relinked_invalid_ts_count + coalesce(v_relinked_invalid_count, 0);

      select count(*)::int
      into v_active_count
      from public.nhsp_shifts nscheck
      where nscheck.source_system = 'HEALTHROSTER'::public.hr_source_enum
        and nscheck.cancelled_at_utc is null
        and nscheck.contract_id = v_pair_contract_id
        and nscheck.candidate_id = v_pair_candidate_id
        and nscheck.client_id = v_pair_client_id
        and nscheck.week_ending_date = v_pair_week_ending_date
        and (
          nscheck.timesheet_id is null
          or not exists (
            select 1
            from public.timesheets tchk2
            where tchk2.timesheet_id = nscheck.timesheet_id
              and tchk2.is_current is true
              and tchk2.revoked_at is null
            limit 1
          )
        );

      if coalesce(v_active_count, 0) > 0 then
        v_ensure_remaining_active_detached_count := v_ensure_remaining_active_detached_count + v_active_count;
        raise exception
          'hr_weekly_apply_transactional: ENSURE invariant failed (active HEALTHROSTER MODE_B shifts remain detached or linked to missing/non-current/revoked timesheets) contract_id=% candidate_id=% client_id=% week_ending_date=% remaining=%.',
          v_pair_contract_id, v_pair_candidate_id, v_pair_client_id, v_pair_week_ending_date, v_active_count;
      end if;

      insert into tmp_aff_ts(timesheet_id)
      values (v_base_week_ts_id)
      on conflict do nothing;
    end loop;

    select coalesce(jsonb_agg(x.ts_id), '[]'::jsonb)
    into v_ensure_sample_created_ts_ids
    from (
      select tct.timesheet_id::text as ts_id
      from tmp_hr_mode_b_created_ts_ids tct
      order by tct.timesheet_id::text
      limit 20
    ) as x;

    v_steps := v_steps || jsonb_build_array(jsonb_build_object(
      'step','HR_MODE_B_ENSURE_BASE_WEEKLY_DONE',
      'ensure_pairs_count', v_ensure_pairs_count,
      'ensure_pairs_skipped_no_active', v_ensure_pairs_skipped_no_active,
      'base_week_created_count', v_ensure_base_week_created_count,
      'base_week_existing_count', v_ensure_base_week_existing_count,
      'base_timesheet_created_count', v_ensure_timesheet_created_count,
      'base_timesheet_reused_count', v_ensure_timesheet_reused_count,
      'missing_timesheet_reference_count', v_ensure_timesheet_missing_reference_count,
      'shifts_attached_null_count', v_ensure_shifts_attached_count,
      'shifts_relinked_invalid_ts_count', v_ensure_shifts_relinked_invalid_ts_count,
      'remaining_active_detached_count', v_ensure_remaining_active_detached_count,
      'sample_pairs', v_ensure_sample_pairs,
      'sample_created_ts_ids', v_ensure_sample_created_ts_ids
    ));
  end if;

  -- ─────────────────────────────────────────────
  -- 9) MODE_A mirror ingestion
  -- ─────────────────────────────────────────────
  if array_length(v_mode_a_external_keys, 1) is not null then
    select public.hr_weekly_mirror_upsert_deterministic(
      p_import_id := p_import_id,
      p_external_row_keys := v_mode_a_external_keys,
      p_actor_user_id := p_actor_user_id
    )
    into v_mirror_result;
  end if;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','MODE_A_MIRROR_DONE'));

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A shift→timesheet linking
  -- ─────────────────────────────────────────────
  create temporary table tmp_mode_a_ts_map(
    external_row_key text primary key,
    timesheet_id uuid not null
  ) on commit drop;

  insert into tmp_mode_a_ts_map(external_row_key, timesheet_id)
  select distinct
    p2m.external_row_key,
    cw0.timesheet_id
  from tmp_p2_ok_mode p2m
  join public.contract_weeks cw0
    on cw0.contract_id = p2m.contract_id
   and cw0.week_ending_date = p2m.week_ending_date
   and cw0.is_adjustment is false
   and coalesce(cw0.additional_seq, 0) = 0
  where p2m.mode = 'MODE_A'
    and p2m.external_row_key = any(coalesce(v_mode_a_external_keys,array[]::text[]))
    and p2m.external_row_key is not null
    and cw0.timesheet_id is not null
  on conflict do nothing;

  select count(*)::int
  into v_mode_a_ts_linked_count
  from tmp_mode_a_ts_map mt;

  create temporary table tmp_mode_a_locked_shift_ids(
    shift_id uuid primary key
  ) on commit drop;

  insert into tmp_mode_a_locked_shift_ids(shift_id)
  select distinct ns_lock.id as shift_id
  from public.nhsp_shifts ns_lock
  join tmp_mode_a_ts_map mt_lock
    on mt_lock.external_row_key = ns_lock.external_row_key
  where ns_lock.source_system = 'HEALTHROSTER'::public.hr_source_enum
    and ns_lock.cancelled_at_utc is null
    and ns_lock.latest_import_id = p_import_id
    and (
      ns_lock.invoice_id is not null
      or exists (
        select 1
        from public.timesheets_financials tf_lock
        cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg_lock(value)
        where tf_lock.is_current = true
          and tf_lock.timesheet_id = ns_lock.timesheet_id
          and nullif(btrim(seg_lock.value->>'nhsp_shift_id'), '') = ns_lock.id::text
          and nullif(btrim(seg_lock.value->>'invoice_locked_invoice_id'), '') is not null
        limit 1
      )
    )
  on conflict do nothing;

  update public.nhsp_shifts nsu
     set timesheet_id = mt.timesheet_id,
         updated_at = v_now
    from tmp_mode_a_ts_map mt
   where nsu.source_system = 'HEALTHROSTER'::public.hr_source_enum
     and nsu.cancelled_at_utc is null
     and nsu.latest_import_id = p_import_id
     and nsu.external_row_key = mt.external_row_key
     and (nsu.timesheet_id is distinct from mt.timesheet_id)
     and not exists (
       select 1
       from tmp_mode_a_locked_shift_ids l
       where l.shift_id = nsu.id
     );

  get diagnostics v_mode_a_shifts_attached_count = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mt2.timesheet_id
  from tmp_mode_a_ts_map mt2
  where mt2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_SHIFTS_LINKED',
      'mode_a_ts_linked_count', v_mode_a_ts_linked_count,
      'mode_a_shifts_attached_count', v_mode_a_shifts_attached_count,
      'mode_a_locked_shift_count', (select count(*)::int from tmp_mode_a_locked_shift_ids)
    )
  );

  -- ─────────────────────────────────────────────
  -- 10) MODE_A weekly validation upserts + email state
  -- ─────────────────────────────────────────────
  select public.hr_weekly_validation_preview(p_import_id := p_import_id)
  into v_weekly_val_payload;

  if v_weekly_val_payload is null or jsonb_typeof(v_weekly_val_payload) <> 'object' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview returned non-object payload.';
  end if;

  if jsonb_typeof(v_weekly_val_payload->'rows') <> 'array' then
    raise exception 'hr_weekly_apply_transactional: hr_weekly_validation_preview payload missing rows array.';
  end if;

  create temporary table tmp_val_rows on commit drop as
  select
    nullif(btrim(r.value->>'timesheet_id'), '')::uuid as timesheet_id,
    nullif(btrim(r.value->>'candidate_id'), '')::uuid as candidate_id,
    nullif(btrim(r.value->>'contract_id'), '')::uuid as contract_id,
    nullif(btrim(r.value->>'week_ending_date'), '')::date as week_ending_date,
    nullif(btrim(r.value->>'client_id'), '')::uuid as client_id,
    upper(coalesce(r.value->>'overall_status','')) as overall_status,
    (lower(coalesce(r.value->>'has_mismatch','false')) in ('true','1')) as has_mismatch,
    r.value as row_json
  from jsonb_array_elements(v_weekly_val_payload->'rows') as r(value)
  where nullif(btrim(r.value->>'timesheet_id'), '') is not null
    and nullif(btrim(r.value->>'candidate_id'), '') is not null
    and nullif(btrim(r.value->>'contract_id'), '') is not null
    and nullif(btrim(r.value->>'week_ending_date'), '') is not null
    and nullif(btrim(r.value->>'client_id'), '') is not null
    and exists (
      select 1 from tmp_mode_a_eligible_groups eligible
      where eligible.candidate_id=nullif(btrim(r.value->>'candidate_id'), '')::uuid
        and eligible.client_id=nullif(btrim(r.value->>'client_id'), '')::uuid
        and eligible.contract_id=nullif(btrim(r.value->>'contract_id'), '')::uuid
        and eligible.week_ending_date=nullif(btrim(r.value->>'week_ending_date'), '')::date
    );

  select count(*)::int
  into v_val_rows_count
  from tmp_val_rows;

  create temporary table tmp_val_mode on commit drop as
  select
    vr.timesheet_id,
    case aval.authority_mode when 'VALIDATION_ONLY' then 'MODE_A'
      when 'AUTHORITATIVE' then 'MODE_B' else 'OUT_OF_SCOPE' end as mode
  from tmp_val_rows vr
  join public.contracts cval
    on cval.id = vr.contract_id
  cross join lateral public._import_review_effective_authority_core_v1(
    'HR_WEEKLY',cval.id,cval.client_id,vr.week_ending_date) aval;

  if exists(select 1 from tmp_val_mode where mode='OUT_OF_SCOPE') then
    raise exception 'HR_WEEKLY_VALIDATION_AUTHORITY_OUT_OF_SCOPE' using errcode='40001';
  end if;

  create temporary table tmp_invalidation_actions(
    timesheet_id uuid not null,
    comparison_key text not null,
    invalidate boolean not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  if v_invalidation_actions_count > 0 then
    insert into tmp_invalidation_actions(timesheet_id, comparison_key, invalidate)
    select
      nullif(btrim(a.value->>'timesheet_id'), '')::uuid as timesheet_id,
      nullif(btrim(a.value->>'comparison_key'), '') as comparison_key,
      (lower(coalesce(a.value->>'invalidate','true')) in ('true','1')) as invalidate
    from jsonb_array_elements(v_invalidation_actions) as a(value)
    where nullif(btrim(a.value->>'timesheet_id'), '') is not null
      and nullif(btrim(a.value->>'comparison_key'), '') is not null
    on conflict (timesheet_id, comparison_key) do update
      set invalidate = excluded.invalidate;
  end if;

  create temporary table tmp_mode_a_missing_ref_clear(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_before text null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_missing_ref_clear(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_before)
  select distinct
    vr.timesheet_id,
    nullif(btrim(coalesce(cx.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(cx.value->>'ref_before'), '') as ref_before
  from tmp_val_rows vr
  join tmp_val_mode vmc
    on vmc.timesheet_id = vr.timesheet_id
   and vmc.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vr.row_json->'comparisons', '[]'::jsonb)) as cx(value)
  left join tmp_invalidation_actions ia
    on ia.timesheet_id = vr.timesheet_id
   and ia.comparison_key = nullif(btrim(coalesce(cx.value->>'comparison_key','')), '')
  where upper(coalesce(cx.value->>'match_status','')) in ('UNMATCHED','MISMATCH')
    and (lower(coalesce(cx.value->>'invoice_locked','false')) in ('true','1')) is false
    and nullif(btrim(coalesce(cx.value->>'invoice_locked_invoice_id','')), '') is null
    and nullif(btrim(coalesce(cx.value->>'ref_before','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx.value->>'work_date','')), '') is not null
    and ia.timesheet_id is not null
    and ia.invalidate is true
  on conflict (timesheet_id, comparison_key) do nothing;

  -- ✅ Capture exact timesheets whose refs were cleared (for post-apply QR reissue + regen)
  drop table if exists pg_temp.tmp_mode_a_ref_clear_upd;
  create temporary table tmp_mode_a_ref_clear_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsclr
       set ref_num = null,
           hr_request_id = null,
           updated_at = v_now
      from tmp_mode_a_missing_ref_clear mrc
      left join public.timesheets_financials tfc
        on tfc.timesheet_id = mrc.timesheet_id
       and tfc.is_current = true
     where nsclr.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsclr.cancelled_at_utc is null
       and nsclr.timesheet_id = mrc.timesheet_id
       and nsclr.work_date = mrc.work_date
       and nsclr.ref_num is not null
       and nsclr.invoice_id is null
       and (tfc.timesheet_id is null or (tfc.locked_by_invoice_id is null and tfc.paid_at_utc is null))
       and to_char((date_trunc('minute', nsclr.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_start_hhmm
       and to_char((date_trunc('minute', nsclr.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrc.ts_end_hhmm
       and coalesce(nsclr.break_mins,0) = coalesce(mrc.ts_break_mins,0)
       and not exists (
         select 1
         from public.timesheets_financials tf_lock
         cross join lateral jsonb_array_elements(coalesce(tf_lock.invoice_breakdown_json->'segments','[]'::jsonb)) as seg(value)
         where tf_lock.is_current = true
           and tf_lock.timesheet_id = nsclr.timesheet_id
           and nullif(btrim(seg.value->>'nhsp_shift_id'), '') = nsclr.id::text
           and nullif(btrim(seg.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsclr.timesheet_id
  )
  insert into tmp_mode_a_ref_clear_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_cleared_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_clear_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_clear_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct mrc2.timesheet_id
  from tmp_mode_a_missing_ref_clear mrc2
  where mrc2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MISSING_SHIFT_REF_CLEARED',
      'ref_cleared_count', v_mode_a_ref_cleared_count,
      'invalidation_actions_count', v_invalidation_actions_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ MODE_A matched ref propagation (Request Id / booking reference)
  -- ─────────────────────────────────────────────
  drop table if exists pg_temp.tmp_mode_a_ref_set;
  create temporary table tmp_mode_a_ref_set(
    timesheet_id uuid not null,
    comparison_key text not null,
    work_date date not null,
    ts_start_hhmm text not null,
    ts_end_hhmm text not null,
    ts_break_mins int not null,
    ref_after text not null,
    primary key (timesheet_id, comparison_key)
  ) on commit drop;

  insert into tmp_mode_a_ref_set(timesheet_id, comparison_key, work_date, ts_start_hhmm, ts_end_hhmm, ts_break_mins, ref_after)
  select distinct
    vrm.timesheet_id,
    nullif(btrim(coalesce(cx2.value->>'comparison_key','')), '') as comparison_key,
    nullif(btrim(cx2.value->>'work_date'), '')::date as work_date,
    nullif(btrim(cx2.value->>'timesheet_start'), '') as ts_start_hhmm,
    nullif(btrim(cx2.value->>'timesheet_end'), '') as ts_end_hhmm,
    coalesce(nullif(btrim(cx2.value->>'timesheet_break_mins'), '')::int, 0) as ts_break_mins,
    nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') as ref_after
  from tmp_val_rows vrm
  join tmp_val_mode vmm
    on vmm.timesheet_id = vrm.timesheet_id
   and vmm.mode = 'MODE_A'
  cross join lateral jsonb_array_elements(coalesce(vrm.row_json->'comparisons', '[]'::jsonb)) as cx2(value)
  where vrm.timesheet_id is not null
    and (
      upper(coalesce(cx2.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
      or (lower(coalesce(cx2.value->>'match','false')) in ('true','1'))
    )
    and nullif(btrim(coalesce(cx2.value->>'ref_after','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_start','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'timesheet_end','')), '') is not null
    and nullif(btrim(coalesce(cx2.value->>'work_date','')), '') is not null
  on conflict (timesheet_id, comparison_key) do nothing;

  drop table if exists pg_temp.tmp_mode_a_ref_set_upd;
  create temporary table tmp_mode_a_ref_set_upd(
    timesheet_id uuid not null
  ) on commit drop;

  with upd as (
    update public.nhsp_shifts nsset
       set ref_num = mrs.ref_after,
           hr_request_id = mrs.ref_after,
           updated_at = v_now
      from tmp_mode_a_ref_set mrs
      left join public.timesheets_financials tfm
        on tfm.timesheet_id = mrs.timesheet_id
       and tfm.is_current = true
     where nsset.source_system = 'HEALTHROSTER'::public.hr_source_enum
       and nsset.cancelled_at_utc is null
       and nsset.timesheet_id = mrs.timesheet_id
       and nsset.work_date = mrs.work_date
       and nsset.invoice_id is null
       and (tfm.timesheet_id is null or (tfm.locked_by_invoice_id is null and tfm.paid_at_utc is null))
       and to_char((date_trunc('minute', nsset.start_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_start_hhmm
       and to_char((date_trunc('minute', nsset.end_utc) at time zone 'Europe/London'), 'HH24:MI') = mrs.ts_end_hhmm
       and coalesce(nsset.break_mins,0) = coalesce(mrs.ts_break_mins,0)
       and (
         nsset.ref_num is distinct from mrs.ref_after
         or nsset.hr_request_id is distinct from mrs.ref_after
       )
       and not exists (
         select 1
         from public.timesheets_financials tf_lock2
         cross join lateral jsonb_array_elements(coalesce(tf_lock2.invoice_breakdown_json->'segments','[]'::jsonb)) as seg2(value)
         where tf_lock2.is_current = true
           and tf_lock2.timesheet_id = nsset.timesheet_id
           and nullif(btrim(seg2.value->>'nhsp_shift_id'), '') = nsset.id::text
           and nullif(btrim(seg2.value->>'invoice_locked_invoice_id'), '') is not null
         limit 1
       )
    returning nsset.timesheet_id
  )
  insert into tmp_mode_a_ref_set_upd(timesheet_id)
  select upd.timesheet_id
  from upd
  where upd.timesheet_id is not null;

  get diagnostics v_mode_a_ref_set_count = row_count;

  insert into tmp_ref_updated_ts(timesheet_id)
  select distinct u.timesheet_id
  from tmp_mode_a_ref_set_upd u
  where u.timesheet_id is not null
  on conflict do nothing;

  insert into tmp_aff_ts(timesheet_id)
  select distinct u2.timesheet_id
  from tmp_mode_a_ref_set_upd u2
  where u2.timesheet_id is not null
  on conflict do nothing;

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_MATCHED_REF_SET',
      'ref_set_count', v_mode_a_ref_set_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ UPDATED: tmp_val_upsert now computes new_pre_validated
  --   new_pre_validated = true when:
  --     - validation result is OK/OVERRIDDEN (=> new_status VALIDATION_OK)
  --     - AND timesheet is NOT authorised yet (timesheets.authorised_at_server IS NULL)
  -- ─────────────────────────────────────────────
  create temporary table tmp_val_upsert on commit drop as
  select
    vr.timesheet_id,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then 'VALIDATION_OK'::public.validation_status_enum
      else 'VALIDATION_ERROR'::public.validation_status_enum
    end as new_status,
    'HEALTHROSTER_WEEKLY'::text as new_reason_code,
    case when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE') then v_now else null end as new_validated_at_utc,
    p_import_id as new_last_source,
    case
      when vr.overall_status in ('OK','PASS','VALIDATION_OK','OVERRIDDEN','OVERRIDE')
       and tva.timesheet_id is not null
       and tva.authorised_at_server is null
      then true
      else false
    end as new_pre_validated
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  left join public.timesheets tva
    on tva.timesheet_id = vr.timesheet_id
   and tva.is_current = true
  where vm.mode = 'MODE_A'
    and vr.timesheet_id is not null;

  -- ✅ UPDATED: include pre_validated changes as "validation_changed"
  select coalesce(array_agg(distinct x.timesheet_id order by x.timesheet_id), array[]::uuid[])
  into v_validation_changed_timesheet_ids
  from (
    select u.timesheet_id
    from tmp_val_upsert u
    left join public.timesheet_validations tv
      on tv.timesheet_id = u.timesheet_id
    where tv.timesheet_id is null
       or tv.status is distinct from u.new_status
       or tv.validated_at_utc is distinct from u.new_validated_at_utc
       or tv.last_source is distinct from u.new_last_source
       or tv.reason_code is distinct from u.new_reason_code
       or tv.pre_validated is distinct from u.new_pre_validated
  ) as x;

  -- ✅ UPDATED: insert/upsert includes pre_validated
  insert into public.timesheet_validations(
    timesheet_id,
    status,
    reason_code,
    validated_at_utc,
    last_source,
    pre_validated,
    updated_at
  )
  select
    u.timesheet_id,
    u.new_status,
    u.new_reason_code,
    u.new_validated_at_utc,
    u.new_last_source,
    u.new_pre_validated,
    v_now
  from tmp_val_upsert u
  on conflict (timesheet_id) do update
    set status = excluded.status,
        reason_code = excluded.reason_code,
        validated_at_utc = excluded.validated_at_utc,
        last_source = excluded.last_source,
        pre_validated = excluded.pre_validated,
        updated_at = excluded.updated_at;

  get diagnostics v_validations_upserted = row_count;

  insert into tmp_aff_ts(timesheet_id)
  select distinct t.tsid
  from unnest(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])) as t(tsid)
  where t.tsid is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct vr.timesheet_id), array[]::uuid[])
  into v_mismatched_tsids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id = vr.timesheet_id
  where vm.mode = 'MODE_A'
    and vr.has_mismatch is true
    and vr.timesheet_id is not null;

  -- A validation-only Weekly timesheet is eligible for configured
  -- auto-authorisation only when the immutable coverage says omissions are
  -- meaningful and every segment on the whole timesheet has one exact
  -- HealthRoster match whose reference has been durably written.  Processing
  -- one selected row, one day or one matching segment can never authorise the
  -- rest of the timesheet by implication.
  select coalesce(array_agg(distinct vr.timesheet_id order by vr.timesheet_id),array[]::uuid[])
  into v_validation_auto_authorise_timesheet_ids
  from tmp_val_rows vr
  join tmp_val_mode vm
    on vm.timesheet_id=vr.timesheet_id
   and vm.mode='MODE_A'
  join tmp_val_upsert vu
    on vu.timesheet_id=vr.timesheet_id
  join public.timesheets t
    on t.timesheet_id=vr.timesheet_id
   and t.is_current=true
   and t.revoked_at is null
  join public.hr_imports hi
    on hi.id=p_import_id
  left join public.timesheets_financials tf
    on tf.timesheet_id=t.timesheet_id
   and tf.is_current=true
  cross join lateral (
    select case
      when jsonb_typeof(t.actual_schedule_json)='array'
       and jsonb_array_length(t.actual_schedule_json)>0
        then jsonb_array_length(t.actual_schedule_json)
      when jsonb_typeof(tf.invoice_breakdown_json)='object'
       and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
        then jsonb_array_length(tf.invoice_breakdown_json->'segments')
      else 0
    end as segment_count
  ) whole_timesheet
  where vu.new_status='VALIDATION_OK'::public.validation_status_enum
    and vu.new_pre_validated=true
    and hi.coverage_mode in ('COMPLETE_ALL','COMPLETE_SELECTED_CANDIDATES')
    and (
      hi.coverage_mode='COMPLETE_ALL'
      or exists (
        select 1
        from public.import_review_scope_candidates scoped_candidate
        where scoped_candidate.import_id=p_import_id
          and scoped_candidate.candidate_id=vr.candidate_id
      )
    )
    and jsonb_typeof(vr.row_json->'comparisons')='array'
    and jsonb_array_length(vr.row_json->'comparisons')>0
    and whole_timesheet.segment_count=jsonb_array_length(vr.row_json->'comparisons')
    and not exists (
      select 1
      from jsonb_array_elements(vr.row_json->'comparisons') comparison(value)
      where not (
        (
          upper(coalesce(comparison.value->>'match_status','')) in ('MATCH','MATCHED','OK','PASS')
          or lower(coalesce(comparison.value->>'match','false')) in ('true','1')
        )
        and lower(coalesce(comparison.value->>'time_match','false')) in ('true','1')
        and nullif(btrim(comparison.value->>'ref_after'),'') is not null
        and nullif(btrim(comparison.value->>'work_date'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_start'),'') is not null
        and nullif(btrim(comparison.value->>'timesheet_end'),'') is not null
        and exists (
          select 1
          from public.nhsp_shifts matched_shift
          where matched_shift.source_system='HEALTHROSTER'::public.hr_source_enum
            and matched_shift.cancelled_at_utc is null
            and matched_shift.timesheet_id=vr.timesheet_id
            and matched_shift.work_date=(comparison.value->>'work_date')::date
            and to_char((date_trunc('minute',matched_shift.start_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_start'
            and to_char((date_trunc('minute',matched_shift.end_utc) at time zone 'Europe/London'),'HH24:MI')=
              comparison.value->>'timesheet_end'
            and coalesce(matched_shift.break_mins,0)=coalesce(nullif(btrim(comparison.value->>'timesheet_break_mins'),'')::integer,0)
            and matched_shift.ref_num=comparison.value->>'ref_after'
            and matched_shift.hr_request_id=comparison.value->>'ref_after'
        )
      )
    );

  -- Query emails are intentionally outside the source transaction. The
  -- database returns selected action IDs; the Worker later calls the
  -- idempotent outbox-backed enqueue RPC after source commit.

  v_steps := v_steps || jsonb_build_array(
    jsonb_build_object(
      'step','MODE_A_VALIDATIONS_DONE',
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count
    )
  );

  -- ─────────────────────────────────────────────
  -- ✅ Compute ref_updated_timesheet_ids (for post-apply QR reissue + tspdf regen decisions)
  -- ─────────────────────────────────────────────
  select coalesce(array_agg(distinct rts.timesheet_id order by rts.timesheet_id), array[]::uuid[])
  into v_ref_updated_timesheet_ids
  from tmp_ref_updated_ts rts
  where rts.timesheet_id is not null;

  v_ref_updated_timesheet_ids_count := coalesce(array_length(v_ref_updated_timesheet_ids, 1), 0);

  -- ─────────────────────────────────────────────
  -- 11) Compute affected_timesheet_ids (MODE_B work + MODE_A validation changes)
  -- ─────────────────────────────────────────────
  -- Build the ordinary authoritative MODE_B scope separately from the
  -- whole-timesheet MODE_A validation scope calculated above.
  -- active imported rows, protected amendment correction pairs and
  -- cancellation/reversal results.  MODE_A validation/reference work is
  -- deliberately excluded even though it remains part of the TSFIN refresh.
  select coalesce(array_agg(distinct target.timesheet_id order by target.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select ns.timesheet_id
    from public.nhsp_shifts ns
    where ns.source_system='HEALTHROSTER'::public.hr_source_enum
      and ns.client_id=v_import_client_id
      and ns.cancelled_at_utc is null
      and ns.external_row_key=any(coalesce(v_force_keys_final,array[]::text[]))
      -- Protected changed-hours rows are represented financially by their
      -- immutable reversal/replacement pair.  The source shift remains linked
      -- to the root for import identity, but refreshing that settled root as
      -- well would count the same delta twice in the correction-chain
      -- residual (live root truth plus the signed pair).
      and not (
        ns.external_row_key=any(
          coalesce(v_invoiced_changed_keys,array[]::text[])
        )
      )
      and ns.timesheet_id is not null
    union all
    select phase3_created.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'created_timesheet_ids','[]'::jsonb)) phase3_created(value)
    union all
    select phase3_updated.value::uuid
    from jsonb_array_elements_text(coalesce(v_phase3_result->'updated_timesheet_ids','[]'::jsonb)) phase3_updated(value)
    union all
    select cancelled.value::uuid
    from jsonb_array_elements_text(coalesce(v_cancellations_result->'affected_timesheet_ids','[]'::jsonb)) cancelled(value)
  ) target
  where target.timesheet_id is not null;

  -- A protected correction must remain a complete TSFIN/lifecycle unit even
  -- when this batch changes only its mutable replacement member.
  select coalesce(array_agg(distinct expanded.timesheet_id order by expanded.timesheet_id),array[]::uuid[])
  into v_authoritative_affected_timesheet_ids
  from (
    select requested.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    where requested.timesheet_id is not null
    union
    select partner.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) requested(timesheet_id)
    join public.timesheets seed
      on seed.timesheet_id=requested.timesheet_id
     and seed.is_current=true
     and seed.correction_id is not null
     and upper(btrim(coalesce(seed.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
    join public.timesheets partner
      on partner.correction_id=seed.correction_id
     and partner.is_current=true
     and upper(btrim(coalesce(partner.adjustment_origin,''))) in (
       'IMPORT_CORRECTION','IMPORT_CANCELLATION','HEALTHROSTER_CHANGED_HOURS',
       'NHSP_CHANGED_HOURS','HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
     )
  ) expanded
  where expanded.timesheet_id is not null;
  insert into tmp_aff_ts(timesheet_id)
  select authoritative.timesheet_id
  from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) authoritative(timesheet_id)
  where authoritative.timesheet_id is not null
  on conflict do nothing;

  select coalesce(array_agg(distinct a.timesheet_id order by a.timesheet_id), array[]::uuid[])
  into v_affected_timesheet_ids
  from tmp_aff_ts a
  where a.timesheet_id is not null
    -- A protected source is immutable financial history. It can enter the
    -- generic affected set through MODE_A validation/reference bookkeeping
    -- even though the authoritative MODE_B scope above correctly selected
    -- only the new correction pair. Never let that bookkeeping requeue the
    -- settled root for TSFIN recalculation.
    and not (
      a.timesheet_id=any(
        coalesce(v_protected_source_timesheet_ids,array[]::uuid[])
      )
      and not (a.timesheet_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
    );

  -- Restore every previously-authorised mutable source and authorise every
  -- financial correction member regardless of the ordinary setting.  A
  -- changed-hours reversal/replacement pair therefore moves together, while
  -- a true cancellation contributes its reversal only.
  select coalesce(array_agg(distinct required.timesheet_id order by required.timesheet_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids
  from (
    select existing.timesheet_id
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) existing(timesheet_id)
    union all
    select correction.timesheet_id
    from unnest(coalesce(v_authoritative_affected_timesheet_ids,array[]::uuid[])) affected(timesheet_id)
    join public.timesheets correction on correction.timesheet_id=affected.timesheet_id
    where correction.is_current=true
      and correction.revoked_at is null
      and coalesce(correction.is_adjustment,false)
      and correction.correction_id is not null
  ) required
  where required.timesheet_id is not null;

  if to_regclass('pg_temp.import_review_reconciliation_units_v1') is not null then
    if exists(
      select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT') and (
        nullif(u.unit_json->>'correction_id','') is null
        or jsonb_typeof(u.unit_json->'applied_member_ids')<>'array'
        or jsonb_array_length(u.unit_json->'applied_member_ids')<>2
        or (select count(*)=2
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REVERSAL')=1
              and count(*) filter(where t.correction_kind='CHANGED_HOURS_REPLACEMENT')=1
              and count(distinct t.parent_timesheet_id)=1
              and count(t.parent_timesheet_id)=2
            from public.timesheets t
            where t.correction_id=u.unit_json->>'correction_id' and t.is_current and t.archived_at_utc is null
              and t.adjustment_origin='IMPORT_CORRECTION'
              and t.correction_kind in ('CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT')) is not true
      )
    ) then
      raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
        detail=jsonb_build_object('reason_code','CORRECTION_MEMBER_SET_INCOMPLETE')::text;
    end if;
    if exists(select 1 from pg_temp.import_review_reconciliation_units_v1 u
      where u.route='AMEND_PAID_UNINVOICED_SOURCE' and (
        nullif(u.unit_json->>'applied_timesheet_id','')::uuid is distinct from u.source_timesheet_id
        or nullif(u.unit_json->>'reviewed_unit_fingerprint','') is distinct from u.unit_fingerprint
        or coalesce(u.unit_json->>'rollover_mode','') not in ('CREATED_CURRENT_OPERATION_SHELL','REUSED_COMPLETED_OPERATION_SHELL')
        or coalesce(u.unit_json->>'historical_paid_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or coalesce(u.unit_json->>'current_shell_tsfin_id','')!~*'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        or u.unit_json->>'applied_result_fingerprint' is distinct from encode(extensions.digest(convert_to(
          jsonb_build_object(
            'applied_timesheet_id',(u.unit_json->>'applied_timesheet_id')::uuid,
            'rollover_mode',u.unit_json->>'rollover_mode',
            'historical_paid_tsfin_id',(u.unit_json->>'historical_paid_tsfin_id')::uuid,
            'current_shell_tsfin_id',(u.unit_json->>'current_shell_tsfin_id')::uuid,
            'intended_authorisation_action',u.unit_json->>'intended_authorisation_action',
            'reviewed_unit_fingerprint',u.unit_json->>'reviewed_unit_fingerprint',
            'reconciliation_fingerprint',u.unit_json->>'reconciliation_fingerprint')::text,'UTF8'),'sha256'),'hex')
        or not exists(select 1 from public.timesheets_financials historical
          where historical.id=(u.unit_json->>'historical_paid_tsfin_id')::uuid
            and historical.timesheet_id=u.source_timesheet_id and not historical.is_current
            and historical.paid_at_utc is not null and historical.locked_by_invoice_id is null)
        or not exists(select 1 from public.timesheets_financials shell
          where shell.id=(u.unit_json->>'current_shell_tsfin_id')::uuid
            and shell.timesheet_id=u.source_timesheet_id and shell.is_current and shell.paid_at_utc is null)
      )) then
      raise exception 'IMPORT_REVIEW_APPLY_POSTCONDITION_FAILED' using errcode='55000',
        detail=jsonb_build_object('reason_code','PAID_SOURCE_ROLLOVER_RESULT_INVALID')::text;
    end if;
    select coalesce(array_agg(distinct x.value::uuid order by x.value::uuid) filter (where x.value is not null),array[]::uuid[])
    into v_operation_bound_correction_timesheet_ids
    from pg_temp.import_review_reconciliation_units_v1 u
    left join lateral jsonb_array_elements_text(coalesce(u.unit_json->'applied_member_ids','[]'::jsonb)) x(value) on true
    where u.route in ('AMEND_EXISTING_REPLACEMENT','CREATE_REVERSAL_REPLACEMENT');
    select coalesce(jsonb_agg(u.unit_json order by u.action_id),'[]'::jsonb) into v_reconciliation_units
    from pg_temp.import_review_reconciliation_units_v1 u;
    select coalesce(array_agg(x order by x),array[]::uuid[]) into v_reauthorise_timesheet_ids
    from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
    where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));
  end if;

  v_auto_authorise_timesheet_ids:=public._import_review_auto_authorise_targets_core_v1(
    v_authoritative_affected_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,false
  );

  select coalesce(array_agg(distinct eligible.timesheet_id order by eligible.timesheet_id),array[]::uuid[])
  into v_auto_authorise_timesheet_ids
  from (
    select unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])) as timesheet_id
    union all
    select unnest(public._import_review_auto_authorise_targets_core_v1(
      v_validation_auto_authorise_timesheet_ids,'HEALTHROSTER'::public.hr_source_enum,true
    )) as timesheet_id
  ) eligible
  where eligible.timesheet_id is not null;
  select coalesce(array_agg(distinct target_id order by target_id),array[]::uuid[])
  into v_auto_authorise_timesheet_ids from (
    select target_id from unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])) x(target_id)
    where not (target_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
    union all
    select u.source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 u
    where u.route='AMEND_PAID_UNINVOICED_SOURCE'
      and u.unit_json->>'intended_authorisation_action'='AUTHORISE'
  ) reviewed_auto;
  select coalesce(array_agg(distinct target_id order by target_id),array[]::uuid[])
  into v_reauthorise_timesheet_ids from (
    select target_id from unnest(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x(target_id)
    where not (target_id=any(coalesce(v_paid_applied_timesheet_ids,array[]::uuid[])))
    union all
    select u.source_timesheet_id from pg_temp.import_review_reconciliation_units_v1 u
    where u.route='AMEND_PAID_UNINVOICED_SOURCE'
      and u.unit_json->>'intended_authorisation_action'='REAUTHORISE'
  ) reviewed_reauthorise;
  select coalesce(array_agg(x order by x),array[]::uuid[]) into v_general_authorise_timesheet_ids
  from unnest(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])||coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])) x
  where not (x=any(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])));

  if array_length(v_affected_timesheet_ids, 1) is not null then
    perform public.enqueue_ts_financials_priority(v_affected_timesheet_ids, 'CONTEXT_CHANGED'::public.ts_fin_reason_enum);
  end if;

  -- ─────────────────────────────────────────────
  -- 12) Preserve the source route.  Whole-import completion is owned by
  -- _import_review_apply_complete_core_v1 after it has proved that no
  -- deferred/selectable work or blockers remain.  An incremental batch must
  -- never make the staged import look globally applied.
  -- ─────────────────────────────────────────────
  update public.hr_imports hi3
  set import_scope = 'HR_WEEKLY'
  where hi3.id = p_import_id;

  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','IMPORT_BATCH_APPLIED'));

  -- ─────────────────────────────────────────────
  -- 13) Logging (invoice_debug only, via _imp_debug_audit)
  -- ─────────────────────────────────────────────
  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_VALIDATIONS_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'val_rows_count', v_val_rows_count,
      'validations_upserted', v_validations_upserted,
      'validation_changed_timesheet_ids_count', coalesce(array_length(v_validation_changed_timesheet_ids, 1), 0),
      'mismatched_timesheet_ids_count', coalesce(array_length(v_mismatched_tsids, 1), 0),
      'email_actions_count', v_email_actions_count,
      'email_jobs_count', v_email_jobs_count,
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids_count', v_ref_updated_timesheet_ids_count,
      'invalidation_actions_count', v_invalidation_actions_count
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  perform public._imp_debug_audit(
    p_actor_user_id,
    'HR_WEEKLY_APPLY_DEBUG',
    jsonb_build_object(
      'import_id', p_import_id::text,
      'client_id', v_import_client_id::text,
      'steps', v_steps
    ),
    'hr_imports',
    p_import_id::text,
    null,
    null,
    null,
    null
  );

  v_review_result:=jsonb_build_object(
    'import_id', p_import_id,
    'client_id', v_import_client_id,
    'mode_b', jsonb_build_object(
      'selected_truth_keys', to_jsonb(coalesce(v_selected_truth_keys_mode_b, array[]::text[])),
      'force_overwrite_external_row_keys', to_jsonb(coalesce(v_force_keys_final, array[]::text[])),
      'skip_external_row_keys', to_jsonb(coalesce(v_skip_keys_final, array[]::text[])),
      'phase1', v_phase1_result,
      'phase15', jsonb_build_object(
        'ok_rows', v_phase15_ok,
        'shift_updated_rows', v_phase15_updated
      ),
      'phase3', v_phase3_result,
      'cancellations', v_cancellations_result
    ),
    'mode_a', jsonb_build_object(
      'mirror', v_mirror_result,
      'validations_upserted', v_validations_upserted,
      'mismatched_timesheet_ids', to_jsonb(coalesce(v_mismatched_tsids, array[]::uuid[])),
      'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
      'mode_a_ref_cleared_count', v_mode_a_ref_cleared_count,
      'mode_a_ref_set_count', v_mode_a_ref_set_count,
      'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
      'whole_timesheet_auto_authorise_eligible_ids',to_jsonb(coalesce(v_validation_auto_authorise_timesheet_ids,array[]::uuid[]))
    ),
    'email_jobs', v_email_jobs,
    'affected_timesheet_ids', to_jsonb(coalesce(v_affected_timesheet_ids, array[]::uuid[])),
    'auto_authorise_timesheet_ids',to_jsonb(coalesce(v_auto_authorise_timesheet_ids,array[]::uuid[])),
    'post_commit_reauthorise_timesheet_ids',to_jsonb(coalesce(v_reauthorise_timesheet_ids,array[]::uuid[])),
    'reconciliation_action_ids',to_jsonb(coalesce(v_reconciliation_action_ids,array[]::text[])),
    'operation_bound_correction_action_ids',to_jsonb(coalesce(v_operation_bound_correction_action_ids,array[]::text[])),
    'general_authorise_timesheet_ids',to_jsonb(coalesce(v_general_authorise_timesheet_ids,array[]::uuid[])),
    'operation_bound_correction_timesheet_ids',to_jsonb(coalesce(v_operation_bound_correction_timesheet_ids,array[]::uuid[])),
    'reconciliation_units',coalesce(v_reconciliation_units,'[]'::jsonb),
    'validation_affected_timesheet_ids', to_jsonb(coalesce(v_validation_changed_timesheet_ids, array[]::uuid[])),
    'ref_updated_timesheet_ids', to_jsonb(coalesce(v_ref_updated_timesheet_ids, array[]::uuid[])),
    'post_commit_email_action_ids',v_post_commit_email_action_ids,
    'review_operation_id',v_review_operation_id
  );
  perform public._import_review_apply_complete_core_v1(p_import_id,v_review_operation_id,p_actor_user_id,v_review_result,
    jsonb_array_length(v_post_commit_email_action_ids)>0 or cardinality(v_affected_timesheet_ids)>0);
  return v_review_result;

exception when others then
  get stacked diagnostics v_sqlstate = returned_sqlstate, v_err = message_text;

  begin
    perform public._imp_debug_audit(
      p_actor_user_id,
      'HR_WEEKLY_APPLY_ERROR',
      jsonb_build_object(
        'import_id', p_import_id::text,
        'client_id', case when v_import_client_id is null then null else v_import_client_id::text end,
        'steps', v_steps,
        'sqlstate', v_sqlstate,
        'error', v_err,
        'last_shift_id', case when v_last_shift_id is null then null else v_last_shift_id::text end
      ),
      'hr_imports',
      p_import_id::text,
      null,
      null,
      null,
      null
    );
  exception when others then
    null;
  end;

  raise;
end;
$function$;

-- hr_weekly_candidate_not_worked_resolution_save_v1(uuid,text,boolean,bigint,integer,text,uuid,uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_candidate_not_worked_resolution_save_v1(p_import_id uuid, p_action_id text, p_confirmed boolean, p_expected_state_version bigint, p_expected_preview_generation integer, p_expected_evidence_fingerprint text, p_actor_user_id uuid DEFAULT NULL::uuid, p_request_id uuid DEFAULT NULL::uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'extensions', 'pg_temp'
AS $function$
declare
  v_state public.import_review_states%rowtype;
  v_action public.import_review_decisions%rowtype;
  v_existing public.import_review_weekly_validation_resolutions%rowtype;
  v_preview jsonb;
  v_preview_evidence jsonb;
  v_preview_timesheet_id uuid;
  v_match_count integer:=0;
  v_hash text;
  v_prior jsonb;
  v_refresh jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_request_id is null or p_confirmed is null
     or coalesce(p_action_id,'')!~'^[0-9a-f]{64}$'
     or coalesce(p_expected_evidence_fingerprint,'')!~'^[0-9a-f]{64}$' then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_INPUT_INVALID' using errcode='22023';
  end if;

  v_hash:=public._import_review_hash_v1(concat_ws('|','hr-weekly-candidate-not-worked-resolution-v1',
    p_import_id,p_action_id,p_confirmed,p_expected_state_version,p_expected_preview_generation,
    p_expected_evidence_fingerprint));

  select event_context_json into v_prior
  from public.import_review_events
  where import_id=p_import_id and operation_id=p_request_id
    and event_code='WEEKLY_CANDIDATE_NOT_WORKED_RESOLUTION_SAVED'
  order by id desc limit 1;
  if found then
    if v_prior->>'request_hash'<>v_hash then
      raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_REQUEST_CONFLICT' using errcode='23505';
    end if;
    return jsonb_build_object('ok',true,'replay',true,'import_id',p_import_id,
      'hr_row_id',v_prior->>'hr_row_id','confirmed',(v_prior->>'confirmed')::boolean,
      'state_version',(v_prior->>'resulting_state_version')::bigint,'status',v_prior->>'status');
  end if;

  select * into v_state
  from public.import_review_states where import_id=p_import_id for update;
  if not found then raise exception 'IMPORT_REVIEW_NOT_FOUND' using errcode='P0002'; end if;
  if v_state.status not in ('BLOCKED','READY','IN_REVIEW')
     or v_state.state_version<>p_expected_state_version
     or v_state.preview_generation<>p_expected_preview_generation then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_REVIEW_STALE' using errcode='40001';
  end if;

  select * into v_action
  from public.import_review_decisions d
  where d.import_id=p_import_id and d.action_id=p_action_id and d.is_current
    and d.hr_row_id is not null and d.timesheet_id is not null
    and d.summary_json->>'source_route'='HR_WEEKLY'
    and d.summary_json->>'authority_mode'='VALIDATION_ONLY'
    and d.summary_json->>'resolution_kind'='WEEKLY_CANDIDATE_DID_NOT_WORK'
    and (
      (p_confirmed and d.action_kind='ADVISORY' and d.summary_json->>'reason_code'='WEEKLY_SHIFT_ABSENT_FROM_TIMESHEET')
      or (not p_confirmed and d.action_kind='NO_ACTION' and d.summary_json->>'reason_code'='CANDIDATE_DID_NOT_WORK_CONFIRMED')
    )
  for update;
  if not found or v_action.evidence_fingerprint<>p_expected_evidence_fingerprint then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_EVIDENCE_STALE' using errcode='40001';
  end if;

  -- Re-run the bounded server comparison.  The option exists only when an
  -- actual submitted Weekly schedule is present and one exact HR row is absent.
  v_preview:=public.hr_weekly_validation_preview(p_import_id);
  with preview_evidence as (
    select nullif(row_json->>'timesheet_id','')::uuid timesheet_id,cx.value evidence_json
    from jsonb_array_elements(coalesce(v_preview->'rows','[]'::jsonb)) rows(row_json)
    cross join lateral jsonb_array_elements(coalesce(row_json->'comparisons','[]'::jsonb)) cx(value)
    where cx.value->>'match_status'='HR_ONLY'
      and nullif(cx.value->>'hr_row_id','')::uuid=v_action.hr_row_id
    union all
    select nullif(row_json->>'timesheet_id','')::uuid timesheet_id,cx.value evidence_json
    from jsonb_array_elements(coalesce(v_preview->'rows','[]'::jsonb)) rows(row_json)
    cross join lateral jsonb_array_elements(coalesce(row_json->'confirmed_exceptions','[]'::jsonb)) cx(value)
    where nullif(cx.value->>'hr_row_id','')::uuid=v_action.hr_row_id
  )
  select count(*),min(timesheet_id::text)::uuid,min(evidence_json::text)::jsonb
  into v_match_count,v_preview_timesheet_id,v_preview_evidence
  from preview_evidence;

  if v_match_count<>1
     or v_preview_timesheet_id is distinct from v_action.timesheet_id
     or coalesce(v_preview_evidence->>'exception_evidence_fingerprint',v_preview_evidence->>'evidence_fingerprint')
        is distinct from v_action.evidence_fingerprint then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_EVIDENCE_STALE' using errcode='40001';
  end if;

  if not exists (
    select 1
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.timesheet_id=v_action.timesheet_id and t.is_current
      and t.revoked_at is null and t.archived_at_utc is null
      and t.sheet_scope='WEEKLY'::public.timesheet_scope_enum
      and c.candidate_id=v_action.candidate_id and c.client_id=v_action.client_id
      and t.week_ending_date=nullif(v_action.summary_json->>'week_ending_date','')::date
      and (
        (jsonb_typeof(t.actual_schedule_json)='array' and jsonb_array_length(t.actual_schedule_json)>0)
        or (
          jsonb_typeof(tf.invoice_breakdown_json)='object'
          and jsonb_typeof(tf.invoice_breakdown_json->'segments')='array'
          and jsonb_array_length(tf.invoice_breakdown_json->'segments')>0
        )
      )
  ) then
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_TIMESHEET_NOT_SUBMITTED' using errcode='40001';
  end if;

  select * into v_existing
  from public.import_review_weekly_validation_resolutions
  where import_id=p_import_id and hr_row_id=v_action.hr_row_id for update;
  if found and v_existing.status='APPLIED' then
    if p_confirmed and v_existing.evidence_fingerprint=v_action.evidence_fingerprint then
      return jsonb_build_object('ok',true,'replay',true,'immutable',true,'import_id',p_import_id,
        'hr_row_id',v_action.hr_row_id,'confirmed',true,'state_version',v_state.state_version,'status',v_state.status);
    end if;
    raise exception 'HR_WEEKLY_CANDIDATE_NOT_WORKED_APPLIED_IMMUTABLE' using errcode='55000';
  end if;

  insert into public.import_review_weekly_validation_resolutions(
    import_id,hr_row_id,timesheet_id,resolution_code,status,evidence_fingerprint,
    preview_generation,state_version,selected_by_user_id,stale_at_utc,stale_reason_code
  ) values (
    p_import_id,v_action.hr_row_id,v_action.timesheet_id,'CANDIDATE_DID_NOT_WORK',
    case when p_confirmed then 'CURRENT' else 'CLEARED' end,
    v_action.evidence_fingerprint,v_state.preview_generation,v_state.state_version,p_actor_user_id,
    case when p_confirmed then null else now() end,
    case when p_confirmed then null else 'USER_CLEARED' end
  )
  on conflict(import_id,hr_row_id) do update set
    timesheet_id=excluded.timesheet_id,resolution_code=excluded.resolution_code,status=excluded.status,
    evidence_fingerprint=excluded.evidence_fingerprint,preview_generation=excluded.preview_generation,
    state_version=excluded.state_version,selected_at_utc=now(),selected_by_user_id=excluded.selected_by_user_id,
    stale_at_utc=excluded.stale_at_utc,stale_reason_code=excluded.stale_reason_code,updated_at_utc=now();

  update public.import_review_states
  set state_version=state_version+1,status='IN_REVIEW',updated_at_utc=now(),updated_by_user_id=p_actor_user_id
  where import_id=p_import_id returning * into v_state;

  insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
  values(p_import_id,v_state.state_version,p_request_id,'WEEKLY_CANDIDATE_NOT_WORKED_RESOLUTION_SAVED',p_actor_user_id,
    jsonb_build_object('request_hash',v_hash,'action_id',p_action_id,'hr_row_id',v_action.hr_row_id,
      'timesheet_id',v_action.timesheet_id,'confirmed',p_confirmed,
      'evidence_fingerprint',v_action.evidence_fingerprint,
      'resulting_state_version',v_state.state_version,'status',v_state.status));

  v_refresh:=public._import_review_refresh_core_v1(p_import_id,v_state.state_version,p_actor_user_id,500);
  return v_refresh||jsonb_build_object('replay',false,'hr_row_id',v_action.hr_row_id,
    'timesheet_id',v_action.timesheet_id,'confirmed',p_confirmed);
end
$function$;

-- hr_weekly_deterministic_rows(uuid)
CREATE OR REPLACE FUNCTION public.hr_weekly_deterministic_rows(p_import_id uuid)
 RETURNS TABLE(external_row_key text, candidate_id uuid, client_id uuid, work_date date, start_utc timestamp with time zone, end_utc timestamp with time zone, contract_id uuid, week_ending_date date)
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
  with imp as (
    select hi.id
    from public.hr_imports hi
    where hi.id = p_import_id
      and hi.source_system = 'HEALTHROSTER'::public.hr_source_enum
    limit 1
  ),
  p2 as (
    select
      p2r.hr_row_id,
      p2r.candidate_id,
      p2r.client_id,
      p2r.contract_id,
      p2r.week_ending_date,
      p2r.action
    from public.weekly_import_phase2(
      p_import_id := p_import_id,
      p_system_type := 'HR_WEEKLY'
    ) as p2r
    where p2r.hr_row_id is not null
  ),
  base as (
    select
      nullif(btrim(r.external_row_key), '') as external_row_key,
      p2.candidate_id as candidate_id,
      p2.client_id as client_id,
      r.date_local as work_date,
      nullif(btrim(r.payload_json->>'start_utc'), '') as start_utc_txt,
      nullif(btrim(r.payload_json->>'end_utc'), '') as end_utc_txt,
      p2.contract_id as contract_id,
      p2.week_ending_date as week_ending_date,
      p2.action as p2_action
    from imp
    join public.hr_rows r
      on r.import_id = p_import_id
    join p2
      on p2.hr_row_id = r.id
  )
  select
    b.external_row_key,
    b.candidate_id,
    b.client_id,
    b.work_date,
    (b.start_utc_txt)::timestamptz as start_utc,
    (b.end_utc_txt)::timestamptz as end_utc,
    b.contract_id,
    b.week_ending_date
  from base b
  where b.external_row_key is not null
    and b.candidate_id is not null
    and b.client_id is not null
    and b.work_date is not null
    and b.start_utc_txt is not null
    and b.end_utc_txt is not null
    and coalesce(upper(b.p2_action::text), '') not like 'REJECT_%'
    and b.start_utc_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})$'
    and b.end_utc_txt   ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?(Z|[+-]\d{2}:?\d{2})$'
$function$;

