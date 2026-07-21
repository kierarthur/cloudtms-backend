-- Atomic client/global settings writes for the existing financial-date policy.

create or replace function public.client_update_with_settings_v1(
  p_client_id uuid,p_expected_client_rev bigint,p_expected_settings_updated_at timestamptz,
  p_client_patch jsonb default '{}'::jsonb,p_financial_policy_patch jsonb default '{}'::jsonb,
  p_actor_user_id uuid default null,p_request_key text default null
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare
  v_patch jsonb:=coalesce(p_client_patch,'{}'); v_policy jsonb:=coalesce(p_financial_policy_patch,'{}');
  v_client public.clients%rowtype; v_in public.clients%rowtype; v_settings public.client_settings%rowtype; v_global public.settings_defaults%rowtype;
  v_reversal public.correction_financials_date_basis_enum; v_replacement public.correction_financials_date_basis_enum;
  v_eligible boolean; v_unknown text[]; v_key text:=btrim(coalesce(p_request_key,'')); v_old jsonb;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_client_id is null or length(v_key) not between 16 and 256 or jsonb_typeof(v_patch)<>'object' or jsonb_typeof(v_policy)<>'object'
    or pg_column_size(v_patch)>65536 or pg_column_size(v_policy)>4096 then raise exception 'CLIENT_UPDATE_INPUT_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(key_name order by key_name),array[]::text[]) into v_unknown
  from jsonb_object_keys(v_patch) as supplied_key(key_name)
  where key_name<>all(array['name','invoice_address','primary_invoice_email','ap_phone','vat_chargeable','payment_terms_days','mileage_charge_rate',
    'ts_queries_email','client_address','contact_title','contact_known_as','contact_forename','contact_surname','contact_job_title','contact_tel',
    'contact_mobile','contact_email','website','notes']::text[]);
  if cardinality(v_unknown)>0 then raise exception 'CLIENT_UPDATE_UNKNOWN_FIELDS' using errcode='22023',detail=to_jsonb(v_unknown)::text; end if;
  select coalesce(array_agg(key_name order by key_name),array[]::text[]) into v_unknown
  from jsonb_object_keys(v_policy) as supplied_key(key_name)
  where key_name<>all(array['reversal_complete_financials_date','reversal_replacement_financials_date']::text[]);
  if cardinality(v_unknown)>0 then raise exception 'CLIENT_POLICY_UPDATE_UNKNOWN_FIELDS' using errcode='22023',detail=to_jsonb(v_unknown)::text; end if;
  if exists(select 1 from jsonb_each(v_policy)e where e.value<>'null'::jsonb and upper(trim(both '"' from e.value::text)) not in ('PAID_DATE','NOW')) then
    raise exception 'CLIENT_POLICY_VALUE_INVALID' using errcode='22023'; end if;
  select * into v_client from public.clients where id=p_client_id for update; if not found then raise exception 'CLIENT_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_settings from public.client_settings where client_id=p_client_id order by effective_from desc nulls last,updated_at desc,id desc limit 1 for update;
  if not found then raise exception 'CLIENT_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;
  select * into v_global from public.settings_defaults where id=1 for share; if not found then raise exception 'GLOBAL_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;
  if v_client.rev is distinct from p_expected_client_rev or v_settings.updated_at is distinct from p_expected_settings_updated_at then
    raise exception 'CLIENT_UPDATE_VERSION_CONFLICT' using errcode='40001',detail=jsonb_build_object('client_rev',v_client.rev,'settings_updated_at',v_settings.updated_at)::text; end if;
  select * into v_in from jsonb_populate_record(null::public.clients,v_patch);
  v_reversal:=case when v_policy?'reversal_complete_financials_date' then nullif(upper(v_policy->>'reversal_complete_financials_date'),'')::public.correction_financials_date_basis_enum else v_settings.reversal_complete_financials_date end;
  v_replacement:=case when v_policy?'reversal_replacement_financials_date' then nullif(upper(v_policy->>'reversal_replacement_financials_date'),'')::public.correction_financials_date_basis_enum else v_settings.reversal_replacement_financials_date end;
  v_eligible:=coalesce(v_settings.is_nhsp,false) or (coalesce(v_settings.requires_hr,false) and coalesce(v_settings.no_timesheet_required,false));
  if ((v_policy?'reversal_complete_financials_date' and v_reversal is not null)
      or (v_policy?'reversal_replacement_financials_date' and v_replacement is not null)) and not v_eligible then
    raise exception 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT' using errcode='22023'; end if;
  perform public._ctms_assert_import_correction_settings_write_v1(v_settings.is_nhsp,v_settings.requires_hr,v_settings.no_timesheet_required,
    case when v_policy?'reversal_complete_financials_date' then v_reversal end,case when v_policy?'reversal_replacement_financials_date' then v_replacement end);
  v_old:=jsonb_build_object('client',to_jsonb(v_client),'reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
    'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date);
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
  update public.client_settings set reversal_complete_financials_date=v_reversal,reversal_replacement_financials_date=v_replacement,updated_at=now()
  where id=v_settings.id returning * into v_settings;
  perform public._inv_write_audit(p_actor_user_id,'CLIENT_UPDATED_WITH_IMPORT_SETTINGS',jsonb_build_object('client_id',p_client_id,
    'request_key_hash',public._import_review_hash_v1(v_key),'old',v_old,'new',jsonb_build_object('client',to_jsonb(v_client),
    'reversal_complete_financials_date',v_settings.reversal_complete_financials_date,'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date)),
    'client',p_client_id::text,null::jsonb,'Atomic client/import policy update',null::text,null::text,'client-update-with-settings:'||v_key);
  return jsonb_build_object('ok',true,'client',to_jsonb(v_client),'client_settings_id',v_settings.id,'eligible',v_eligible,
    'stored',jsonb_build_object('reversal_complete_financials_date',v_settings.reversal_complete_financials_date,
      'reversal_replacement_financials_date',v_settings.reversal_replacement_financials_date),
    'effective',jsonb_build_object('reversal_complete_financials_date',coalesce(v_settings.reversal_complete_financials_date,v_global.reversal_complete_financials_date),
      'reversal_replacement_financials_date',coalesce(v_settings.reversal_replacement_financials_date,v_global.reversal_replacement_financials_date)),
    'source',jsonb_build_object('reversal_complete_financials_date',case when v_settings.reversal_complete_financials_date is null then 'GLOBAL' else 'CLIENT' end,
      'reversal_replacement_financials_date',case when v_settings.reversal_replacement_financials_date is null then 'GLOBAL' else 'CLIENT' end),
    'client_rev',v_client.rev,'settings_updated_at',v_settings.updated_at);
end $function$;

create or replace function public.settings_defaults_import_financial_policy_update_v1(
  p_expected_updated_at timestamptz,p_reversal_complete_financials_date text,
  p_reversal_replacement_financials_date text,p_actor_user_id uuid default null,p_request_key text default null
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare v public.settings_defaults%rowtype; v_old jsonb; a public.correction_financials_date_basis_enum; b public.correction_financials_date_basis_enum; k text:=btrim(coalesce(p_request_key,''));
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  if length(k) not between 16 and 256 or upper(btrim(coalesce(p_reversal_complete_financials_date,''))) not in ('PAID_DATE','NOW')
    or upper(btrim(coalesce(p_reversal_replacement_financials_date,''))) not in ('PAID_DATE','NOW') then raise exception 'GLOBAL_IMPORT_POLICY_UPDATE_INPUT_INVALID' using errcode='22023'; end if;
  a:=upper(btrim(p_reversal_complete_financials_date))::public.correction_financials_date_basis_enum;
  b:=upper(btrim(p_reversal_replacement_financials_date))::public.correction_financials_date_basis_enum;
  select * into v from public.settings_defaults where id=1 for update; if not found then raise exception 'GLOBAL_SETTINGS_NOT_FOUND' using errcode='P0002'; end if;
  if v.updated_at is distinct from p_expected_updated_at then raise exception 'GLOBAL_SETTINGS_VERSION_CONFLICT' using errcode='40001',detail=v.updated_at::text; end if;
  v_old:=jsonb_build_object('reversal_complete_financials_date',v.reversal_complete_financials_date,'reversal_replacement_financials_date',v.reversal_replacement_financials_date,'updated_at',v.updated_at);
  update public.settings_defaults set reversal_complete_financials_date=a,reversal_replacement_financials_date=b,updated_at=now() where id=1 returning * into v;
  perform public._inv_write_audit(p_actor_user_id,'GLOBAL_IMPORT_FINANCIAL_POLICY_UPDATED',jsonb_build_object('request_key_hash',public._import_review_hash_v1(k),'old',v_old,
    'new',jsonb_build_object('reversal_complete_financials_date',v.reversal_complete_financials_date,'reversal_replacement_financials_date',v.reversal_replacement_financials_date,'updated_at',v.updated_at)),
    'settings_defaults','1',null::jsonb,'Global import correction financial-date policy update',null::text,null::text,'settings-defaults-import-policy:'||k);
  return jsonb_build_object('ok',true,'stored',jsonb_build_object('reversal_complete_financials_date',v.reversal_complete_financials_date,
    'reversal_replacement_financials_date',v.reversal_replacement_financials_date),'updated_at',v.updated_at,
    'historical_recalculation_triggered',false,'frozen_drafts_changed',false);
end $function$;

revoke all on function public.client_update_with_settings_v1(uuid,bigint,timestamptz,jsonb,jsonb,uuid,text) from public,anon,authenticated;
grant execute on function public.client_update_with_settings_v1(uuid,bigint,timestamptz,jsonb,jsonb,uuid,text) to service_role;
revoke all on function public.settings_defaults_import_financial_policy_update_v1(timestamptz,text,text,uuid,text) from public,anon,authenticated;
grant execute on function public.settings_defaults_import_financial_policy_update_v1(timestamptz,text,text,uuid,text) to service_role;
