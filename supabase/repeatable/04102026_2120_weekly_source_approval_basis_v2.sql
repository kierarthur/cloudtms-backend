-- JOINT-CONTRACT-V4 I1. Read-only approved evidence, never an Authorise owner.
-- Additional amounts remain the calculator's amounts; this validates their
-- saved unit/rate evidence and uses the existing stable component-ID rule.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_head_origin_guard_v2()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare v_keys text[];
begin
  if tg_op='UPDATE' and new.source_origin_json is distinct from old.source_origin_json then
    raise exception 'WEEKLY_SOURCE_HEAD_ORIGIN_IMMUTABLE' using errcode='55000';
  end if;
  if new.source_origin_json is null then return new; end if;
  case new.source_origin_json->>'origin_kind'
    when 'CURRENT_FINAL_SOURCE_V1' then
      v_keys:=array['origin_kind','final_revision_id','source_cycle_id','revision_number',
        'manifest_hash','policy_fingerprint','accepted_action_id'];
    when 'PROTECTED_LOCAL_DECISION_V1' then
      v_keys:=array['origin_kind','publication_request_id','generation_id','request_sha256',
        'source_qualification_digest','policy_fingerprint','before_origin','before_inventory_digest'];
    else
      if new.source_origin_json ? 'origin_kind' then
        raise exception 'WEEKLY_SOURCE_HEAD_ORIGIN_INVALID' using errcode='22023';
      end if;
      v_keys:=array['final_revision_id','source_cycle_id','revision_number','manifest_hash','policy_fingerprint'];
  end case;
  perform private.weekly_source_publication_require_keys_v1(new.source_origin_json,v_keys,'head.source_origin');
  if private.weekly_source_publication_request_digest_v1(new.source_origin_json)
       is distinct from new.source_generation_digest then
    raise exception 'WEEKLY_SOURCE_HEAD_ORIGIN_DIGEST_MISMATCH' using errcode='23514';
  end if;
  return new;
end;
$function$;
drop trigger if exists weekly_source_head_origin_guard_v2 on public.weekly_source_entitlement_heads;
create trigger weekly_source_head_origin_guard_v2
  before insert or update on public.weekly_source_entitlement_heads
  for each row execute function private.weekly_source_head_origin_guard_v2();

create or replace function private.weekly_source_initial_additional_components_v2(
  p_components jsonb, p_units jsonb, p_expected_pay numeric, p_expected_charge numeric,
  p_family_booking_id text
) returns jsonb
language plpgsql immutable
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_result jsonb:=p_components;
  v_entry record;
  v_component jsonb;
  v_code text;
  v_member text;
  v_units numeric;
  v_pay_rate numeric;
  v_charge_rate numeric;
  v_pay numeric;
  v_charge numeric;
  v_total_pay numeric:=0;
  v_total_charge numeric:=0;
begin
  if jsonb_typeof(p_components) is distinct from 'array'
     or jsonb_typeof(p_units) is distinct from 'object'
     or p_expected_pay is null or p_expected_charge is null
     or nullif(btrim(p_family_booking_id),'') is null then
    raise exception 'BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED' using errcode='23514';
  end if;
  if exists(select 1 from jsonb_each(p_units) u
    group by upper(btrim(u.key)) having count(*)<>1) then
    raise exception 'BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED' using errcode='23514';
  end if;
  for v_entry in select key,value from jsonb_each(p_units) order by upper(key),key loop
    v_code:=btrim(v_entry.key);
    if v_code='' or jsonb_typeof(v_entry.value) is distinct from 'object'
       or not (v_entry.value ?& array['unit_count','pay_rate','charge_rate','pay_ex_vat','charge_ex_vat'])
       or exists(select 1 from jsonb_each(v_entry.value) e
         where e.key=any(array['unit_count','pay_rate','charge_rate','pay_ex_vat','charge_ex_vat'])
           and (jsonb_typeof(e.value) not in ('number','string')
             or (e.value#>>'{}') !~ '^[+-]?[0-9]+([.][0-9]+)?$')) then
      raise exception 'BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED' using errcode='23514';
    end if;
    v_units:=(v_entry.value->>'unit_count')::numeric;
    v_pay_rate:=(v_entry.value->>'pay_rate')::numeric;
    v_charge_rate:=(v_entry.value->>'charge_rate')::numeric;
    v_pay:=(v_entry.value->>'pay_ex_vat')::numeric;
    v_charge:=(v_entry.value->>'charge_ex_vat')::numeric;
    if v_units<0 or v_pay_rate<0 or v_charge_rate<0
       or v_units<>round(v_units,6) or v_pay_rate<>round(v_pay_rate,6)
       or v_charge_rate<>round(v_charge_rate,6)
       or v_pay is distinct from round(v_units*v_pay_rate,2)
       or v_charge is distinct from round(v_units*v_charge_rate,2) then
      raise exception 'BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED' using errcode='23514';
    end if;
    v_total_pay:=v_total_pay+v_pay;
    v_total_charge:=v_total_charge+v_charge;
    -- The saved calculator's code occurrence belongs to this stable booking
    -- family, not every other week using the same code. Neither amount,
    -- physical version, contract, current head nor ordinal forms its identity.
    v_member:='additional:'||btrim(p_family_booking_id)||':'||upper(v_code);
    if v_units<>0 or v_pay<>0 or v_charge<>0 then
      v_component:=jsonb_build_object(
        'component_ordinal',jsonb_array_length(v_result)+1,
        'component_id',private.weekly_source_entitlement_component_id_v1(
          'ADDITIONAL_UNIT','ADDITIONAL_CODE',upper(v_code),v_member),
        'component_kind','ADDITIONAL_UNIT','economic_key_type','ADDITIONAL_CODE',
        'economic_key_value',upper(v_code),'component_member_identity',v_member,
        'segment_id',null,'segment_key',null,'segment_stable_key',null,
        'work_date',null,'reference_number',null,
        'hours_day',null,'hours_night',null,'hours_sat',null,'hours_sun',null,'hours_bh',null,
        'additional_code_raw',v_code,'unit_count',to_char(v_units,'FM9999999999990.000000'),
        'unit_pay_rate',to_char(v_pay_rate,'FM9999999999990.000000'),
        'unit_charge_rate',to_char(v_charge_rate,'FM9999999999990.000000'),
        'expense_code',null,'pay_ex_vat',to_char(v_pay,'FM9999999999990.00'),
        'charge_ex_vat',to_char(v_charge,'FM9999999999990.00'),
        'exclude_from_pay',false,'origin','WEEKLY_SOURCE',
        'movement_id',null,'movement_group_id',null);
      v_result:=v_result||jsonb_build_array(
        private.weekly_source_publication_component_canonical_v1(v_component,'initial.additional'));
    end if;
  end loop;
  if v_total_pay is distinct from p_expected_pay
     or v_total_charge is distinct from p_expected_charge then
    raise exception 'BPAY_NEXT_SOURCE_FIRST_ADDITIONAL_UNMAPPED' using errcode='23514';
  end if;
  return v_result;
end;
$function$;

-- Called by the existing inventory owner with its own complete vector. NULL
-- means no certificate, not an approved zero. Pre-authorisation TSFIN never
-- qualifies here; its separate saved-local Candidate read is not financial
-- authority. No Banking Pay or business row is written by either helper.
create or replace function private.weekly_source_inventory_approval_basis_v2(
  p_root_timesheet_id uuid, p_inventory jsonb
) returns jsonb
language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_root public.timesheets%rowtype;
  v_contract public.contracts%rowtype;
  v_auth public.weekly_source_root_authorisations%rowtype;
  v_fin public.timesheets_financials%rowtype;
  v_head public.weekly_source_entitlement_heads%rowtype;
  v_family public.weekly_exceptional_pay_target_families%rowtype;
  v_identity jsonb;
  v_scope jsonb;
  v_origin jsonb;
  v_components jsonb;
  v_exact_components jsonb;
  v_work_count integer;
  v_used_rate numeric;
  v_expenses jsonb;
  v_component jsonb;
  v_canonical jsonb;
  v_details jsonb:='[]'::jsonb;
  v_detail jsonb;
  v_policy jsonb;
  v_pairs jsonb:='[]'::jsonb;
  v_hashes jsonb:='[]'::jsonb;
  v_sha text;
  v_inventory_sha text;
  v_entitlement_sha text;
  v_total numeric:=0;
  v_charge_total numeric:=0;
  v_policy_sha text;
  v_count integer;
  v_n integer:=0;
  v_authority_kind text;
  v_key text;
begin
  if p_root_timesheet_id is null or (p_inventory->>'ok') is distinct from 'true'
     or jsonb_typeof(p_inventory->'components') is distinct from 'array' then return null; end if;
  v_identity:=private.weekly_source_resolve_root_identity_v1(p_root_timesheet_id);
  if (v_identity->>'ok') is distinct from 'true'
     or (v_identity->>'canonical_timesheet_id')::uuid is distinct from p_root_timesheet_id
     or (v_identity->>'family_is_current') is distinct from 'true' then return null; end if;
  select * into v_root from public.timesheets where timesheet_id=p_root_timesheet_id;
  if not found or not v_root.is_current or v_root.authorised_at_server is null
     or v_root.revoked_at is not null or v_root.archived_at_utc is not null then return null; end if;
  select * into v_contract from public.contracts where id=v_root.contract_id;
  if not found or v_contract.candidate_id is null or v_contract.client_id is null then return null; end if;
  select count(*) into v_count from public.weekly_source_root_authorisations a
    where a.root_timesheet_id=p_root_timesheet_id and a.withdrawn_at_utc is null;
  if v_count<>1 then return null; end if;
  select * into strict v_auth from public.weekly_source_root_authorisations a
    where a.root_timesheet_id=p_root_timesheet_id and a.withdrawn_at_utc is null;
  if v_auth.family_booking_id is distinct from v_root.booking_id
     or v_auth.timesheet_version is distinct from v_root.version then return null; end if;
  select count(*) into v_count from public.weekly_exceptional_pay_target_families f
    where btrim(f.root_family_booking_id)=btrim(v_root.booking_id);
  if v_count>1 then return null; end if;
  if v_count=1 then
    select * into strict v_family from public.weekly_exceptional_pay_target_families f
      where btrim(f.root_family_booking_id)=btrim(v_root.booking_id);
    if v_family.root_timesheet_id is distinct from v_root.timesheet_id
       or v_family.candidate_id is distinct from v_contract.candidate_id
       or v_family.contract_id is distinct from v_contract.id
       or v_family.week_ending_date is distinct from v_root.week_ending_date then return null; end if;
  end if;
  v_scope:=jsonb_build_object('root_timesheet_id',v_root.timesheet_id,
    'family_booking_id',v_root.booking_id,'target_family_id',v_family.id,
    'candidate_id',v_contract.candidate_id,'client_id',v_contract.client_id,
    'contract_id',v_contract.id,'week_ending_date',v_root.week_ending_date,
    'root_version',v_root.version::text,'family_bound_version',v_family.bound_version::text);
  v_components:=p_inventory->'components';
  if (p_inventory->>'component_count')::integer is distinct from jsonb_array_length(v_components)
     or exists(select 1 from jsonb_array_elements(v_components) c
       group by c.value->>'component_id' having count(*)<>1) then return null; end if;
  for v_component in select value from jsonb_array_elements(v_components) loop
    v_n:=v_n+1;
    v_canonical:=private.weekly_source_publication_component_canonical_v1(
      v_component-'component_sha256','approval_basis.component');
    v_sha:=encode(private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_canonical)),'hex');
    if (v_canonical->>'component_ordinal')::integer<>v_n
       or v_sha is distinct from v_component->>'component_sha256' then return null; end if;
    v_pairs:=v_pairs||jsonb_build_array(jsonb_build_object(
      'component_ordinal',v_canonical->'component_ordinal','component_id',v_canonical->'component_id'));
    v_hashes:=v_hashes||jsonb_build_array(v_sha);
    v_total:=v_total+case when (v_canonical->>'exclude_from_pay')::boolean
      then 0 else (v_canonical->>'pay_ex_vat')::numeric end;
    if v_canonical->>'charge_ex_vat' is null then return null; end if;
    v_charge_total:=v_charge_total+(v_canonical->>'charge_ex_vat')::numeric;
  end loop;
  v_inventory_sha:=encode(private.weekly_source_publication_request_digest_v1(
    jsonb_build_object('components',v_pairs)),'hex');
  if v_inventory_sha is distinct from p_inventory->>'inventory_digest' then return null; end if;

  if p_inventory->>'authority'='TSFIN' then
    if v_auth.current_entitlement_head_id is not null
       or exists(select 1 from public.weekly_source_entitlement_heads h
         where btrim(h.root_family_booking_id)=btrim(v_root.booking_id)) then return null; end if;
    select count(*) into v_count from public.timesheets_financials f
      where f.timesheet_id=p_root_timesheet_id and f.is_current;
    if v_count<>1 then return null; end if;
    select * into strict v_fin from public.timesheets_financials f
      where f.timesheet_id=p_root_timesheet_id and f.is_current;
    if v_fin.is_stale or v_fin.authorised_at_utc is null
       or v_fin.timesheet_version is distinct from v_root.version
       or v_fin.candidate_id is distinct from v_contract.candidate_id
       or v_fin.client_id is distinct from v_contract.client_id
       or v_total is distinct from v_fin.total_pay_ex_vat
       or v_charge_total is distinct from v_fin.total_charge_ex_vat
       or jsonb_typeof(v_fin.invoice_breakdown_json->'segments') is distinct from 'array'
       or jsonb_typeof(v_fin.policy_snapshot_json) is distinct from 'object'
       or v_fin.policy_snapshot_json='{}'::jsonb
       or jsonb_typeof(v_fin.rate_source_refs_json) is distinct from 'object'
       or v_fin.rate_source_refs_json='{}'::jsonb then return null; end if;
    select coalesce(jsonb_agg(jsonb_build_object(
      'work_event_id',e.work_event_id,'source_observation_kind',e.source_observation_kind,
      'candidate_reimbursement_ex_vat',e.candidate_reimbursement_ex_vat::text,
      'client_charge_ex_vat',e.client_charge_ex_vat::text) order by e.work_event_id),'[]'::jsonb)
      into v_expenses from public.weekly_source_expense_pay_materialisations m
      join public.weekly_expense_authority_generations e on e.id=m.expense_authority_generation_id
      where m.root_timesheet_id=v_root.timesheet_id and m.candidate_timesheet_financial_id=v_fin.id;
    v_exact_components:=private.weekly_source_initial_additional_components_v2(
      private.weekly_source_entitlement_components_v1(v_fin.invoice_breakdown_json->'segments',v_expenses),
      v_fin.additional_units_json,v_fin.additional_pay_ex_vat,v_fin.additional_charge_ex_vat,v_root.booking_id);
    if v_exact_components is distinct from (select coalesce(jsonb_agg(c.value-'component_sha256'
      order by (c.value->>'component_ordinal')::integer),'[]'::jsonb)
      from jsonb_array_elements(v_components) c(value)) then return null; end if;
    select count(*)::integer into v_work_count from jsonb_array_elements(v_exact_components) c(value)
      where c.value->>'component_kind'='WORKED_TIME';
    for v_detail in select value from jsonb_array_elements(v_fin.invoice_breakdown_json->'segments') loop
      if coalesce(v_detail->>'start','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
         or coalesce(v_detail->>'end','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
         or (v_detail->>'overnight')::boolean is null
         or (v_detail->>'break_mins')::integer is null
         or (v_detail->>'break_mins')::integer<0 then return null; end if;
      foreach v_key in array array['day','night','sat','sun','bh'] loop
        -- Preserve the established Banking first-snapshot selector exactly.
        -- No segment.display pay or current contract rate may replace the
        -- genuine weekly Source vector or the single frozen TSFIN rate.
        v_used_rate:=case
          when jsonb_typeof(v_detail->'weekly_source')='object'
            then (v_detail#>>array['weekly_source','pay_vector','rates',v_key])::numeric
          when v_work_count=1 then (to_jsonb(v_fin)->>('pay_'||v_key))::numeric
          else null end;
        if coalesce((v_detail->>('hours_'||v_key))::numeric,0)<>0
           and not (v_used_rate is null and
             coalesce((v_detail->>'exclude_from_pay')::boolean,false))
           and (v_used_rate is null or v_used_rate<>round(v_used_rate,6)) then return null; end if;
      end loop;
    end loop;
    -- Pin the entire approved detail/rate evidence, not a live contract price.
    v_details:=jsonb_build_object('segments',v_fin.invoice_breakdown_json->'segments',
      'additional_units',v_fin.additional_units_json,
      'actual_schedule',v_fin.actual_schedule_json,
      'rate_source_refs',v_fin.rate_source_refs_json);
    v_policy:=v_fin.policy_snapshot_json;
    -- Saved calculator facts legitimately contain fractional JSON numbers.
    -- They are opaque approved snapshot evidence, not the integer-only closed
    -- publication transport. Preserve their exact stored JSONB and domain hash.
    v_policy_sha:=encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_APPROVED_POLICY_V2',v_policy),'hex');
    v_authority_kind:='LOCKED_FINAL_SOURCE';
    v_origin:=jsonb_build_object('kind','INITIAL_AUTHORISED_TSFIN_V1',
      'root_authorisation_id',v_auth.id,'financial_snapshot_id',v_fin.id,
      'authorisation_generation',v_auth.authorisation_generation,
      'root_timesheet_id',v_root.timesheet_id,'root_version',v_root.version::text,
      'authorised_row_signature',v_auth.authorised_row_signature,
      'financial_snapshot_digest',encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',
        jsonb_build_object('policy',v_policy,'details',v_details,'components',v_components)),'hex'));
  elsif p_inventory->>'authority'='HEAD' then
    select * into v_head from public.weekly_source_entitlement_heads h
      where h.id=(p_inventory->>'head_id')::uuid and h.state='COMMITTED_CURRENT';
    if not found or v_auth.current_entitlement_head_id is distinct from v_head.id
       or v_head.root_timesheet_id is distinct from v_root.timesheet_id
       or v_head.root_family_booking_id is distinct from v_root.booking_id
       or v_head.root_timesheet_version is distinct from v_root.version
       or v_head.candidate_id is distinct from v_contract.candidate_id
       or v_head.contract_id is distinct from v_contract.id
       or v_head.week_ending_date is distinct from v_root.week_ending_date
       or v_head.component_count<>v_n then return null; end if;
    -- The genuine publisher stores its own exact closed origin at allocation.
    -- APP witness is only an amendment exception, never a general KEEP/move
    -- approval gate. Old missing origin is unavailable, not reconstructed.
    v_policy:=v_head.source_origin_json;
    v_policy_sha:=v_policy->>'policy_fingerprint';
    if jsonb_typeof(v_policy) is distinct from 'object'
       or coalesce(v_policy_sha,'') !~ '^[0-9a-f]{64}$'
       or private.weekly_source_publication_request_digest_v1(v_policy)
            is distinct from v_head.source_generation_digest then return null; end if;
    for v_component in select value from jsonb_array_elements(v_components) loop
      if v_component->>'component_kind'='WORKED_TIME' then
        if to_regclass('private.bpay_next_source_chosen_detail') is null then return null; end if;
        execute 'select count(*),jsonb_agg(to_jsonb(d))->0
          from private.bpay_next_source_chosen_detail d
          where d.head_id=$1 and d.component_id=$2
            and d.decision_bundle_id=$3 and d.bundle_revision=$4'
          into v_count,v_detail using v_head.id,(v_component->>'component_id')::uuid,
            v_head.decision_bundle_id,v_head.bundle_revision;
        if v_count<>1 or v_detail->>'component_sha256' is distinct from
             ('\x'||(v_component->>'component_sha256'))
           or v_detail->>'detail_sha256' is distinct from ('\x'||encode(
             sha256(convert_to((v_detail->'detail_json')::text,'UTF8')),'hex')) then return null; end if;
        if v_detail#>>'{detail_json,work_date}' is distinct from v_component->>'work_date'
           or coalesce(v_detail#>>'{detail_json,start}','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or coalesce(v_detail#>>'{detail_json,end}','') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
           or (v_detail#>>'{detail_json,overnight}')::boolean is null
           or (v_detail#>>'{detail_json,break_minutes}')::integer is null
           or (v_detail#>>'{detail_json,break_minutes}')::integer<0
           or jsonb_typeof(v_detail#>'{detail_json,rates}') is distinct from 'object' then return null; end if;
        foreach v_key in array array['day','night','sat','sun','bh'] loop
          if coalesce((v_component->>('hours_'||v_key))::numeric,0)<>0
             and (v_detail#>>array['detail_json','rates',v_key]) is null then return null; end if;
        end loop;
        v_details:=v_details||jsonb_build_array(jsonb_build_object(
          'component_id',v_component->'component_id','detail',v_detail->'detail_json'));
      else
        v_details:=v_details||jsonb_build_array(jsonb_build_object(
          'component_id',v_component->'component_id','detail',v_component-'component_sha256'));
      end if;
    end loop;
    v_authority_kind:=v_head.authority_kind;
    v_origin:=jsonb_build_object('kind','COMMITTED_SOURCE_HEAD_V1','head_id',v_head.id,
      'head_revision',v_head.head_revision::text,'decision_bundle_id',v_head.decision_bundle_id,
      'bundle_revision',v_head.bundle_revision::text,'root_authorisation_id',v_auth.id,
      'authorisation_generation',v_auth.authorisation_generation,
      'root_timesheet_id',v_root.timesheet_id,'root_version',v_root.version::text,
      'source_generation_digest',encode(v_head.source_generation_digest,'hex'),
      'source_revision',v_policy);
  else return null;
  end if;
  v_entitlement_sha:=encode(private.weekly_source_publication_request_digest_v1(
    jsonb_build_object('authority_kind',v_authority_kind,'certified_zero',v_n=0,
      'component_count',v_n,'components',v_hashes)),'hex');
  if v_head.id is not null and (v_head.inventory_digest is distinct from decode(v_inventory_sha,'hex')
     or v_head.entitlement_digest is distinct from decode(v_entitlement_sha,'hex')) then return null; end if;
  v_origin:=v_origin||jsonb_build_object('inventory_digest',v_inventory_sha,
    'entitlement_digest',v_entitlement_sha);
  return jsonb_build_object('scope',v_scope,'origin',v_origin,'coverage_complete',true,
    'component_count',v_n,'inventory_digest',v_inventory_sha,'entitlement_digest',v_entitlement_sha,
    'detail_digest',encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_details),'hex'),
    'approved_pay_ex_vat',to_char(v_total,'FM9999999999990.00'),
    'policy_fingerprint',v_policy_sha);
end;
$function$;

alter function private.weekly_source_initial_additional_components_v2(jsonb,jsonb,numeric,numeric,text) owner to postgres;
alter function private.weekly_source_inventory_approval_basis_v2(uuid,jsonb) owner to postgres;
alter function private.weekly_source_head_origin_guard_v2() owner to postgres;
revoke all on function private.weekly_source_initial_additional_components_v2(jsonb,jsonb,numeric,numeric,text)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_inventory_approval_basis_v2(uuid,jsonb)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_head_origin_guard_v2()
  from public,anon,authenticated,service_role;

commit;
