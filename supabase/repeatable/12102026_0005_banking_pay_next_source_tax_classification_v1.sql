-- SP20 / BD127: Source classification and exact target guards only.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_stage_source_current_v1(
  p_root_timesheet_id uuid, p_source_event_id uuid, p_expected_head_id uuid
) returns table(work_id uuid,revision_id uuid,source_event_id uuid)
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_segment_key text;
  v_existing record;
  v_ts public.timesheets%rowtype;
  v_tf public.timesheets_financials%rowtype;
  v_contract public.contracts%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_inventory jsonb;
  v_basis jsonb;
  v_scope jsonb;
  v_origin jsonb;
  v_initial_details jsonb;
  v_inventory_digest bytea;
  v_component jsonb;
  v_segment jsonb;
  v_chosen_component_sha bytea;
  v_chosen_detail_sha bytea;
  v_head_bundle_id uuid;
  v_head_bundle_revision bigint;
  v_segment_match_count integer;
  v_break jsonb;
  v_head_count integer;
  v_auth_id uuid;
  v_auth_generation integer;
  v_auth_signature text;
  v_auth_booking text;
  v_auth_version integer;
  v_head_authority text;
  v_head_certified_zero boolean;
  v_head_component_count integer;
  v_head_revision bigint;
  v_head_inventory_digest bytea;
  v_head_entitlement_digest bytea;
  v_head_generation_digest bytea;
  v_head_origin jsonb;
  v_candidate_name text;
  v_candidate_ref text;
  v_candidate_mileage_rate numeric;
  v_client_name text;
  v_client_mileage_rate numeric;
  v_component_id uuid;
  v_component_kind text;
  v_component_key text;
  v_component_amount numeric;
  v_total numeric:=0;
  v_hours numeric;
  v_rate_count integer;
  v_schedule_count integer;
  v_line_id uuid;
  v_shift_id uuid;
  v_break_no integer;
  v_break_minutes integer;
  v_break_total integer;
  v_break_start text;
  v_break_end text;
  v_use_segment boolean;
  v_line_no integer:=0;
  v_work_count integer:=0;
  v_missing_work_date boolean:=false;
  v_detail_kind text;
  v_revision_id uuid;
  v_first_snapshot boolean;
  v_source_pay_method text;
  v_bucket record;
  v_source_rate numeric;
begin
  if p_root_timesheet_id is null or p_source_event_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_SOURCE_STAGE_INPUT_INVALID';
  end if;
  if (select active_owner from private.bpay_next_module_control
      where id=1 for share)<>'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- Exact replay precedes current-state discovery: the Source head may have
  -- advanced since this already accepted event, but its revision cannot.
  select r.id as revision_id,w.id as work_id,r.physical_timesheet_id,
         r.source_head_id,r.source_kind
    into v_existing
    from private.bpay_next_work_revision r
    join private.bpay_next_work w on w.id=r.work_id
    where r.source_event_id=p_source_event_id;
  if found then
    if v_existing.physical_timesheet_id is distinct from p_root_timesheet_id
       or v_existing.source_head_id is distinct from p_expected_head_id
       or v_existing.source_kind not in ('SOURCE','PROTECTED') then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_REPLAY_CONFLICT';
    end if;
    work_id:=v_existing.work_id;
    revision_id:=v_existing.revision_id;
    source_event_id:=p_source_event_id;
    return next;
    return;
  end if;

  if private.bpay_next_approval_route_v1(p_root_timesheet_id)<>'SOURCE_HOURS' then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_ROUTE_INVALID';
  end if;
  select * into strict v_ts from public.timesheets
    where timesheet_id=p_root_timesheet_id for key share;
  select * into strict v_contract from public.contracts
    where id=v_ts.contract_id for key share;
  v_first_snapshot:=p_expected_head_id is null;
  -- A first authorisation is the sole case in which the financial snapshot
  -- supplies Source money. A committed head supplies its own complete money
  -- and may have no current TSFIN row (the Source publisher does not create
  -- one). Never require or silently substitute an older snapshot for a head.
  select * into v_tf from public.timesheets_financials
    where timesheet_id=p_root_timesheet_id and is_current=true for share;
  -- The saved contract channel is available even when the first-authorisation
  -- owner leaves TSFIN.pay_method NULL, or a head has no TSFIN at all.
  v_source_pay_method:=upper(coalesce(
    case when v_first_snapshot then v_tf.pay_method end,
    v_contract.pay_method_snapshot,''));
  if v_ts.is_current is distinct from true
     or (v_first_snapshot and (
       v_tf.id is null or v_ts.authorised_at_server is null
       or v_tf.authorised_at_utc is null
       or v_tf.is_stale is distinct from false
       or v_tf.timesheet_version<>v_ts.version
       or v_tf.candidate_id is distinct from v_contract.candidate_id
       or v_tf.client_id is distinct from v_contract.client_id))
     or v_source_pay_method not in ('PAYE','UMBRELLA') then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_STAGE_FINANCIAL_NOT_CURRENT',
      detail=pg_catalog.jsonb_build_object(
        'timesheet_current',v_ts.is_current is true,
        'timesheet_authorised',v_ts.authorised_at_server is not null,
        'financial_authorised',v_tf.authorised_at_utc is not null,
        'financial_fresh',v_tf.is_stale is false,
        'version_matches',v_tf.timesheet_version=v_ts.version,
        'candidate_matches',v_tf.candidate_id is not distinct from v_contract.candidate_id,
        'client_matches',v_tf.client_id is not distinct from v_contract.client_id,
        'pay_method',v_source_pay_method)::text;
  end if;
  select a.id,a.authorisation_generation,a.authorised_row_signature,
         a.family_booking_id,a.timesheet_version
    into v_auth_id,v_auth_generation,v_auth_signature,v_auth_booking,v_auth_version
    from public.weekly_source_root_authorisations a
    where a.root_timesheet_id=p_root_timesheet_id
      and a.withdrawn_at_utc is null
      and a.current_entitlement_head_id is not distinct from p_expected_head_id
    for share;
  if v_auth_id is null then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_STAGE_AUTHORITY_MISMATCH';
  end if;
  if v_first_snapshot then
    if v_auth_id<>p_source_event_id
       or exists(select 1 from public.weekly_source_entitlement_heads h
                 where pg_catalog.btrim(h.root_family_booking_id)
                       =pg_catalog.btrim(v_ts.booking_id)) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_FIRST_SNAPSHOT_NOT_PERMITTED';
    end if;
    v_head_authority:='LOCKED_FINAL_SOURCE';
  else
    if p_source_event_id<>p_expected_head_id then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_EVENT_MISMATCH';
    end if;
    select h.authority_kind,h.certified_zero,h.component_count,
           h.decision_bundle_id,h.bundle_revision,h.head_revision,
           h.inventory_digest,h.entitlement_digest,h.source_generation_digest,
           h.source_origin_json
      into v_head_authority,v_head_certified_zero,v_head_component_count,
           v_head_bundle_id,v_head_bundle_revision,v_head_revision,
           v_head_inventory_digest,v_head_entitlement_digest,v_head_generation_digest,
           v_head_origin
      from public.weekly_source_entitlement_heads h
      where h.id=p_expected_head_id and h.root_timesheet_id=p_root_timesheet_id
        and h.root_family_booking_id=v_ts.booking_id
        and h.root_timesheet_version=v_ts.version
        and h.candidate_id=v_contract.candidate_id
        and h.week_ending_date=v_ts.week_ending_date
        and h.contract_id=v_contract.id and h.state='COMMITTED_CURRENT'
      for share;
    if not found then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_NOT_CURRENT';
    end if;
  end if;
  v_inventory:=private.weekly_source_effective_inventory_v1(p_root_timesheet_id);
  if coalesce((v_inventory->>'ok')::boolean,false) is not true
     or (v_inventory->>'authority') is distinct from
        (case when v_first_snapshot then 'TSFIN' else 'HEAD' end)
     or (v_inventory->>'head_id')::uuid is distinct from p_expected_head_id
     or pg_catalog.jsonb_typeof(v_inventory->'components')<>'array'
     or (v_inventory->>'component_count')::integer is distinct from
        pg_catalog.jsonb_array_length(v_inventory->'components')
     or (not v_first_snapshot and v_head_component_count is distinct from
        pg_catalog.jsonb_array_length(v_inventory->'components')) then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_INVENTORY_NOT_EXACT';
  end if;
  -- JD01: I7 calls the genuine Source I1 once for this complete vector. Missing
  -- basis is unavailable, never a zero approval or a second live-money route.
  -- This qualification precedes every Banking financial INSERT. The actual
  -- Source owner must capture all chosen HEAD detail before invoking this mapper.
  v_basis:=v_inventory->'approval_basis';
  v_scope:=v_basis->'scope';
  v_origin:=v_basis->'origin';
  if pg_catalog.jsonb_typeof(v_basis) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_scope) is distinct from 'object'
     or pg_catalog.jsonb_typeof(v_origin) is distinct from 'object'
     or v_basis->'coverage_complete' is distinct from 'true'::jsonb
     or v_basis->'component_count' is distinct from
        pg_catalog.to_jsonb(pg_catalog.jsonb_array_length(v_inventory->'components'))
     or coalesce(v_basis->>'inventory_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'entitlement_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'detail_digest','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'policy_fingerprint','') !~ '^[0-9a-f]{64}$'
     or coalesce(v_basis->>'approved_pay_ex_vat','') !~ '^-?[0-9]+[.][0-9]{2}$'
     or v_basis->>'inventory_digest' is distinct from v_inventory->>'inventory_digest'
     or v_scope->'root_timesheet_id' is distinct from pg_catalog.to_jsonb(v_ts.timesheet_id)
     or v_scope->'family_booking_id' is distinct from pg_catalog.to_jsonb(v_ts.booking_id)
     or v_scope->'root_version' is distinct from pg_catalog.to_jsonb(v_ts.version::text)
     or v_scope->'candidate_id' is distinct from pg_catalog.to_jsonb(v_contract.candidate_id)
     or v_scope->'client_id' is distinct from pg_catalog.to_jsonb(v_contract.client_id)
     or v_scope->'contract_id' is distinct from pg_catalog.to_jsonb(v_contract.id)
     or v_scope->'week_ending_date' is distinct from pg_catalog.to_jsonb(v_ts.week_ending_date)
     or v_auth_booking is distinct from v_ts.booking_id
     or v_auth_version is distinct from v_ts.version then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_BASIS_NOT_EXACT';
  end if;
  -- A genuine ordinary HEAD can have no exceptional family. A declared family
  -- instead binds its exact PK/current root and bound version; neither lane
  -- trims or remaps the immutable raw WORK booking identity.
  if (v_scope->'target_family_id'='null'::jsonb
      and v_scope->'family_bound_version'='null'::jsonb) is not true then
    if pg_catalog.jsonb_typeof(v_scope->'target_family_id') is distinct from 'string'
       or pg_catalog.jsonb_typeof(v_scope->'family_bound_version') is distinct from 'string'
       or coalesce(v_scope->>'target_family_id','') !~
          '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       or coalesce(v_scope->>'family_bound_version','') !~ '^[1-9][0-9]*$' then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_SCOPE_NOT_EXACT';
    end if;
    if not exists(select 1 from public.weekly_exceptional_pay_target_families f
      where f.id=(v_scope->>'target_family_id')::uuid
        and f.bound_version::text=v_scope->>'family_bound_version'
        and f.root_timesheet_id=v_ts.timesheet_id
        and f.candidate_id=v_contract.candidate_id and f.contract_id=v_contract.id
        and f.week_ending_date=v_ts.week_ending_date) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_SCOPE_NOT_EXACT';
    end if;
  end if;
  v_inventory_digest:=pg_catalog.decode(v_basis->>'inventory_digest','hex');
  if v_first_snapshot then
    v_initial_details:=pg_catalog.jsonb_build_object(
      'segments',v_tf.invoice_breakdown_json->'segments',
      'additional_units',v_tf.additional_units_json,
      'actual_schedule',v_tf.actual_schedule_json,
      'rate_source_refs',v_tf.rate_source_refs_json);
    if v_origin is distinct from pg_catalog.jsonb_build_object(
        'kind','INITIAL_AUTHORISED_TSFIN_V1','root_authorisation_id',v_auth_id,
        'financial_snapshot_id',v_tf.id,'authorisation_generation',v_auth_generation,
        'root_timesheet_id',v_ts.timesheet_id,'root_version',v_ts.version::text,
        'authorised_row_signature',v_auth_signature,
        'financial_snapshot_digest',pg_catalog.encode(
          private.weekly_source_sha256_jsonb_v1(
            'WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',pg_catalog.jsonb_build_object(
            'policy',v_tf.policy_snapshot_json,'details',v_initial_details,
            'components',v_inventory->'components')),'hex'),
        'inventory_digest',v_basis->>'inventory_digest',
        'entitlement_digest',v_basis->>'entitlement_digest')
       or v_basis->>'detail_digest' is distinct from pg_catalog.encode(
         private.weekly_source_sha256_jsonb_v1(
           'WEEKLY_SOURCE_APPROVED_DETAIL_V2',v_initial_details),'hex')
       or v_basis->>'policy_fingerprint' is distinct from pg_catalog.encode(
         private.weekly_source_sha256_jsonb_v1(
           'WEEKLY_SOURCE_APPROVED_POLICY_V2',v_tf.policy_snapshot_json),'hex') then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_INITIAL_BASIS_ORIGIN_MISMATCH';
    end if;
  elsif v_origin is distinct from pg_catalog.jsonb_build_object(
      'kind','COMMITTED_SOURCE_HEAD_V1','head_id',p_expected_head_id,
      'head_revision',v_head_revision::text,'decision_bundle_id',v_head_bundle_id,
      'bundle_revision',v_head_bundle_revision::text,'root_authorisation_id',v_auth_id,
      'authorisation_generation',v_auth_generation,'root_timesheet_id',v_ts.timesheet_id,
      'root_version',v_ts.version::text,
      'source_generation_digest',pg_catalog.encode(v_head_generation_digest,'hex'),
      'source_revision',v_head_origin,'inventory_digest',v_basis->>'inventory_digest',
      'entitlement_digest',v_basis->>'entitlement_digest')
     or v_inventory_digest is distinct from v_head_inventory_digest
     or pg_catalog.decode(v_basis->>'entitlement_digest','hex') is distinct from
        v_head_entitlement_digest
     or v_basis->>'policy_fingerprint' is distinct from v_head_origin->>'policy_fingerprint'
     or pg_catalog.jsonb_typeof(v_head_origin) is distinct from 'object'
     or private.weekly_source_publication_request_digest_v1(v_head_origin) is distinct from
        v_head_generation_digest then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HEAD_BASIS_ORIGIN_MISMATCH';
  end if;
  for v_component in select value from pg_catalog.jsonb_array_elements(
    v_inventory->'components')
  loop
    v_component_kind:=v_component->>'component_kind';
    if v_component_kind not in
       ('WORKED_TIME','ADDITIONAL_UNIT','EXPENSE','SOURCE_FIXED_EXPENSE')
       or nullif(v_component->>'component_id','') is null
       or (v_component->>'pay_ex_vat')::numeric is null
       or (v_component->>'pay_ex_vat')::numeric<>
          pg_catalog.round((v_component->>'pay_ex_vat')::numeric,2)
       or (v_component->>'exclude_from_pay')::boolean is null then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_COMPONENT_INVALID';
    end if;
    if v_component_kind='WORKED_TIME' then
      v_work_count:=v_work_count+1;
      if nullif(v_component->>'work_date','') is null then
        v_missing_work_date:=true;
      end if;
    end if;
    v_total:=v_total+case when (v_component->>'exclude_from_pay')::boolean
      then 0 else (v_component->>'pay_ex_vat')::numeric end;
  end loop;
  if v_total<>pg_catalog.round(v_total,2) then
    raise exception using errcode='22003', message='BPAY_NEXT_SOURCE_TOTAL_PRECISION_INVALID';
  end if;
  if (v_basis->>'approved_pay_ex_vat')::numeric is distinct from v_total then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_APPROVAL_MONEY_MISMATCH';
  end if;
  -- With no committed head, the authorised financial snapshot is the first
  -- Source amount. An empty or incomplete component inventory must never
  -- turn a positive authorised amount into a certified zero payment.
  if v_first_snapshot and (v_tf.total_pay_ex_vat is null
       or v_total is distinct from pg_catalog.round(v_tf.total_pay_ex_vat,2)) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_FIRST_TOTAL_MISMATCH';
  end if;
  v_head_count:=pg_catalog.jsonb_array_length(v_inventory->'components');
  if v_head_count=0 and v_total<>0 then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_EMPTY_MONEY_INVALID';
  end if;
  v_detail_kind:=case when v_work_count>0 and not v_missing_work_date
    then 'SHIFT' when v_work_count>0 then 'AGGREGATE' else 'FIXED' end;
  select coalesce(nullif(btrim(c.display_name),''),
                  nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),''),
                  c.id::text),c.tms_ref,c.mileage_pay_rate
    into strict v_candidate_name,v_candidate_ref,v_candidate_mileage_rate
    from public.candidates c where c.id=v_contract.candidate_id;
  select cl.name,cl.mileage_charge_rate
    into strict v_client_name,v_client_mileage_rate
    from public.clients cl where cl.id=v_contract.client_id;
  select count(*) into v_schedule_count
    from private.bpay_next_contract_rate_rows_v1(
      v_contract.rates_json,v_contract.bucket_labels_json,
      v_contract.additional_rates_json,
      coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,
               v_candidate_mileage_rate),
      coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,
               v_client_mileage_rate));
  -- Source head publication can cover two roots. The worker-control lock is
  -- taken before either work lock, matching the pair publisher and worker.
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_contract.candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_contract.candidate_id for update;
  select * into v_work from private.bpay_next_work
    where booking_id=v_ts.booking_id for update;
  if not found then
    insert into private.bpay_next_work
      (candidate_id,contract_id,original_timesheet_id,booking_id,
       work_kind,week_ending_date)
      values(v_contract.candidate_id,v_contract.id,v_ts.timesheet_id,
             v_ts.booking_id,'SOURCE',v_ts.week_ending_date)
      returning * into v_work;
  elsif v_work.candidate_id<>v_contract.candidate_id
     or v_work.contract_id<>v_contract.id
     or v_work.week_ending_date<>v_ts.week_ending_date
     or v_work.work_kind<>'SOURCE' then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_WORK_IDENTITY_CONFLICT';
  end if;
  insert into private.bpay_next_work_revision
    (work_id,revision_no,source_kind,source_head_id,source_event_id,
     physical_timesheet_id,financial_snapshot_id,physical_timesheet_version,
     source_pay_channel,week_ending_date,detail_kind,expected_line_count,
     expected_rate_schedule_count,certified_zero,approved_source_ex_vat,
     candidate_display_name,candidate_reference,client_display_name,
     job_title,band_label,timesheet_reference,source_inventory_digest)
    values(v_work.id,v_work.current_revision_no+1,
      case when v_head_authority='PROTECTED' then 'PROTECTED' else 'SOURCE' end,
      p_expected_head_id,p_source_event_id,v_ts.timesheet_id,v_tf.id,v_ts.version,
      v_source_pay_method,v_ts.week_ending_date,v_detail_kind,v_head_count,
      v_schedule_count,case when v_first_snapshot then v_head_count=0
        else v_head_certified_zero end,v_total,
      v_candidate_name,v_candidate_ref,v_client_name,
      v_contract.role,v_contract.band,v_ts.reference_number,v_inventory_digest)
    returning id into v_revision_id;
  insert into private.bpay_next_rate_schedule
    (revision_id,rate_family,rate_code,unit_label,paye_rate,umbrella_rate,charge_rate)
    select v_revision_id,r.rate_family,r.rate_code,r.unit_label,
           r.paye_rate,r.umbrella_rate,r.charge_rate
      from private.bpay_next_contract_rate_rows_v1(
        v_contract.rates_json,v_contract.bucket_labels_json,
        v_contract.additional_rates_json,
        coalesce(v_tf.mileage_pay_rate,v_contract.mileage_pay_rate,
                 v_candidate_mileage_rate),
        coalesce(v_tf.mileage_charge_rate,v_contract.mileage_charge_rate,
                 v_client_mileage_rate)) r;

  for v_component in select value from pg_catalog.jsonb_array_elements(
    v_inventory->'components') order by (value->>'component_ordinal')::integer
  loop
    v_component_id:=(v_component->>'component_id')::uuid;
    v_component_kind:=v_component->>'component_kind';
    v_component_key:='SOURCE:'||v_component_id::text;
    v_component_amount:=case when (v_component->>'exclude_from_pay')::boolean
      then 0 else (v_component->>'pay_ex_vat')::numeric end;
    v_hours:=coalesce((v_component->>'hours_day')::numeric,0)
      +coalesce((v_component->>'hours_night')::numeric,0)
      +coalesce((v_component->>'hours_sat')::numeric,0)
      +coalesce((v_component->>'hours_sun')::numeric,0)
      +coalesce((v_component->>'hours_bh')::numeric,0);
    if (v_first_snapshot and v_hours<0)
       or v_hours<>pg_catalog.round(v_hours,6) then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_HOURS_INVALID';
    end if;
    v_rate_count:=0;
    if v_component_kind='WORKED_TIME' then
      -- A committed head can contain signed correction hours. Freeze every
      -- non-zero bucket exactly, but never invent a Source hourly rate from
      -- its independently authoritative component amount.
      v_rate_count:=(case when coalesce((v_component->>'hours_day')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_night')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_sat')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_sun')::numeric,0)<>0 then 1 else 0 end)
        +(case when coalesce((v_component->>'hours_bh')::numeric,0)<>0 then 1 else 0 end);
    end if;
    v_line_no:=v_line_no+1;
    insert into private.bpay_next_approved_line
      (revision_id,line_no,component_key,source_component_id,component_kind,tax_treatment,
       work_date,approved_quantity,approved_unit_rate,
       expected_rate_detail_count,source_pay_ex_vat,evidence_ref)
      values(v_revision_id,v_line_no,v_component_key,v_component_id,
        case v_component_kind when 'WORKED_TIME' then 'WORK'
          when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end,
        -- OD03: EXPENSE_CODE (including SOURCE_FIXED_EXPENSE/MILEAGE) is fixed reimbursement.
        case when v_component_kind in ('WORKED_TIME','ADDITIONAL_UNIT') then 'TAXABLE'
          when v_component_kind in ('EXPENSE','SOURCE_FIXED_EXPENSE') then 'NON_TAXABLE' else null end,
        nullif(v_component->>'work_date','')::date,
        case when v_component_kind='WORKED_TIME' then v_hours
          when v_component_kind='ADDITIONAL_UNIT'
            then (v_component->>'unit_count')::numeric else null end,
        case when v_component_kind='ADDITIONAL_UNIT'
          then (v_component->>'unit_pay_rate')::numeric else null end,
        v_rate_count,v_component_amount,
        'source-component:'||v_component_id::text||'#'||
          coalesce(v_component->>'component_sha256','no-hash'))
      returning id into v_line_id;
    if v_component_kind='WORKED_TIME'
       and nullif(v_component->>'work_date','') is not null then
      v_segment:=null; v_segment_match_count:=0;
      if v_first_snapshot then
        select pg_catalog.count(*)::integer,
               pg_catalog.jsonb_agg(segment.value)->0
          into v_segment_match_count,v_segment
          from pg_catalog.jsonb_array_elements(
            case when pg_catalog.jsonb_typeof(v_tf.invoice_breakdown_json->'segments')='array'
              then v_tf.invoice_breakdown_json->'segments' else '[]'::jsonb end)
            segment(value)
          where segment.value->>'segment_id'=v_component->>'segment_id';
      else
        -- A published head stores amount and bucket hours but not accepted
        -- clocks/rates. Only its exact chosen-detail key may add those facts.
        select d.detail_json,d.component_sha256,d.detail_sha256
          into v_segment,v_chosen_component_sha,v_chosen_detail_sha
          from private.bpay_next_source_chosen_detail d
          where d.head_id=p_expected_head_id
            and d.component_id=v_component_id
            and d.decision_bundle_id=v_head_bundle_id
            and d.bundle_revision=v_head_bundle_revision;
        if not found or v_segment is null or (
           v_chosen_component_sha is distinct from
             pg_catalog.decode(v_component->>'component_sha256','hex')
           or v_chosen_detail_sha is distinct from
             pg_catalog.sha256(pg_catalog.convert_to(v_segment::text,'UTF8'))
           or v_segment->>'work_date' is distinct from v_component->>'work_date') then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_CHOSEN_DETAIL_MISMATCH';
        end if;
      end if;
      if v_segment_match_count>1 then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_SEGMENT_ID_AMBIGUOUS';
      end if;
      v_use_segment:=v_segment is not null
        and coalesce(v_segment->>'date',v_segment->>'work_date')
            =v_component->>'work_date'
        and (not v_first_snapshot or (
        coalesce((v_segment->>'hours_day')::numeric,0)
            =coalesce((v_component->>'hours_day')::numeric,0)
        and coalesce((v_segment->>'hours_night')::numeric,0)
            =coalesce((v_component->>'hours_night')::numeric,0)
        and coalesce((v_segment->>'hours_sat')::numeric,0)
            =coalesce((v_component->>'hours_sat')::numeric,0)
        and coalesce((v_segment->>'hours_sun')::numeric,0)
            =coalesce((v_component->>'hours_sun')::numeric,0)
        and coalesce((v_segment->>'hours_bh')::numeric,0)
            =coalesce((v_component->>'hours_bh')::numeric,0)));
      if v_first_snapshot and not v_use_segment then
        raise exception using errcode='23514',
          message='BPAY_NEXT_SOURCE_FIRST_SEGMENT_EVIDENCE_MISMATCH';
      end if;
      v_segment_key:=case when v_first_snapshot then nullif(v_segment#>>'{weekly_source,work_event_id}','')
        when v_segment->'from_frozen_pair'='true'::jsonb then nullif(v_segment->>'segment_key','')
        when v_segment->'from_reviewed_target_tsfin'='true'::jsonb then nullif(v_segment->>'before_segment_id','')
        else nullif(v_segment->>'work_event_id','') end;
      if v_segment_key is not null and v_segment->'from_frozen_pair' is distinct from 'true'::jsonb
        and v_segment->'from_reviewed_target_tsfin' is distinct from 'true'::jsonb then
        v_segment_key:=v_segment_key::uuid::text;
      end if;
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,
         approved_hours,
         detail_label,immutable_evidence_ref,segment_key,segment_pay_ex_vat,pay_excluded,
         hours_day,hours_night,hours_sat,hours_sun,hours_bh)
        values(v_line_id,1,(v_component->>'work_date')::date,
          case when v_use_segment then nullif(v_segment->>'start_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'end_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'start','') end,
          case when v_use_segment then nullif(v_segment->>'end','') end,
          case when v_use_segment then (v_segment->>'overnight')::boolean end,
          v_hours,
          nullif(v_component->>'reference_number',''),
          'source-component:'||v_component_id::text,v_segment_key,v_component_amount,
          (v_component->>'exclude_from_pay')::boolean,
          coalesce((v_component->>'hours_day')::numeric,0),coalesce((v_component->>'hours_night')::numeric,0),
          coalesce((v_component->>'hours_sat')::numeric,0),coalesce((v_component->>'hours_sun')::numeric,0),
          coalesce((v_component->>'hours_bh')::numeric,0))
        returning id into v_shift_id;
      if v_use_segment then
        if v_segment ? 'breaks'
           and pg_catalog.jsonb_typeof(v_segment->'breaks') not in ('array','null') then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_BREAK_SHAPE_INVALID';
        end if;
        v_break_no:=0; v_break_total:=0;
        for v_break in select value from pg_catalog.jsonb_array_elements(
          case when pg_catalog.jsonb_typeof(v_segment->'breaks')='array'
            then v_segment->'breaks' else '[]'::jsonb end)
        loop
          v_break_start:=v_break->>'start';
          v_break_end:=v_break->>'end';
          if pg_catalog.jsonb_typeof(v_break)<>'object'
             or coalesce(v_break_start,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$'
             or coalesce(v_break_end,'') !~ '^([01][0-9]|2[0-3]):[0-5][0-9]$' then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_BREAK_CLOCK_INVALID';
          end if;
          v_break_minutes:=(
            (pg_catalog.substring(v_break_end,1,2)::integer*60
              +pg_catalog.substring(v_break_end,4,2)::integer)
            -(pg_catalog.substring(v_break_start,1,2)::integer*60
              +pg_catalog.substring(v_break_start,4,2)::integer)+1440)%1440;
          if v_break_minutes=0 then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_BREAK_ZERO_WINDOW';
          end if;
          v_break_no:=v_break_no+1;
          v_break_total:=v_break_total+v_break_minutes;
          insert into private.bpay_next_break_detail
            (shift_detail_id,break_no,break_start_local,break_end_local,break_minutes)
            values(v_shift_id,v_break_no,v_break_start,v_break_end,v_break_minutes);
        end loop;
        if v_break_no=0
           and coalesce((v_segment->>'break_mins')::integer,
                        (v_segment->>'break_minutes')::integer,0)>0 then
          insert into private.bpay_next_break_detail
            (shift_detail_id,break_no,break_minutes)
            values(v_shift_id,1,coalesce((v_segment->>'break_mins')::integer,
                (v_segment->>'break_minutes')::integer));
        elsif v_break_no>0
           and (v_segment ? 'break_mins' or v_segment ? 'break_minutes')
           and v_break_total is distinct from
               coalesce((v_segment->>'break_mins')::integer,
                        (v_segment->>'break_minutes')::integer) then
          raise exception using errcode='23514',
            message='BPAY_NEXT_SOURCE_BREAK_DURATION_CONFLICT';
        end if;
      end if;
    end if;
    if v_rate_count>0 then
      for v_bucket in select * from (values
        ('DAY'::text,'hours_day'::text,v_tf.pay_day),
        ('NIGHT','hours_night',v_tf.pay_night),
        ('SAT','hours_sat',v_tf.pay_sat),
        ('SUN','hours_sun',v_tf.pay_sun),
        ('BH','hours_bh',v_tf.pay_bh)) b(code,hours_key,pay_rate)
      loop
        if coalesce((v_component->>v_bucket.hours_key)::numeric,0)<>0 then
          -- TSFIN's pay_day/... are explicitly FIRST-SEGMENT display rates.
          -- A Source first authorisation may contain several shifts with
          -- different rates in the same bucket, so use each exact matched
          -- segment's immutable Source pay vector when it exists.
          v_source_rate:=case
            when not v_use_segment then null
            when v_first_snapshot
              and pg_catalog.jsonb_typeof(v_segment->'weekly_source')='object'
              then (v_segment#>>array['weekly_source','pay_vector','rates',
                                    pg_catalog.lower(v_bucket.code)])::numeric
            when v_first_snapshot and v_work_count=1 then v_bucket.pay_rate
            when v_first_snapshot then null
            else (v_segment->'rates'->>pg_catalog.lower(v_bucket.code))::numeric
          end;
           if v_use_segment
              and not (v_source_rate is null and (
                coalesce(v_segment->>'from_frozen_pair','false')='true'
                or (v_first_snapshot
                    and (v_component->>'exclude_from_pay')::boolean)))
             and (v_source_rate is null
             or v_source_rate<>pg_catalog.round(v_source_rate,6)) then
            raise exception using errcode='23514',
              message='BPAY_NEXT_SOURCE_USED_RATE_MISSING';
          end if;
          insert into private.bpay_next_rate_detail
            (approved_line_id,bucket,approved_hours,source_pay_rate)
            values(v_line_id,v_bucket.code,
              (v_component->>v_bucket.hours_key)::numeric,v_source_rate);
        end if;
      end loop;
    end if;
  end loop;
  work_id:=v_work.id;
  revision_id:=v_revision_id;
  source_event_id:=p_source_event_id;
  return next;
end
$function$;

create or replace function private.bpay_next_initial_tsfin_certificate_v1(p_root_authorisation_id uuid)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  a public.weekly_source_root_authorisations%rowtype;
  t public.timesheets%rowtype;f public.timesheets_financials%rowtype;
  w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  p private.bpay_next_publication%rowtype;c public.contracts%rowtype;
  inventory jsonb;basis jsonb;origin jsonb;details jsonb;n integer;epoch bigint;
begin
  if p_root_authorisation_id is null then
    raise exception 'BPAY_NEXT_INITIAL_AUTH_ID_REQUIRED' using errcode='22023';end if;
  select m.owner_epoch into epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if epoch is null then raise exception 'BPAY_NEXT_BINDING_NEXT_REQUIRED' using errcode='55000';end if;
  select x.* into strict a from public.weekly_source_root_authorisations x where x.id=p_root_authorisation_id;
  select x.* into strict t from public.timesheets x where x.timesheet_id=a.root_timesheet_id;
  select x.* into strict c from public.contracts x where x.id=t.contract_id;
  select x.* into strict r from private.bpay_next_work_revision x where x.source_event_id=a.id;
  select x.* into strict w from private.bpay_next_work x where x.id=r.work_id;
  select x.* into strict f from public.timesheets_financials x where x.id=r.financial_snapshot_id;
  select x.* into strict p from private.bpay_next_publication x
    where x.command_id=private.bpay_next_source_command_id_v1(a.id);
  if a.withdrawn_at_utc is not null or a.current_entitlement_head_id is not null
     or exists(select 1 from public.weekly_source_entitlement_heads h where btrim(h.root_family_booking_id)=btrim(t.booking_id))
     or t.is_current is distinct from true or t.authorised_at_server is null
     or t.revoked_at is not null or t.archived_at_utc is not null
     or (a.family_booking_id,a.timesheet_version) is distinct from (t.booking_id,t.version)
     or (w.work_kind,w.candidate_id,w.contract_id,w.original_timesheet_id,w.booking_id,w.week_ending_date,w.current_revision_id)
       is distinct from ('SOURCE'::text,c.candidate_id,c.id,t.timesheet_id,t.booking_id,t.week_ending_date,r.id)
     or w.approval_state is distinct from 'APPROVED'
     or (r.source_kind,r.source_head_id,r.source_event_id,r.physical_timesheet_id,r.physical_timesheet_version,r.week_ending_date)
       is distinct from ('SOURCE'::text,null::uuid,a.id,t.timesheet_id,t.version,t.week_ending_date)
     or r.approved_at_utc is null or r.sealed_at_utc is null
     or f.is_current is distinct from true or f.is_stale is distinct from false or f.authorised_at_utc is null
     or (f.timesheet_id,f.timesheet_version,f.candidate_id,f.client_id)
       is distinct from (t.timesheet_id,t.version,w.candidate_id,c.client_id)
     or (p.work_id,p.revision_id,p.candidate_id,p.revision_no)
       is distinct from (w.id,r.id,w.candidate_id,r.revision_no)
     or not exists(select 1 from private.bpay_next_command command
       join private.bpay_next_command_member member on member.command_id=command.id and member.candidate_id=w.candidate_id
       where command.id=p.command_id and command.command_kind='POSITION_APPLY'
         and command.module_epoch=epoch and command.status in ('SEALED','COMPLETE')
         and command.sealed_at_utc is not null and command.expected_member_count=1 and member.member_no=1
         -- INITIAL_METADATA_PHASE_V2_BEGIN: no fabricated enrollment/job.
         and ((command.status='SEALED' and command.enrolled_member_count=0
           and p.status='QUEUED' and p.phase='NEW' and p.applied_at_utc is null
           and not exists(select 1 from private.bpay_next_job job where job.command_id=command.id))
          or (command.status='COMPLETE' and command.enrolled_member_count=1
           and p.status='APPLIED' and p.phase='DONE' and p.applied_at_utc is not null
           and (select count(*) from private.bpay_next_job job where job.command_id=command.id)=1
           and exists(select 1 from private.bpay_next_job job
             join private.bpay_next_worker_control ctl on ctl.candidate_id=job.candidate_id
             where job.command_id=command.id and job.candidate_id=w.candidate_id
               and job.job_kind='POSITION_APPLY' and job.command_sequence=command.agency_sequence
               and job.module_epoch=command.module_epoch and job.status='DONE' and job.phase='DONE'
               and job.owner_epoch>0 and job.owner_epoch<=ctl.active_owner_epoch
               and job.lease_nonce is null and job.lease_until_utc is null)))
         -- INITIAL_METADATA_PHASE_V2_END
       )
  then raise exception 'BPAY_NEXT_INITIAL_AUTH_NOT_CURRENT' using errcode='55000';end if;
  inventory:=private.weekly_source_effective_inventory_v1(t.timesheet_id);
  basis:=inventory->'approval_basis';origin:=basis->'origin';
  details:=jsonb_build_object('segments',f.invoice_breakdown_json->'segments','additional_units',f.additional_units_json,
    'actual_schedule',f.actual_schedule_json,'rate_source_refs',f.rate_source_refs_json);
  if inventory->>'ok' is distinct from 'true' or inventory->>'authority' is distinct from 'TSFIN'
     or inventory->>'head_id' is not null or jsonb_typeof(inventory->'components') is distinct from 'array'
     or jsonb_typeof(basis) is distinct from 'object' or basis->'coverage_complete' is distinct from 'true'::jsonb
     or basis is distinct from private.weekly_source_inventory_approval_basis_v2(t.timesheet_id,inventory)
     or origin is distinct from jsonb_build_object('kind','INITIAL_AUTHORISED_TSFIN_V1',
       'root_authorisation_id',a.id,'financial_snapshot_id',f.id,'authorisation_generation',a.authorisation_generation,
       'root_timesheet_id',t.timesheet_id,'root_version',t.version::text,'authorised_row_signature',a.authorised_row_signature,
       'financial_snapshot_digest',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_FINANCIAL_SNAPSHOT_V2',
         jsonb_build_object('policy',f.policy_snapshot_json,'details',details,'components',inventory->'components')),'hex'),
       'inventory_digest',basis->>'inventory_digest','entitlement_digest',basis->>'entitlement_digest')
     or basis->>'detail_digest' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',details),'hex')
     or basis->>'policy_fingerprint' is distinct from encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',f.policy_snapshot_json),'hex')
     or r.source_inventory_digest is distinct from decode(basis->>'inventory_digest','hex')
     or r.approved_source_ex_vat is distinct from (basis->>'approved_pay_ex_vat')::numeric
     or r.expected_line_count is distinct from jsonb_array_length(inventory->'components')
  then raise exception 'BPAY_NEXT_INITIAL_I1_CERTIFICATE_REQUIRED' using errcode='55000';end if;
  select count(*) into n from private.bpay_next_approved_line l where l.revision_id=r.id;
  if n<>r.expected_line_count or exists(select 1 from jsonb_array_elements(inventory->'components') component
      left join private.bpay_next_approved_line l on l.revision_id=r.id
        and l.source_component_id=(component.value->>'component_id')::uuid
        and l.component_key='SOURCE:'||(component.value->>'component_id')
      where l.id is null or l.line_no is distinct from (component.value->>'component_ordinal')::integer
        or l.evidence_ref is distinct from 'source-component:'||(component.value->>'component_id')||'#'||(component.value->>'component_sha256')
        or l.source_pay_ex_vat is distinct from case when (component.value->>'exclude_from_pay')::boolean then 0 else (component.value->>'pay_ex_vat')::numeric end
        or l.component_kind is distinct from case component.value->>'component_kind' when 'WORKED_TIME' then 'WORK' when 'ADDITIONAL_UNIT' then 'ADDITIONAL' else 'EXPENSE' end
        or l.tax_treatment is distinct from case when component.value->>'component_kind' in ('WORKED_TIME','ADDITIONAL_UNIT') then 'TAXABLE'::text
          when component.value->>'component_kind' in ('EXPENSE','SOURCE_FIXED_EXPENSE') then 'NON_TAXABLE'::text else null::text end)
  then raise exception 'BPAY_NEXT_INITIAL_SEALED_COMPONENTS_MISMATCH' using errcode='23514';end if;
  return jsonb_build_object('candidate_id',w.candidate_id,'work_id',w.id,'revision_id',r.id,'command_id',p.command_id,
    'source_channel',r.source_pay_channel,'origin',origin,'basis',basis,'components',inventory->'components',
    'certificate_digest',encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',basis),'hex'));
end
$function$;

create or replace function private.bpay_next_capture_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_row record;
  v_member record;
  v_seen integer:=0;
  v_selected_seen integer:=0;
  v_resolution_count integer:=0;
  v_managed_seen integer:=0;
  v_needs_resolution boolean;
  v_collection private.bpay_next_work_collection%rowtype;
  v_work_cursor uuid;
  v_component_cursor text;
  v_issue text;v_answered_source boolean;
  v_issue_work_id uuid;
  v_query_pin private.bpay_next_run_work_query_pin_v2%rowtype;
  v_binding private.bpay_next_source_component_binding_version%rowtype;v_book record;
  v_segments bigint;v_snooze jsonb;v_segment_result jsonb;v_segment_visited integer:=0;
  v_seed_effect uuid;v_seed_hold uuid;v_seed_holds uuid[];v_seed record;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CAPTURE_POSITION_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('RESERVE','ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_LEASE_STALE';
  end if;
  if v_job.phase='ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'replay',true,'rows_visited',0);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
      'replay',true,'rows_visited',0,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CAPTURE_POSITION_WORKER_NOT_PREPARING';
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  for v_member in
    select s.* from private.bpay_next_selected_position_keys_v1(v_worker.id,v_work_cursor,v_component_cursor,p_limit) s
  loop
    -- The Candidate control is already exclusively held. Lock this exact
    -- selected WORK before its position, matching the reentrant seed order.
    perform 1 from private.bpay_next_work w where w.id=v_member.work_id
      and w.candidate_id=v_worker.candidate_id for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_POSITION_WORK_SCOPE_INVALID';end if;
    -- Read and lock the exact position after choosing the bounded member.
    -- Do not FOR SHARE the nullable side of an outer join (PostgreSQL refuses
    -- that), and never turn a missing selected position into an omitted line.
    select v_member.run_work_id as run_work_id,v_member.work_id as work_id,
           v_member.captured_revision_id as captured_revision_id,
           p.component_key,p.applied_revision_id,
           p.approved_source_ex_vat,p.realised_source_ex_vat,
           p.held_source_ex_vat,p.realised_target_ex_vat,
           p.realised_target_vat,p.realised_target_inc_vat,
           p.held_target_ex_vat,p.held_target_vat,
           p.held_target_inc_vat,p.source_basis_channel,
           rev.source_pay_channel,rev.source_kind,rev.source_event_id,
           rev.source_head_id,rev.financial_snapshot_id,
           rev.physical_timesheet_id,rev.physical_timesheet_version,
           w.work_kind,w.candidate_id as origin_candidate_id,
           l.component_kind,l.id as approved_line_id,
           l.source_pay_ex_vat as approved_line_source_ex_vat,l.tax_treatment,
           p.source_book_state,p.source_book_revision
      into v_row
      from private.bpay_next_position p
      join private.bpay_next_work_revision rev
        on rev.id=v_member.captured_revision_id and rev.work_id=v_member.work_id
      join private.bpay_next_work w on w.id=rev.work_id
      left join private.bpay_next_approved_line l
        on l.revision_id=v_member.captured_revision_id
       and l.component_key=v_member.component_key
      where p.work_id=v_member.work_id and p.component_key=v_member.component_key
      for update of p;
    v_issue:=null;
    if not found
       or v_row.applied_revision_id is distinct from v_member.captured_revision_id then
      v_issue:='CAPTURE_POSITION_CHANGED';
    elsif v_row.approved_line_id is null
       and v_row.approved_source_ex_vat<>0 then
      v_issue:='POSITION_COMPONENT_EVIDENCE_MISSING';
    elsif v_row.approved_line_id is not null
       and v_row.approved_line_source_ex_vat is distinct from
         v_row.approved_source_ex_vat then
      v_issue:='POSITION_APPROVAL_AMOUNT_MISMATCH';
    end if;
    -- The Direct no-HEAD PROTECTED_WORK route retains its original restrictions.
    v_answered_source:=coalesce(v_row.work_kind='SOURCE'
      and (v_row.source_head_id is not null
        or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
          and v_row.financial_snapshot_id is not null))
      and v_row.source_pay_channel in ('PAYE','UMBRELLA')
      and ((v_row.component_kind in ('WORK','ADDITIONAL') and v_row.tax_treatment='TAXABLE')
        or (v_row.component_kind in ('EXPENSE','MILEAGE') and v_row.tax_treatment='NON_TAXABLE')),false);
    if v_worker.target_pay_channel='UMBRELLA' and v_issue is null and not v_answered_source and
       (v_row.source_pay_channel is distinct from 'UMBRELLA'
        or v_row.work_kind not in ('ORDINARY','SOURCE')
        or v_row.component_kind is distinct from 'WORK'
        or v_row.tax_treatment is distinct from 'TAXABLE') then
      v_issue:='UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
    if v_issue is not null then
      v_issue_work_id:=v_member.work_id;exit;
    end if;
    -- Typed initial I-1 is the ONLY new no-HEAD binding arm. Initial and
    -- common-HEAD captures require self-canonical, exactly classified Source components.
    v_binding:=null;v_book:=null;
    if v_row.work_kind='SOURCE' and (v_row.source_head_id is not null
      or (v_row.source_kind='SOURCE' and v_row.source_event_id is not null
        and v_row.financial_snapshot_id is not null)) then
      select v.* into v_binding from private.bpay_next_source_component_binding b
        join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
        where b.canonical_work_id=v_row.work_id and b.canonical_component_key=v_row.component_key
          and v.approval_work_id=v_row.work_id and v.approval_revision_id=v_row.captured_revision_id
          and v.approval_component_key=v_row.component_key
          and ((v_row.source_head_id is not null and v.origin_kind='COMMON_HEAD_RECEIPT_V1'
              and v.head_id=v_row.source_head_id)
            or (v_row.source_head_id is null and v.origin_kind='INITIAL_AUTHORISED_TSFIN_V1'
              and v.initial_root_authorisation_id=v_row.source_event_id
              and v.initial_financial_snapshot_id=v_row.financial_snapshot_id
              and v.initial_root_timesheet_id=v_row.physical_timesheet_id
              and v.initial_root_version=v_row.physical_timesheet_version));
      if v_binding.id is null or v_binding.publication_command_id_2 is not null
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or not v_answered_source then
        v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
      else
        if v_row.source_book_state='UNBOUND' then
          -- Capture is already a bounded selected point with real APPLIED/DONE
          -- authority. Adoption never borrows a queued writer's lease here.
          v_seed_effect:=null;v_seed_hold:=null;v_seed_holds:=null;
          if v_row.realised_source_ex_vat>0 then
            select b.original_effect_id into v_seed_effect from private.bpay_next_work_collection b
              where b.work_id=v_row.work_id and b.component_key=v_row.component_key
                and b.candidate_id=v_worker.candidate_id and b.paid_basis_qualified;
          end if;
          if v_row.held_source_ex_vat>0 then
            select pg_catalog.array_agg(h.id order by h.id) into v_seed_holds from (
              select id from private.bpay_next_hold where work_id=v_row.work_id
                and component_key=v_row.component_key and status='ACTIVE' order by id limit 2) h;
            if pg_catalog.cardinality(v_seed_holds) is distinct from 1 then
              raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ACTIVE_HOLD_WITNESS_AMBIGUOUS';
            end if;
            v_seed_hold:=v_seed_holds[1];
          end if;
          select seed.* into strict v_seed from private.bpay_next_source_disposition_seed_v1(
            v_row.work_id,v_row.component_key,v_row.captured_revision_id,v_seed_effect,v_seed_hold) seed;
          if v_seed.qualification_state is distinct from 'READY' then
            raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SELECTED_SEED_NOT_READY';
          end if;
          select p.source_book_state,p.source_book_revision into v_row.source_book_state,v_row.source_book_revision
            from private.bpay_next_position p where p.work_id=v_row.work_id and p.component_key=v_row.component_key;
        end if;
        select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
          v_binding.id,v_row.source_book_revision) s;
        if v_book.qualification_state is distinct from 'READY'
           or (v_book.candidate_id,v_book.canonical_work_id,v_book.canonical_component_key,
               v_book.approval_work_id,v_book.approval_revision_id,v_book.approved_line_id,
               v_book.source_channel,v_book.tax_treatment,v_book.approved_source_ex_vat,
               v_book.native_realised_paye,v_book.native_held_paye,v_book.native_realised_umbrella,v_book.native_held_umbrella)
             is distinct from (v_worker.candidate_id,v_row.work_id,v_row.component_key,
               v_row.work_id,v_row.captured_revision_id,v_row.approved_line_id,
               v_row.source_pay_channel,v_row.tax_treatment,v_row.approved_source_ex_vat,
               case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
               case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end) then
          v_issue:='SOURCE_BOOK_APPROVAL_UNBOUND';
        end if;
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    v_collection:=null;
    -- Only the exact typed original ordinary PAYE liability can own a
    -- negative residual. Unknown signed/cross-basis/held shapes still REVIEW.
    if v_worker.target_pay_channel='PAYE' and v_row.source_pay_channel='PAYE'
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat<0
       and (v_row.held_source_ex_vat,v_row.held_target_ex_vat,v_row.held_target_vat,v_row.held_target_inc_vat)
         is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
      select b.* into v_collection from private.bpay_next_work_collection b
        where b.work_id=v_row.work_id and b.component_key=v_row.component_key;
      if v_collection.case_id is not null then
        v_collection:=private.bpay_next_work_collection_origin_v1(
          v_collection.case_id,v_collection.case_component_id,v_worker.candidate_id);
      else v_collection:=null;end if;
      if v_collection.id is not null and
         (v_collection.observed_revision_id,v_collection.observed_approved_source_ex_vat,v_collection.observed_realised_source_ex_vat)
           is distinct from (v_row.captured_revision_id,v_row.approved_source_ex_vat,v_row.realised_source_ex_vat) then
        v_collection:=null;
      end if;
    end if;
    v_needs_resolution:=v_collection.id is null and (
       v_row.approved_line_id is null
       or v_row.approved_source_ex_vat<0
       or ((v_row.realised_source_ex_vat<>0
          or v_row.held_source_ex_vat<>0
          or v_row.realised_target_inc_vat<>0
          or v_row.held_target_inc_vat<>0)
         and v_row.source_basis_channel is distinct from v_row.source_pay_channel)
       or v_row.realised_source_ex_vat<0
       or v_row.realised_target_ex_vat<0 or v_row.realised_target_vat<0 or v_row.realised_target_inc_vat<0
       or (v_row.held_source_ex_vat>0 and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<=0)
       or (v_row.held_source_ex_vat=0 and v_row.held_target_inc_vat<>0)
       or v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat<0);
    -- Only genuine positive Source WORK consumes the retained disposition.
    -- Expense/additional/ordinary, zero, realised/held-away, managed negative
    -- and already-unresolved financial shapes do not become query holds.
    -- The actual pin validated the typed origin before this immutable row;
    -- missing evidence is REVIEW, never a lazy live Source reader fallback.
    if v_row.work_kind='SOURCE' and v_row.source_kind in ('SOURCE','PROTECTED')
       and v_row.source_event_id is not null
       and v_row.component_kind in ('WORK','PROTECTED_WORK')
       and not v_needs_resolution
       and v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat>0 then
      select qp.* into v_query_pin from private.bpay_next_run_work_query_pin_v2 qp
        where qp.run_work_id=v_row.run_work_id;
      if found and (private.bpay_next_prepare_job_run_v1(v_query_pin.prepare_job_id) is distinct from v_run_id
        or (v_query_pin.run_worker_id,v_query_pin.candidate_id,
          v_query_pin.work_id,v_query_pin.captured_revision_id,
          v_query_pin.module_epoch,v_query_pin.gate_version)
          is distinct from (v_worker.id,v_worker.candidate_id,
          v_row.work_id,v_row.captured_revision_id,v_module_epoch,'PAY_QUERY_STATE_V3'::text)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_MISMATCH';
      end if;
      if not found or v_query_pin.disposition='UNAVAILABLE' then
        v_issue:='SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE';
      elsif v_query_pin.disposition='BLOCKED' then
        -- Only the original cut's complete, exact named shift exclusions
        -- can bypass a whole-Work blocker. Never read today's query here.
        if not exists(select 1 from private.bpay_next_run_work_query_snapshot snapshot
          where snapshot.run_id=v_run_id and snapshot.work_id=v_row.work_id
            and snapshot.expected_revision_id=v_row.captured_revision_id
            and snapshot.physical_timesheet_id::text=(v_query_pin.scope->>'root_timesheet_id')
            and jsonb_typeof(snapshot.segment_query_exclusions)='array'
            and jsonb_array_length(snapshot.segment_query_exclusions)>0)
        then v_issue:='SOURCE_PAY_QUERY_OPEN';end if;
      elsif v_query_pin.disposition<>'CLEAR' or v_query_pin.scope is null
         or v_query_pin.query_state_sha256 is null
         or pg_catalog.octet_length(v_query_pin.query_state_sha256)<>32 then
        raise exception using errcode='23514',message='BPAY_NEXT_CAPTURE_QUERY_PIN_INVALID';
      end if;
      if v_issue is not null then
        v_issue_work_id:=v_row.work_id;exit;
      end if;
    end if;
    select count(*) into v_segments from (
      select s.segment_key from private.bpay_next_approved_segment s
        where s.revision_id=v_row.captured_revision_id and s.component_key=v_row.component_key
      union select s.segment_key from private.bpay_next_work_segment_origin_share s
        where s.work_id=v_row.work_id and s.component_key=v_row.component_key) members;
    if v_segments>0 and v_row.work_kind='ORDINARY' then
      -- Exact segment negative ownership is sealed after complete child capture.
      -- Initial native net-negative classification cannot bypass that census.
      if v_collection.id is not null then v_needs_resolution:=true;end if;
      v_collection:=null;
    end if;
    v_snooze:=null;
    if v_segments=0 then
      v_snooze:=private.bpay_next_snooze_at_cut_v1(v_row.work_id,v_row.captured_revision_id,
        null,v_row.component_key,v_run.snooze_control_sequence,v_run.snooze_business_date);
    end if;
    insert into private.bpay_next_run_position
      (run_worker_id,run_work_id,work_id,captured_revision_id,
       component_key,approved_line_id,source_pay_channel,
       source_basis_channel,
       approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,
       residual_source_ex_vat,realised_target_ex_vat,realised_target_vat,
       realised_target_inc_vat,held_target_ex_vat,held_target_vat,
       held_target_inc_vat,needs_financial_resolution,work_collection_id,work_collection_revision,
       source_binding_version_id,canonical_source_work_id,canonical_source_component_key,source_book_revision,
       segment_expected_member_count,segment_attribution_complete,snooze_id,snooze_version,snooze_excluded_positive_ex_vat)
      values(v_worker.id,v_row.run_work_id,v_row.work_id,
       v_row.captured_revision_id,v_row.component_key,v_row.approved_line_id,
       v_row.source_pay_channel,v_row.source_basis_channel,
       v_row.approved_source_ex_vat,
       v_row.realised_source_ex_vat,v_row.held_source_ex_vat,
       v_row.approved_source_ex_vat-v_row.realised_source_ex_vat
         -v_row.held_source_ex_vat,
       v_row.realised_target_ex_vat,v_row.realised_target_vat,
       v_row.realised_target_inc_vat,v_row.held_target_ex_vat,
       v_row.held_target_vat,v_row.held_target_inc_vat,
       v_needs_resolution,v_collection.id,v_collection.reconcile_revision,
       v_binding.id,v_binding.canonical_work_id,v_binding.canonical_component_key,
       case when v_binding.id is null then null else v_row.source_book_revision end,
       v_segments,v_segments=0,(v_snooze->>'id')::uuid,(v_snooze->>'version')::bigint,
       case when v_snooze is not null then greatest(v_row.approved_source_ex_vat-v_row.realised_source_ex_vat-v_row.held_source_ex_vat,0) else 0 end);
    if v_needs_resolution then
      v_resolution_count:=v_resolution_count+1;
    end if;
    if v_collection.id is not null then v_managed_seen:=v_managed_seen+1;end if;
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
    if v_member.selection_mode='SUBSET' then
      v_selected_seen:=v_selected_seen+1;
    end if;
  end loop;
  if v_resolution_count>0 then
    update private.bpay_next_run_worker
      set financial_resolution_count=financial_resolution_count+v_resolution_count
      where id=v_worker.id;
  end if;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_position_count=captured_position_count+v_seen,
          expected_managed_position_count=expected_managed_position_count+v_managed_seen,
          captured_selected_component_count=
            captured_selected_component_count+v_selected_seen
      where id=v_worker.id;
  end if;
  update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
  if v_issue is not null then
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=v_issue_work_id where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
  end if;
  if v_seen<p_limit then
    if v_worker.captured_selected_component_count+v_selected_seen
       <>v_worker.expected_selected_component_count then
      select rw.work_id into strict v_issue_work_id
        from private.bpay_next_run_work rw
        join private.bpay_next_work_choice choice
          on choice.run_id=v_run_id and choice.work_id=rw.work_id
         and choice.selection_mode='SUBSET'
        where rw.run_worker_id=v_worker.id
        order by rw.work_id limit 1;
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='CAPTURE_POSITION_CHANGED',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen);
    end if;
    v_segment_result:=private.bpay_next_capture_segment_page_v1(v_worker.id,p_limit-v_seen);
    v_segment_visited:=(v_segment_result->>'rows_visited')::integer;
    select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker.id;
    if v_worker.status='REVIEW' then
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen+v_segment_visited);
    end if;
    if not (v_segment_result->>'done')::boolean then
      update private.bpay_next_job set position_work_cursor=v_work_cursor,position_component_cursor=v_component_cursor where id=p_job_id;
      return jsonb_build_object('done',false,'phase','RESERVE','rows_visited',v_seen+v_segment_visited,
        'work_cursor',v_work_cursor,'component_cursor',v_component_cursor,'replay',false);
    end if;
    update private.bpay_next_job
      set phase='ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,'phase','ALLOCATE',
      'rows_visited',v_seen+v_segment_visited,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','RESERVE',
    'rows_visited',v_seen+v_segment_visited,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;

create or replace function private.bpay_next_hold_fresh_position_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_work_cursor uuid,p_expected_component_cursor text,
  p_limit integer default 100
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_module_epoch bigint;
  v_issue_work_id uuid;
  v_row record;
  v_position private.bpay_next_position%rowtype;
  v_value record;v_book record;
  v_reimbursement_window uuid;v_reimbursement_vat_pct numeric;v_reimbursement_window_count bigint;v_reimbursement_amounts jsonb;
  v_run_line_id uuid;v_hold_id uuid;v_included_source numeric(18,2);v_freeze_zero boolean;
  v_seen integer:=0;
  v_affected_work_ids uuid[]:=array[]::uuid[];
  v_work_cursor uuid;
  v_component_cursor text;
  v_line_count bigint;
  v_managed_count bigint;
  v_total_ex numeric(18,2);
  v_total_vat numeric(18,2);
  v_total_inc numeric(18,2);
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit not between 1 and 100
     or (p_expected_work_cursor is null and p_expected_component_cursor is not null) then
    raise exception using errcode='22023',message='BPAY_NEXT_HOLD_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id) into strict v_run_id
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_module_epoch
    from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_module_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select candidate_id into strict v_job.candidate_id
    from private.bpay_next_job where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_job.candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED'
     or v_job.job_kind<>'PREPARE' or v_job.status<>'LEASED'
     or v_job.phase not in ('ALLOCATE','FINANCE_ALLOCATE')
     or v_job.module_epoch<>v_module_epoch
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000',message='BPAY_NEXT_HOLD_LEASE_STALE';
  end if;
  if v_job.phase='FINANCE_ALLOCATE' then
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',0,'replay',true);
  end if;
  if v_job.position_work_cursor is distinct from p_expected_work_cursor
     or v_job.position_component_cursor is distinct from p_expected_component_cursor then
    return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
      'rows_visited',0,'replay',true,
      'work_cursor',v_job.position_work_cursor,
      'component_cursor',v_job.position_component_cursor);
  end if;
  select * into strict v_worker from private.bpay_next_run_worker
    where run_id=v_run_id and candidate_id=v_job.candidate_id for update;
  if v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null then
    raise exception using errcode='55000',
      message='BPAY_NEXT_HOLD_WORKER_NOT_PREPARING';
  end if;
  if v_worker.financial_resolution_count>0 then
    select rp.work_id into strict v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id and rp.needs_financial_resolution
      order by rp.work_id,rp.component_key limit 1;
    update private.bpay_next_run_worker
      set status='REVIEW',review_issue_code='FINANCIAL_RESOLUTION_REQUIRED',
          review_issue_work_id=v_issue_work_id
      where id=v_worker.id;
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
  end if;
  if v_worker.target_pay_channel='UMBRELLA'
     and (v_worker.target_umbrella_id is null
       or v_worker.target_umbrella_vat_chargeable is null) then
    select rp.work_id into v_issue_work_id
      from private.bpay_next_run_position rp
      where rp.run_worker_id=v_worker.id
      order by rp.work_id,rp.component_key limit 1;
    if v_issue_work_id is not null then
      update private.bpay_next_run_worker
        set status='REVIEW',review_issue_code='TARGET_UMBRELLA_MISSING',
            review_issue_work_id=v_issue_work_id
        where id=v_worker.id;
      return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,0);
    end if;
  end if;
  v_work_cursor:=v_job.position_work_cursor;
  v_component_cursor:=v_job.position_component_cursor;
  v_line_count:=v_worker.captured_line_count;
  v_managed_count:=v_worker.captured_managed_position_count;
  v_total_ex:=v_worker.gross_ex_vat;
  v_total_vat:=v_worker.gross_vat;
  v_total_inc:=v_worker.gross_inc_vat;
  for v_row in
    select rp.*,rw.id as pinned_work_id,rev.valuation_policy_id,
      w.work_kind as captured_work_kind,l.component_kind as captured_component_kind,l.tax_treatment as captured_tax_treatment
      from private.bpay_next_run_position rp
      join private.bpay_next_run_work rw on rw.id=rp.run_work_id
      join private.bpay_next_work_revision rev
        on rev.id=rp.captured_revision_id and rev.work_id=rp.work_id
      join private.bpay_next_work w on w.id=rp.work_id
      left join private.bpay_next_approved_line l on l.id=rp.approved_line_id
        and l.revision_id=rp.captured_revision_id and l.component_key=rp.component_key
      where rp.run_worker_id=v_worker.id
        and (v_work_cursor is null or rp.work_id>v_work_cursor
          or (rp.work_id=v_work_cursor
            and rp.component_key>v_component_cursor))
      order by rp.work_id,rp.component_key limit p_limit
  loop
    if not v_row.segment_attribution_complete then
      raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_CAPTURE_INCOMPLETE';
    end if;
    v_included_source:=case when v_row.segment_member_count>0 then v_row.segment_included_positive_ex_vat
      else greatest(v_row.residual_source_ex_vat,0)-v_row.snooze_excluded_positive_ex_vat end;
    v_freeze_zero:=v_row.approved_line_id is not null and v_row.approved_source_ex_vat=0 and v_row.realised_source_ex_vat=0 and v_row.held_source_ex_vat=0
      and v_row.snooze_id is null and (v_row.segment_member_count=0 or exists(
        select 1 from private.bpay_next_run_work_segment_parent p join private.bpay_next_run_work_segment s on s.id=p.run_work_segment_id
          where p.run_work_id=v_row.run_work_id and p.component_key=v_row.component_key and s.disposition='INCLUDED'));
    if v_row.needs_financial_resolution
       or (v_row.work_collection_id is null and
         ((v_row.approved_line_id is null and (v_row.segment_member_count=0 or v_included_source>0)) or v_row.residual_source_ex_vat<0)) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_HOLD_UNEXPECTED_POSITION_SHAPE';
    end if;
    select * into strict v_position from private.bpay_next_position
      where work_id=v_row.work_id and component_key=v_row.component_key
      for update;
    if v_position.applied_revision_id is distinct from v_row.captured_revision_id
       or v_position.approved_source_ex_vat is distinct from
         v_row.approved_source_ex_vat
       or v_position.realised_source_ex_vat is distinct from
         v_row.realised_source_ex_vat
       or v_position.held_source_ex_vat is distinct from
         v_row.held_source_ex_vat
       or v_position.source_basis_channel is distinct from
         v_row.source_basis_channel
       or v_position.realised_target_ex_vat is distinct from
         v_row.realised_target_ex_vat
       or v_position.realised_target_vat is distinct from
         v_row.realised_target_vat
       or v_position.realised_target_inc_vat is distinct from
         v_row.realised_target_inc_vat
       or v_position.held_target_ex_vat is distinct from
         v_row.held_target_ex_vat
       or v_position.held_target_vat is distinct from
         v_row.held_target_vat
       or v_position.held_target_inc_vat is distinct from
         v_row.held_target_inc_vat then
      raise exception using errcode='55000',
        message='BPAY_NEXT_HOLD_CURRENT_POSITION_CHANGED';
    end if;
    if v_row.source_binding_version_id is not null then
      select s.* into strict v_book from private.bpay_next_bound_source_snapshot_v1(
        v_row.source_binding_version_id,v_row.source_book_revision) s;
      if v_book.qualification_state is distinct from 'READY'
         or (v_book.canonical_work_id,v_book.canonical_component_key,v_book.approval_work_id,
             v_book.approval_revision_id,v_book.approved_line_id,v_book.source_channel,v_book.tax_treatment,
             v_book.approved_source_ex_vat,v_book.native_realised_paye,v_book.native_held_paye,
             v_book.native_realised_umbrella,v_book.native_held_umbrella)
           is distinct from (v_row.work_id,v_row.component_key,v_row.work_id,
             v_row.captured_revision_id,v_row.approved_line_id,v_row.source_pay_channel,v_row.captured_tax_treatment,
             v_row.approved_source_ex_vat,
             case when v_row.source_pay_channel='PAYE' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='PAYE' then v_row.held_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.realised_source_ex_vat else 0::numeric end,
             case when v_row.source_pay_channel='UMBRELLA' then v_row.held_source_ex_vat else 0::numeric end)
         or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
         or coalesce(v_row.captured_work_kind='SOURCE'
           and ((v_row.captured_component_kind in ('WORK','ADDITIONAL') and v_row.captured_tax_treatment='TAXABLE')
             or (v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE')),false) is not true
         or v_row.canonical_source_work_id is distinct from v_row.work_id
         or v_row.canonical_source_component_key is distinct from v_row.component_key then
        raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED';
      end if;
    end if;
    if v_row.work_collection_id is not null then
      -- Classification and original provenance were frozen by RESERVE. Never
      -- reread a newer WORK approval or create an ordinary negative/zero hold.
      update private.bpay_next_run_position set managed_capture_completed=true
        where id=v_row.id and not managed_capture_completed;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_ALREADY_SPENT';end if;
      v_managed_count:=v_managed_count+1;
    end if;
    if v_included_source>0 or v_freeze_zero then
    if v_row.source_binding_version_id is not null and v_row.captured_work_kind='SOURCE'
      and v_row.captured_component_kind in ('EXPENSE','MILEAGE') and v_row.captured_tax_treatment='NON_TAXABLE' then
      -- Source reimbursement keeps its original gross; only ordinary line VAT applies.
      -- Use the same immutable winning policy window and existing VAT calculator.
      if v_included_source is null or v_row.valuation_policy_id is null or v_run.pay_date is null
        or v_row.source_pay_channel is null or v_row.source_pay_channel not in ('PAYE','UMBRELLA')
        or v_worker.target_pay_channel is null or v_worker.target_pay_channel not in ('PAYE','UMBRELLA')
        or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_vat_chargeable is null)
        or v_included_source<>pg_catalog.round(v_included_source,2) then
        raise exception using errcode='22023',message='BPAY_NEXT_TARGET_VALUATION_INPUT_INVALID';end if;
      v_reimbursement_window:=null;v_reimbursement_vat_pct:=null;v_reimbursement_window_count:=null;
      select w.source_window_id,w.vat_rate_pct,count(*) over(partition by w.date_from)
        into v_reimbursement_window,v_reimbursement_vat_pct,v_reimbursement_window_count
        from private.bpay_next_valuation_policy_window w
        where w.policy_id=v_row.valuation_policy_id and v_run.pay_date>=w.date_from
          and v_run.pay_date<=coalesce(w.date_to,'infinity'::date)
        order by w.date_from desc,w.source_window_id desc limit 1;
      if v_reimbursement_window is null then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING';end if;
      if v_reimbursement_window_count<>1 then
        raise exception using errcode='23514',message='BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS';end if;
      if v_worker.target_pay_channel='UMBRELLA' then
        v_reimbursement_amounts:=public._pay_umbrella_vat_calc(v_included_source,
          v_reimbursement_vat_pct,v_worker.target_umbrella_vat_chargeable);
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          (v_reimbursement_amounts->>'vat')::numeric as target_vat,
          (v_reimbursement_amounts->>'inc')::numeric as target_inc_vat into strict v_value;
      else
        select v_reimbursement_window as policy_window_id,v_included_source as target_ex_vat,
          0::numeric as target_vat,v_included_source as target_inc_vat into strict v_value;
      end if;
    else
    select * into strict v_value
      from private.bpay_next_value_source_for_target_v1(
        v_included_source,v_row.source_pay_channel,
        v_worker.target_pay_channel,
        case when v_worker.target_pay_channel='UMBRELLA'
          then v_worker.target_umbrella_vat_chargeable else false end,
        v_row.valuation_policy_id,v_run.pay_date);
    end if;
    v_line_count:=v_line_count+1;
    insert into private.bpay_next_run_line
      (run_worker_id,run_work_id,line_no,approved_line_id,
       captured_revision_id,work_id,component_key,source_consumed_ex_vat,
       source_pay_channel,target_pay_channel,valuation_policy_id,
       policy_window_id,frozen_ex_vat,frozen_vat,frozen_inc_vat,source_binding_version_id,
       canonical_source_work_id,canonical_source_component_key,source_book_revision,updates_original_source_scalar)
      values(v_worker.id,v_row.run_work_id,v_line_count,
       v_row.approved_line_id,v_row.captured_revision_id,v_row.work_id,
       v_row.component_key,v_included_source,
       v_row.source_pay_channel,v_worker.target_pay_channel,
       v_row.valuation_policy_id,v_value.policy_window_id,
       v_value.target_ex_vat,v_value.target_vat,v_value.target_inc_vat,
       v_row.source_binding_version_id,v_row.canonical_source_work_id,v_row.canonical_source_component_key,
       v_row.source_book_revision,case when v_row.source_binding_version_id is null then null else true end)
      returning id into v_run_line_id;
    -- An authorised zero still has immutable shift/rate advice detail. It
    -- becomes a frozen zero line, never a zero-valued financial hold.
    if v_included_source>0 then
      insert into private.bpay_next_hold
        (run_line_id,work_id,component_key,source_reserved_ex_vat,
         target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,status)
        values(v_run_line_id,v_row.work_id,v_row.component_key,
         v_included_source,v_value.target_ex_vat,
         v_value.target_vat,v_value.target_inc_vat,'ACTIVE') returning id into v_hold_id;
      perform private.bpay_next_record_work_segment_origin_shares_v1(v_run_line_id,v_hold_id);
      update private.bpay_next_position
        set held_source_ex_vat=held_source_ex_vat+v_included_source,
            held_target_ex_vat=held_target_ex_vat+v_value.target_ex_vat,
            held_target_vat=held_target_vat+v_value.target_vat,
            held_target_inc_vat=held_target_inc_vat+v_value.target_inc_vat,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where work_id=v_row.work_id and component_key=v_row.component_key;
    end if;
    v_total_ex:=v_total_ex+v_value.target_ex_vat;
    v_total_vat:=v_total_vat+v_value.target_vat;
    v_total_inc:=v_total_inc+v_value.target_inc_vat;
    end if;
    v_affected_work_ids:=array_append(v_affected_work_ids,v_row.work_id);
    v_work_cursor:=v_row.work_id;
    v_component_cursor:=v_row.component_key;
    v_seen:=v_seen+1;
  end loop;
  if v_seen>0 then
    update private.bpay_next_run_worker
      set captured_line_count=v_line_count,
          captured_managed_position_count=v_managed_count,
          gross_ex_vat=v_total_ex,gross_vat=v_total_vat,
          gross_inc_vat=v_total_inc
      where id=v_worker.id;
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_job.candidate_id;
    perform private.bpay_next_workbench_refresh_many_v1(v_affected_work_ids,null);
  end if;
  if v_seen<p_limit then
    if v_managed_count<>v_worker.expected_managed_position_count or exists(
      select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
        and work_collection_id is not null and not managed_capture_completed) then
      raise exception using errcode='23514',message='BPAY_NEXT_COLLECTION_CAPTURE_COUNT_MISMATCH';
    end if;
    update private.bpay_next_job
      set phase='FINANCE_ALLOCATE',position_work_cursor=null,
          position_component_cursor=null
      where id=p_job_id;
    return pg_catalog.jsonb_build_object('done',true,
      'phase','FINANCE_ALLOCATE','rows_visited',v_seen,'replay',false);
  end if;
  update private.bpay_next_job
    set position_work_cursor=v_work_cursor,
        position_component_cursor=v_component_cursor
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('done',false,'phase','ALLOCATE',
    'rows_visited',v_seen,'work_cursor',v_work_cursor,
    'component_cursor',v_component_cursor,'replay',false);
end
$function$;
alter function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_initial_tsfin_certificate_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_initial_tsfin_certificate_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) owner to postgres;
revoke all on function private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
