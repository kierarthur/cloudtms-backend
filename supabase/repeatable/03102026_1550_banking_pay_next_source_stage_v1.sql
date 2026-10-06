-- Convert one Source-owned, already final approval into the small immutable
-- Banking Pay revision. The Source owner calls this only at its successful
-- outer boundary; this routine never chooses a Source decision or approves a
-- separate expense-claim Timesheet.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_stage_source_current_v1(
  p_root_timesheet_id uuid, p_source_event_id uuid, p_expected_head_id uuid
) returns table(work_id uuid,revision_id uuid,source_event_id uuid)
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
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
        case when v_component_kind='WORKED_TIME' then 'TAXABLE' else null end,
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
      insert into private.bpay_next_shift_detail
        (approved_line_id,detail_no,work_date,shift_start_at,shift_end_at,
         shift_start_local,shift_end_local,shift_overnight,
         approved_hours,
         detail_label,immutable_evidence_ref)
        values(v_line_id,1,(v_component->>'work_date')::date,
          case when v_use_segment then nullif(v_segment->>'start_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'end_utc','')::timestamptz end,
          case when v_use_segment then nullif(v_segment->>'start','') end,
          case when v_use_segment then nullif(v_segment->>'end','') end,
          case when v_use_segment then (v_segment->>'overnight')::boolean end,
          v_hours,
          nullif(v_component->>'reference_number',''),
          'source-component:'||v_component_id::text)
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

alter function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_stage_source_current_v1(uuid,uuid,uuid)
  from public, anon, authenticated, service_role;

commit;
