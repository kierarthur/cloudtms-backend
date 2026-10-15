set local jit=off;
set local pg_show_plans.is_enabled=off;
do $vacancy$ begin if exists(select 1 from unnest(array['private.weekly_source_nhsp_paid_overlap_basis_v1(uuid,uuid,uuid)','private.bpay_next_source_nhsp_magnitude_v1(jsonb,jsonb,jsonb)','private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid)','private.weekly_source_native_line_vat_v1(bigint,boolean,numeric)','private.weekly_source_nhsp_price_admission_v1(uuid,uuid)','private.weekly_source_nhsp_invoice_values_v1(uuid,uuid)']) id where to_regprocedure(id) is not null) then raise exception 'HR_NHSP_NEW_HELPER_NOT_VACANT';end if;end $vacancy$;
-- Source-purpose native INPUT only. All current selection and paid facts
-- remain delegated to their genuine native owners. No synthetic snapshot,
-- no-reversal claim, monetary calculation or paid-writer permission.
create or replace function private.weekly_source_nhsp_paid_overlap_basis_v1(
  p_work_event_id uuid,p_actor_user_id uuid,p_source_cycle_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_event public.weekly_work_events%rowtype;v_cycle public.weekly_source_cycles%rowtype;
  v_root public.timesheets%rowtype;v_contract public.contracts%rowtype;
  v_movement public.weekly_source_billing_movements%rowtype;v_resolution public.weekly_source_row_resolutions%rowtype;
  v_row public.weekly_source_upload_rows%rowtype;v_upload public.weekly_source_uploads%rowtype;
  v_roots uuid[];v_root_id uuid;v_scope uuid;v_scope_count integer;v_native jsonb;
  v_payment jsonb;v_rows jsonb:='[]'::jsonb;v_result jsonb;v_complete boolean:=true;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_work_event_id is null or p_actor_user_id is null or p_source_cycle_id is null then
    raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_REQUEST_INVALID' using errcode='22023';end if;
  if private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
    raise exception 'WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED' using errcode='42501';end if;
  select * into strict v_event from public.weekly_work_events where id=p_work_event_id;
  select * into strict v_cycle from public.weekly_source_cycles where id=p_source_cycle_id;
  if v_event.first_source_group_id is distinct from v_cycle.source_group_id
    or (v_cycle.scope_client_id is not null and v_cycle.scope_client_id is distinct from v_event.client_id)
    or not exists(select 1 from public.weekly_source_groups g
      where g.id=v_cycle.source_group_id and g.active and g.source_family='NHSP') then
    raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_SCOPE_FORBIDDEN' using errcode='42501';end if;
  perform private.weekly_source_office_authority_v1(p_actor_user_id,'VIEW_PAY_WORK',
    v_cycle.source_group_id,v_event.client_id,v_cycle.finalisation_week_ending);
  if not private.weekly_source_office_next_owner_v1() then
    v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',
      'state','UNAVAILABLE','reason','NEXT_NOT_ACTIVE','work_event_id',v_event.id,
      'candidate_id',v_event.candidate_id,'client_id',v_event.client_id,'root_timesheet_ids',null,
      'source_cycle_id',v_cycle.id,'source_group_id',v_cycle.source_group_id,
      'native_current_rows',null,'native_payment_facts',null,
      'no_reversal_proved',null,'paid_writer_permission',null);
    return v_result||jsonb_build_object('inputs_sha256',encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',v_result),'hex'));end if;
  -- This is the single existing Source payment classifier/Original reader.
  -- Its present CYCLE history field is not relabelled a NHSP current head.
  -- The actual NHSP MOVEMENT selection below remains separately identified.
  v_payment:=private.weekly_source_work_event_payment_facts_v1(v_event.id,p_actor_user_id,v_cycle.id);
  select coalesce(array_agg(distinct family.canonical_timesheet_id order by family.canonical_timesheet_id),'{}'::uuid[])
    into v_roots from public.weekly_source_row_timesheet_lineages lineage
    cross join lateral public._pay_timesheet_rotation_scope(array[lineage.timesheet_id]) family
    where lineage.work_event_id=v_event.id and lineage.candidate_id=v_event.candidate_id
      and lineage.client_id=v_event.client_id;
  if cardinality(v_roots)=0 or cardinality(v_roots)>200
    or to_jsonb(v_roots) is distinct from v_payment->'root_timesheet_ids' then
    raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_ROOT_UNBOUND' using errcode='55000';end if;
  foreach v_root_id in array v_roots loop
    select * into strict v_root from public.timesheets t where t.timesheet_id=v_root_id;
    select * into strict v_contract from public.contracts c where c.id=v_root.contract_id;
    if not v_root.is_current or v_root.revoked_at is not null or v_root.archived_at_utc is not null
      or v_root.sheet_scope<>'WEEKLY' or v_root.line_type<>'HOURS'
      or (v_contract.candidate_id,v_contract.client_id) is distinct from (v_event.candidate_id,v_event.client_id)
      or v_event.work_date not between v_root.week_ending_date-6 and v_root.week_ending_date
      or v_event.work_date not between v_contract.start_date and coalesce(v_contract.end_date,'infinity'::date)
      or not exists(select 1 from public.weekly_source_group_clients membership
        where membership.source_group_id=v_event.first_source_group_id and membership.client_id=v_contract.client_id
          and v_root.week_ending_date between membership.valid_from and coalesce(membership.valid_to,'infinity'::date)) then
      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_ROOT_FORBIDDEN' using errcode='42501';end if;
    perform private.weekly_source_office_authority_v1(p_actor_user_id,'VIEW_PAY_WORK',
      v_event.first_source_group_id,v_contract.client_id,v_root.week_ending_date);
    select count(*),min(scope.id::text)::uuid into v_scope_count,v_scope
      from private.bpay_next_source_current_scopes scope where scope.root_timesheet_id=v_root_id;
    if v_scope_count>1 then
      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_SCOPE_AMBIGUOUS' using errcode='55000';end if;
    if v_scope_count=0 or not exists(select 1 from private.bpay_next_source_current_events ce
      where ce.scope_id=v_scope and ce.work_event_id=v_event.id) then
      -- Uncaptured does not mean absent/unpaid/no reversal. Preserve it as
      -- an explicit unavailable native position; no history fallback occurs.
      v_complete:=false;
      v_rows:=v_rows||jsonb_build_array(jsonb_build_object('root_timesheet_id',v_root_id,
        'state','UNAVAILABLE','reason','NATIVE_CURRENT_UNCAPTURED','native_current_event',null));
      continue;
    end if;
    if not exists(select 1 from private.bpay_next_source_current_scopes scope where scope.id=v_scope
      and scope.root_timesheet_id=v_root_id and scope.family_booking_id=v_root.booking_id
      and scope.source_group_id=v_event.first_source_group_id
      and scope.candidate_id=v_contract.candidate_id and scope.client_id=v_contract.client_id
      and scope.contract_id=v_contract.id and scope.week_ending_date=v_root.week_ending_date) then
      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_SCOPE_UNBOUND' using errcode='55000';end if;
    -- The native pure reader itself revalidates genuine CURRENT observation,
    -- magnitude ambiguity, selected movement and immutable lineage/economics.
    v_native:=private.bpay_next_source_current_event_v1(v_root_id,v_event.id);
    if v_native->>'schema_version' is distinct from 'BPAY_NEXT_SOURCE_CURRENT_EVENT_V1'
      or v_native->>'root_timesheet_id' is distinct from v_root_id::text
      or v_native->>'work_event_id' is distinct from v_event.id::text
      or v_native->>'source_kind' is distinct from 'NHSP'
      or v_native->'source_observed' is distinct from 'true'::jsonb
      or jsonb_typeof(v_native->'source_present') is distinct from 'boolean' then
      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_NATIVE_UNBOUND' using errcode='55000';end if;
    if v_native->'source_present'='true'::jsonb then
      select * into strict v_movement from public.weekly_source_billing_movements m
        where m.id=(v_native->>'movement_id')::uuid;
      select * into strict v_resolution from public.weekly_source_row_resolutions r
        where r.id=(v_native->>'row_resolution_id')::uuid;
      select * into strict v_row from public.weekly_source_upload_rows r where r.id=v_resolution.upload_row_id;
      select * into strict v_upload from public.weekly_source_uploads u where u.id=v_row.upload_id;
      if v_movement.source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' or v_movement.pay_effect<>'NORMAL'
        or v_movement.source_line_kind<>'NHSP_PHYSICAL_POSITIVE' or v_movement.movement_role not in ('POSITIVE','REPLACEMENT')
        or v_movement.work_event_id is distinct from v_event.id
        or (v_movement.candidate_id,v_movement.actual_client_id,v_movement.contract_id) is distinct from
          (v_contract.candidate_id,v_contract.client_id,v_contract.id)
        or v_movement.final_revision_id::text is distinct from v_native->>'final_revision_id'
        or v_movement.source_facts_json->>'row_resolution_id' is distinct from v_resolution.id::text
        or v_resolution.mapping_state<>'RESOLVED' or v_resolution.work_event_id is distinct from v_event.id
        or (v_resolution.candidate_id,v_resolution.client_id,v_resolution.contract_id) is distinct from
          (v_contract.candidate_id,v_contract.client_id,v_contract.id)
        or v_row.work_date is distinct from v_event.work_date or v_row.row_finalisation_state<>'SOURCE_WORKED'
        or not exists(select 1 from public.weekly_source_format_profiles profile
          where profile.id=v_upload.source_format_profile_id and profile.profile_code='NHSP_FINAL_BACKING_V1')
        or not exists(select 1 from public.weekly_source_row_timesheet_lineages lineage
          join public.weekly_source_row_economic_snapshots economic on economic.row_resolution_id=lineage.row_resolution_id
          where lineage.id=(v_native->>'lineage_id')::uuid and lineage.row_resolution_id=v_resolution.id
            and lineage.work_event_id=v_event.id and lineage.family_booking_id=v_root.booking_id
            and (lineage.candidate_id,lineage.client_id,lineage.contract_id,lineage.week_ending_date) is not distinct from
              (v_contract.candidate_id,v_contract.client_id,v_contract.id,v_root.week_ending_date)
            and economic.id=(v_native->>'economic_snapshot_id')::uuid and economic.work_event_id=v_event.id)
        or not exists(select 1 from private.bpay_next_source_current_origins origin
          where origin.origin_kind='MOVEMENT' and origin.origin_id=v_movement.id and origin.movement_id=v_movement.id
            and origin.scope_id=v_scope and origin.work_event_id=v_event.id and origin.active
            and origin.final_revision_id=v_movement.final_revision_id and origin.row_resolution_id=v_resolution.id
            and origin.source_hash=v_movement.movement_economic_hash and origin.contribution=1) then
        raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_MOVEMENT_UNBOUND' using errcode='55000';end if;
    elsif v_native->'movement_id' is distinct from 'null'::jsonb then
      raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_NATIVE_UNBOUND' using errcode='55000';end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object('root_timesheet_id',v_root_id,
      'state','CURRENT_NATIVE_POSITION','native_current_event',v_native));
  end loop;
  v_result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',
    'state',case when v_complete then 'NATIVE_POSITION_INPUTS' else 'UNAVAILABLE' end,
    'reason',case when v_complete then null else 'NATIVE_CURRENT_UNCAPTURED' end,
    'work_event_id',v_event.id,'candidate_id',v_event.candidate_id,'client_id',v_event.client_id,
    'source_cycle_id',v_cycle.id,'source_group_id',v_cycle.source_group_id,'root_timesheet_ids',to_jsonb(v_roots),
    'native_current_rows',v_rows,'native_payment_facts',v_payment,
    'no_reversal_proved',null,'paid_writer_permission',null);
  if octet_length(v_result::text)>524288 then
    raise exception 'WEEKLY_SOURCE_NHSP_PAID_BASIS_BYTE_BOUND' using errcode='54000';end if;
  return v_result||jsonb_build_object('inputs_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',v_result),'hex'));
end;
$function$;
alter function private.weekly_source_nhsp_paid_overlap_basis_v1(uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_nhsp_paid_overlap_basis_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Proposed shared native extraction. This is the exact existing NHSP
-- magnitude assembly, factored once; it grants no financial or writer authority.
create or replace function private.bpay_next_source_nhsp_magnitude_v1(
  p_source_facts jsonb,p_pay_vector jsonb,p_charge_vector jsonb
) returns jsonb language plpgsql immutable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_magnitude jsonb;v_bucket text;v_hours jsonb:='{}';
begin
  foreach v_bucket in array array['day','night','sat','sun','bh'] loop
    v_hours:=v_hours||jsonb_build_object('hours_'||v_bucket,round(abs((p_pay_vector#>>array['hours',v_bucket])::numeric),2)::text);
  end loop;
  v_magnitude:=jsonb_build_object('work_date',p_source_facts->>'work_date',
    'start_at_local',p_source_facts->>'start_at_local','end_at_local',p_source_facts->>'end_at_local',
    'break_minutes',p_source_facts->>'break_minutes',
    'pay_pence',abs((p_pay_vector->>'total_pence')::bigint)::text,
    'charge_pence',abs((p_charge_vector->>'total_pence')::bigint)::text)||v_hours;
  return v_magnitude;
end $f$;
alter function private.bpay_next_source_nhsp_magnitude_v1(jsonb,jsonb,jsonb) OWNER TO CURRENT_USER;
revoke all on function private.bpay_next_source_nhsp_magnitude_v1(jsonb,jsonb,jsonb) from public,anon,authenticated,service_role;

-- Source purpose INPUT only. The whole sealed physical population and every
-- negative's real native C1 resolution are required before absence of a full
-- reversal is certified. This does not admit Same/Separate or financial writes.
create or replace function private.weekly_source_nhsp_negative_population_v1(
  p_upload_id uuid,p_work_event_id uuid,p_actor_user_id uuid,p_source_cycle_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  u public.weekly_source_uploads%rowtype;c public.weekly_source_cycles%rowtype;
  scope public.weekly_source_report_scopes%rowtype;p public.weekly_source_projection_publications%rowtype;
  w public.weekly_work_events%rowtype;row public.weekly_source_upload_rows%rowtype;
  resolution public.weekly_source_row_resolutions%rowtype;e public.weekly_source_row_economic_snapshots%rowtype;
  m public.weekly_source_billing_movements%rowtype;origin private.bpay_next_source_current_origins%rowtype;
  basis jsonb;position jsonb;target jsonb;targets jsonb:='[]'::jsonb;negatives jsonb:='[]'::jsonb;
  magnitude jsonb;v_pay jsonb;v_charge jsonb;result jsonb;native jsonb;
  generation integer;counted integer;total integer;negative_count integer:=0;
  complete boolean:=true;matching boolean:=false;current_available boolean:=true;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_upload_id is null or p_work_event_id is null or p_actor_user_id is null or p_source_cycle_id is null then
    raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_REQUEST_INVALID' using errcode='22023';end if;
  select * into strict u from public.weekly_source_uploads where id=p_upload_id;
  select * into strict c from public.weekly_source_cycles where id=p_source_cycle_id;
  select * into strict w from public.weekly_work_events where id=p_work_event_id;
  select * into strict scope from public.weekly_source_report_scopes where id=u.report_scope_id;
  if u.source_cycle_id is distinct from c.id or u.purpose<>'ORDINARY' or u.correction_session_id is not null
    or u.state<>'CURRENT' or u.row_manifest_hash is null or u.malformed_count<>0 or u.blocking_economic_duplicate_count<>0
    or scope.source_cycle_id is distinct from c.id or scope.source_group_id is distinct from c.source_group_id
    or scope.client_id is distinct from w.client_id or scope.self_bill_report_id is not null
    or scope.current_complete_upload_id is distinct from u.id
    or w.first_source_group_id is distinct from c.source_group_id
    or not exists(select 1 from public.weekly_source_groups g where g.id=c.source_group_id and g.active and g.source_family='NHSP')
    or not exists(select 1 from public.weekly_source_format_profiles profile where profile.id=u.source_format_profile_id
      and profile.active and profile.profile_code='NHSP_FINAL_BACKING_V1')
    or private.weekly_source_upload_manifest_hash_v1(u.id) is distinct from u.row_manifest_hash then
    raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_SCOPE_UNBOUND' using errcode='55000';end if;
  -- Actual viewer and both cycle/root dated permissions are proved inside the
  -- one native basis owner before any cash/current selection is returned.
  basis:=private.weekly_source_nhsp_paid_overlap_basis_v1(w.id,p_actor_user_id,c.id);
  if basis->>'schema_version' is distinct from 'WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1'
    or basis->>'work_event_id' is distinct from w.id::text or basis->>'source_cycle_id' is distinct from c.id::text
    or basis->>'inputs_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_NHSP_PAID_OVERLAP_BASIS_INPUT_V1',basis-'inputs_sha256'),'hex') then
    raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_BASIS_UNBOUND' using errcode='55000';end if;
  if basis->>'state' is distinct from 'NATIVE_POSITION_INPUTS' then current_available:=false;
  else
    for position in select value from jsonb_array_elements(basis->'native_current_rows') loop
      native:=position->'native_current_event';
      if position->>'state' is distinct from 'CURRENT_NATIVE_POSITION' or native->'source_present' is distinct from 'true'::jsonb then
        current_available:=false;continue;end if;
      select * into strict m from public.weekly_source_billing_movements where id=(native->>'movement_id')::uuid;
      select * into strict origin from private.bpay_next_source_current_origins o
        where o.origin_kind='MOVEMENT' and o.origin_id=m.id and o.active and o.contribution=1;
      magnitude:=private.bpay_next_source_nhsp_magnitude_v1(m.source_facts_json,m.canonical_pay_vector_json,m.canonical_charge_vector_json);
      if not exists(select 1 from private.bpay_next_source_current_magnitudes mag
        where mag.scope_id=origin.scope_id and mag.work_event_id=w.id and mag.magnitude_hash=origin.magnitude_hash
          and mag.selected_movement_id=m.id and mag.economic_magnitude=magnitude)
        or origin.magnitude_hash is distinct from private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_NHSP_MAGNITUDE_V1',magnitude) then
        raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_CURRENT_MAGNITUDE_UNBOUND' using errcode='55000';end if;
      targets:=targets||jsonb_build_array(jsonb_build_object('root_timesheet_id',position->'root_timesheet_id',
        'scope_id',origin.scope_id,'movement_id',m.id,'movement_economic_hash',encode(m.movement_economic_hash,'hex'),
        'work_event_id',m.work_event_id,'candidate_id',m.candidate_id,'client_id',m.actual_client_id,'contract_id',m.contract_id,
        'economic_magnitude',magnitude,'magnitude_sha256',encode(origin.magnitude_hash,'hex')));
    end loop;
    if jsonb_array_length(targets)=0 then current_available:=false;end if;
  end if;
  select count(*),min(x.id::text)::uuid into counted,p.id from public.weekly_source_projection_publications x
    where x.upload_id=u.id and x.source_cycle_id=c.id and x.report_scope_id=scope.id
      and x.authority_scope_kind='NHSP_REPORT_SCOPE' and x.authority_scope_version=scope.version
      and x.correction_session_id is null and x.state in ('BUILDING','CURRENT');
  if counted<>1 then complete:=false;
  else
    select * into strict p from public.weekly_source_projection_publications where id=p.id;
    if p.state='CURRENT' and scope.current_projection_publication_id is distinct from p.id then
      raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_PUBLICATION_UNBOUND' using errcode='55000';end if;
    if p.authority_scope_version>2147483647 then
      raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_GENERATION_UNBOUND' using errcode='55000';end if;
    generation:=coalesce(p.projection_generation,p.authority_scope_version::integer);
  end if;
  select count(*) into total from public.weekly_source_upload_rows where upload_id=u.id;
  if total<>u.accepted_count then
    raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_POPULATION_UNBOUND' using errcode='55000';end if;
  -- Scan every genuine physical row. A missing sign is not evidence that no
  -- full negative exists. Ref Num/name/ward never substitutes event identity.
  for row in select r.* from public.weekly_source_upload_rows r where r.upload_id=u.id order by r.source_row_ordinal loop
    if row.row_finalisation_state is distinct from 'SOURCE_WORKED' or row.source_money_parse_state is distinct from 'VALID'
      or row.source_qualification_profile_version is distinct from 'NHSP_TWO_COMPONENT_PENCE_V1'
      or row.source_shift_charge_pence is null then
      raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_PHYSICAL_UNBOUND' using errcode='55000';end if;
    if row.source_shift_charge_pence>=0 then continue;end if;
    negative_count:=negative_count+1;
    select count(*) into counted from public.weekly_source_row_resolutions r
      where r.upload_row_id=row.id and r.generation=generation;
    if counted<>1 then complete:=false;
      negatives:=negatives||jsonb_build_array(jsonb_build_object('upload_row_id',row.id,
        'normalised_row_hash',encode(row.normalised_row_hash,'hex'),'state','UNAVAILABLE','reason','NATIVE_NEGATIVE_RESOLUTION_UNAVAILABLE'));
      continue;end if;
    select * into strict resolution from public.weekly_source_row_resolutions r where r.upload_row_id=row.id and r.generation=generation;
    if resolution.mapping_state<>'RESOLVED' or resolution.work_event_id is null or resolution.candidate_id is null
      or resolution.client_id is null or resolution.contract_id is null or resolution.pay_effect<>'NORMAL'
      or resolution.paid_overlap_choice_id is not null or resolution.source_row_fingerprint is distinct from row.normalised_row_hash then
      complete:=false;
      negatives:=negatives||jsonb_build_array(jsonb_build_object('upload_row_id',row.id,
        'normalised_row_hash',encode(row.normalised_row_hash,'hex'),'state','UNAVAILABLE','reason','NATIVE_NEGATIVE_RESOLUTION_UNAVAILABLE'));
      continue;end if;
    select * into strict e from public.weekly_source_row_economic_snapshots where id=
      private.weekly_source_finalisation_economic_assert_v1(resolution.id,c.id,'NHSP_WEEKLY');
    v_pay:=private.weekly_source_finalisation_vector_v1(e.id,'PAY',1::smallint);
    v_charge:=private.weekly_source_finalisation_vector_v1(e.id,'CHARGE',1::smallint);
    magnitude:=private.bpay_next_source_nhsp_magnitude_v1(jsonb_build_object('work_date',row.work_date,
      'start_at_local',row.start_at_local,'end_at_local',row.end_at_local,'break_minutes',row.break_minutes),v_pay,v_charge);
    for target in select value from jsonb_array_elements(targets) loop
      if resolution.work_event_id::text=target->>'work_event_id' and resolution.candidate_id::text=target->>'candidate_id'
        and resolution.client_id::text=target->>'client_id' and resolution.contract_id::text=target->>'contract_id'
        and magnitude=target->'economic_magnitude' then matching:=true;end if;
    end loop;
    negatives:=negatives||jsonb_build_array(jsonb_build_object('upload_row_id',row.id,'source_row_ordinal',row.source_row_ordinal,
      'normalised_row_hash',encode(row.normalised_row_hash,'hex'),'state','NATIVE_RESOLVED_NEGATIVE',
      'row_resolution_id',resolution.id,'generation',resolution.generation,'work_event_id',resolution.work_event_id,
      'candidate_id',resolution.candidate_id,'client_id',resolution.client_id,'contract_id',resolution.contract_id,
      'economic_snapshot_id',e.id,'calculation_fingerprint',encode(e.calculation_fingerprint,'hex'),
      'economic_magnitude',magnitude,'magnitude_sha256',encode(private.weekly_source_sha256_jsonb_v1(
        'BPAY_NEXT_SOURCE_NHSP_MAGNITUDE_V1',magnitude),'hex')));
  end loop;
  result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_NHSP_NEGATIVE_POPULATION_INPUT_V1',
    'upload_id',u.id,'content_sha256',encode(u.content_sha256,'hex'),'row_manifest_hash',encode(u.row_manifest_hash,'hex'),
    'source_cycle_id',c.id,'source_group_id',c.source_group_id,'report_scope_id',scope.id,'authority_scope_version',scope.version,
    'projection_publication_id',p.id,'projection_generation',generation,'work_event_id',w.id,'native_current_basis',basis,
    'current_targets',targets,'physical_row_count',total,'negative_row_count',negative_count,'negative_rows',negatives,
    'negative_population_complete',complete,'current_position_available',current_available,
    'no_reversal_proved',case when complete and current_available then to_jsonb(not matching) else 'null'::jsonb end,
    'paid_writer_permission',null);
  if octet_length(result::text)>1048576 then raise exception 'WEEKLY_SOURCE_NHSP_NEGATIVE_BYTE_BOUND' using errcode='54000';end if;
  return result||jsonb_build_object('inputs_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_NHSP_NEGATIVE_POPULATION_INPUT_V1',result),'hex'));
end;
$function$;
alter function private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;

-- Single exact native line VAT assembly, including its original numeric
-- typmods and signed pence. No invoice/pay authority is granted here.
create or replace function private.weekly_source_native_line_vat_v1(
  p_invoice_presentation_charge_pence bigint,p_vat_chargeable boolean,p_vat_rate_pct numeric
) returns jsonb language plpgsql immutable security definer
set search_path to 'public','private','pg_catalog','pg_temp' as $function$
declare v_vat_rate numeric(5,2);v_vat numeric(12,2);v_total numeric(12,2);
begin
  v_vat_rate:=case when p_vat_chargeable
    then p_vat_rate_pct else 0 end;
  v_vat:=pg_catalog.round((p_invoice_presentation_charge_pence::numeric/100)*(v_vat_rate/100),2);
  v_total:=(p_invoice_presentation_charge_pence::numeric/100)+v_vat;
  return pg_catalog.jsonb_build_object('vat_rate_pct',v_vat_rate,'vat_amount',v_vat,'total_inc_vat',v_total);
end;
$function$;
alter function private.weekly_source_native_line_vat_v1(bigint,boolean,numeric) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_native_line_vat_v1(bigint,boolean,numeric) from public,anon,authenticated,service_role;

-- Exact existing FIN4 NHSP price admission, factored once for the physical
-- writer and the forthcoming invoice-only consumer. C1 remains native.
create or replace function private.weekly_source_nhsp_price_admission_v1(
  p_row_resolution_id uuid,p_source_cycle_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp' as $function$
declare v_row record;v_upload public.weekly_source_uploads%rowtype;
  v_economic public.weekly_source_row_economic_snapshots%rowtype;v_generation integer;
  v_charge_check public.weekly_source_charge_checks%rowtype;
  v_charge_acceptance public.weekly_source_charge_acceptances%rowtype;v_price_admission_result text;
begin
  select source_row.*,resolution.id as resolution_id,resolution.work_event_id,
    resolution.candidate_id,resolution.client_id,resolution.contract_id,resolution.generation
    into strict v_row from public.weekly_source_upload_rows source_row
    join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
    where resolution.id=p_row_resolution_id;
  select * into strict v_upload from public.weekly_source_uploads where id=v_row.upload_id;
  if v_upload.source_cycle_id is distinct from p_source_cycle_id then
    raise exception 'WEEKLY_SOURCE_NHSP_PRICE_SCOPE_UNBOUND' using errcode='55000';end if;
  select * into strict v_economic from public.weekly_source_row_economic_snapshots where id=
    private.weekly_source_finalisation_economic_assert_v1(v_row.resolution_id,p_source_cycle_id,'NHSP_WEEKLY');
  v_generation:=v_row.generation;
      select * into v_charge_check
      from public.weekly_source_charge_checks charge_check
      where charge_check.upload_row_id=v_row.id
        and charge_check.row_resolution_id=v_row.resolution_id
        and charge_check.generation=v_generation;
      v_charge_acceptance.id:=null;
      if found and v_charge_check.comparison_result in ('MISMATCH','ZERO_SOURCE_CHARGE') then
        select * into v_charge_acceptance
        from public.weekly_source_charge_acceptances acceptance
        where acceptance.charge_check_id=v_charge_check.id
          and acceptance.upload_row_id=v_row.id
          and acceptance.row_resolution_id=v_row.resolution_id
          and acceptance.source_upload_id=v_upload.id
          and acceptance.contract_id=v_row.contract_id
          and acceptance.acceptance_kind=case v_charge_check.comparison_result
            when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO' else 'ACCEPTED_DISPARITY' end
          and acceptance.source_upload_hash=v_upload.content_sha256
          and acceptance.source_row_fingerprint=(select resolution.source_row_fingerprint
            from public.weekly_source_row_resolutions resolution where resolution.id=v_row.resolution_id)
          and acceptance.contract_and_rate_fingerprint=v_economic.contract_and_rate_fingerprint
          and acceptance.effective_policy_fingerprint=v_economic.effective_policy_fingerprint
          and acceptance.charge_calculation_fingerprint=v_economic.calculation_fingerprint
          and acceptance.acceptance_policy_fingerprint=
            private.weekly_source_charge_acceptance_policy_fingerprint_v1();
      end if;
      v_price_admission_result:=case v_charge_check.comparison_result
        when 'ZERO_SOURCE_CHARGE' then 'ACCEPTED_ZERO'
        when 'MISMATCH' then 'ACCEPTED_DISPARITY'
        else v_charge_check.comparison_result end;
      if not found or (select pg_catalog.count(*)
          from public.weekly_source_charge_checks charge_check
          where charge_check.upload_row_id=v_row.id
            and charge_check.row_resolution_id=v_row.resolution_id
            and charge_check.generation=v_generation)<>1
         or v_charge_check.source_commission_pence is distinct from v_row.source_commission_pence
         or v_charge_check.source_total_cost_pence is distinct from v_row.source_total_cost_pence
         or v_charge_check.source_shift_charge_pence is distinct from v_row.source_shift_charge_pence
         or v_charge_check.calculated_segment_charge_pence is distinct from v_economic.calculated_charge_pence
         or v_charge_check.source_charge_difference_pence is distinct from
              (v_row.source_shift_charge_pence-v_economic.calculated_charge_pence)
         or v_charge_check.charge_calculation_fingerprint is distinct from
              v_economic.calculation_fingerprint
         or v_charge_check.comparison_profile_version<>'NHSP_TWO_COMPONENT_PENCE_V1'
         or v_charge_check.comparison_result not in (
              'EXACT','SOURCE_ROUNDING_EQUIVALENT','MISMATCH','ZERO_SOURCE_CHARGE'
            )
         or not (
           (v_charge_check.comparison_result='EXACT'
             and v_charge_check.source_charge_difference_pence=0)
           or (v_charge_check.comparison_result='SOURCE_ROUNDING_EQUIVALENT'
             and v_charge_check.row_sign_kind in ('POSITIVE','FULL_NEGATIVE')
             and (v_charge_check.source_charge_difference_pence=1
               or v_charge_check.source_charge_difference_pence=-1)
             and ((v_charge_check.source_shift_charge_pence>0
                   and v_charge_check.calculated_segment_charge_pence>0)
               or (v_charge_check.source_shift_charge_pence<0
                   and v_charge_check.calculated_segment_charge_pence<0)))
           or (v_charge_check.comparison_result='MISMATCH'
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_DISPARITY')
           or (v_charge_check.comparison_result='ZERO_SOURCE_CHARGE'
             and v_charge_check.source_commission_pence=0
             and v_charge_check.source_total_cost_pence=0
             and v_charge_check.source_shift_charge_pence=0
             and v_charge_check.calculated_segment_charge_pence>0
             and v_charge_acceptance.id is not null
             and v_charge_acceptance.acceptance_kind='ACCEPTED_ZERO')
         )
         or v_charge_check.row_sign_kind is distinct from (case
              when v_economic.row_sign=1 then 'POSITIVE' else 'FULL_NEGATIVE' end)
         or v_charge_check.phase_severity='FINALISATION_BLOCKER'
         or v_charge_check.blocker_code is not null then
        raise exception 'WEEKLY_SOURCE_NHSP_PRICE_GATE_FAILED'
          using errcode='55000',detail=v_row.source_row_ordinal::text;
      end if;
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_NHSP_PRICE_ADMISSION_V1',
    'upload_row_id',v_row.id,'row_resolution_id',v_row.resolution_id,'economic_snapshot_id',v_economic.id,
    'charge_check',to_jsonb(v_charge_check),'charge_acceptance',to_jsonb(v_charge_acceptance),
    'price_admission_result',v_price_admission_result);
end;
$function$;
alter function private.weekly_source_nhsp_price_admission_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_nhsp_price_admission_v1(uuid,uuid) from public,anon,authenticated,service_role;

-- Actual NHSP native invoice values only. This can price neither an ordinary
-- Agency/HealthRoster row nor a negative as a paid-match invoice companion.
-- Paid choice/no-reversal/final origin admission remains with its real writer.
create or replace function private.weekly_source_nhsp_invoice_values_v1(
  p_row_resolution_id uuid,p_source_cycle_id uuid
) returns jsonb language plpgsql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare r public.weekly_source_row_resolutions%rowtype;s public.weekly_source_upload_rows%rowtype;
  u public.weekly_source_uploads%rowtype;e public.weekly_source_row_economic_snapshots%rowtype;
  admission jsonb;vat jsonb;result jsonb;
begin
  select * into strict r from public.weekly_source_row_resolutions where id=p_row_resolution_id;
  select * into strict s from public.weekly_source_upload_rows where id=r.upload_row_id;
  select * into strict u from public.weekly_source_uploads where id=s.upload_id;
  if u.source_cycle_id is distinct from p_source_cycle_id or u.report_scope_id is null
    or not exists(select 1 from public.weekly_source_report_scopes scope
      where scope.id=u.report_scope_id and scope.source_cycle_id=u.source_cycle_id
        and scope.client_id=r.client_id and scope.self_bill_report_id is null)
    or not exists(select 1 from public.weekly_source_format_profiles profile
      where profile.id=u.source_format_profile_id and profile.profile_code='NHSP_FINAL_BACKING_V1')
    or s.row_finalisation_state is distinct from 'SOURCE_WORKED' or s.source_money_parse_state is distinct from 'VALID'
    or s.source_qualification_profile_version is distinct from 'NHSP_TWO_COMPONENT_PENCE_V1'
    or s.source_shift_charge_pence is null or s.source_shift_charge_pence<0 then
    raise exception 'WEEKLY_SOURCE_NHSP_INVOICE_VALUES_SCOPE_UNBOUND' using errcode='55000';end if;
  admission:=private.weekly_source_nhsp_price_admission_v1(r.id,p_source_cycle_id);
  select * into strict e from public.weekly_source_row_economic_snapshots where id=(admission->>'economic_snapshot_id')::uuid;
  if e.row_sign<>1 then raise exception 'WEEKLY_SOURCE_NHSP_INVOICE_VALUES_SIGN_UNBOUND' using errcode='55000';end if;
  vat:=private.weekly_source_native_line_vat_v1(s.source_shift_charge_pence,e.invoice_vat_chargeable,e.invoice_vat_rate_pct);
  result:=jsonb_build_object('schema_version','WEEKLY_SOURCE_NHSP_NATIVE_INVOICE_VALUES_V1',
    'row_resolution_id',r.id,'upload_row_id',s.id,'economic_snapshot_id',e.id,'source_cycle_id',p_source_cycle_id,
    'source_commission_pence',s.source_commission_pence,'source_total_cost_pence',s.source_total_cost_pence,
    'source_shift_charge_pence',s.source_shift_charge_pence,'calculated_comparison_charge_pence',e.calculated_charge_pence,
    'native_price_admission',admission,'native_line_vat',vat,'paid_writer_permission',null);
  return result||jsonb_build_object('inputs_sha256',encode(private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_NHSP_NATIVE_INVOICE_VALUES_V1',result),'hex'));
end;
$function$;
alter function private.weekly_source_nhsp_invoice_values_v1(uuid,uuid) OWNER TO CURRENT_USER;
revoke all on function private.weekly_source_nhsp_invoice_values_v1(uuid,uuid) from public,anon,authenticated,service_role;

do $vat$ declare x jsonb;begin
 x:=private.weekly_source_native_line_vat_v1(101,true,20);if (x->>'vat_amount')::numeric<>0.20 or (x->>'total_inc_vat')::numeric<>1.21 then raise exception 'HR_NHSP_NATIVE_VAT_POSITIVE_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(-101,true,20);if (x->>'vat_amount')::numeric<>-0.20 or (x->>'total_inc_vat')::numeric<>-1.21 then raise exception 'HR_NHSP_NATIVE_VAT_NEGATIVE_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(1,true,20);if (x->>'vat_amount')::numeric<>0 or (x->>'total_inc_vat')::numeric<>0.01 then raise exception 'HR_NHSP_NATIVE_VAT_PENNY_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(5,true,20);if (x->>'vat_amount')::numeric<>0.01 or (x->>'total_inc_vat')::numeric<>0.06 then raise exception 'HR_NHSP_NATIVE_VAT_ROUNDING_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(-5,true,20);if (x->>'vat_amount')::numeric<>-0.01 or (x->>'total_inc_vat')::numeric<>-0.06 then raise exception 'HR_NHSP_NATIVE_VAT_NEGATIVE_ROUNDING_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(100,false,20);if (x->>'vat_rate_pct')::numeric<>0 or (x->>'vat_amount')::numeric<>0 or (x->>'total_inc_vat')::numeric<>1 then raise exception 'HR_NHSP_NATIVE_VAT_EXEMPT_FAILED';end if;
 x:=private.weekly_source_native_line_vat_v1(0,true,20);if (x->>'vat_amount')::numeric<>0 or (x->>'total_inc_vat')::numeric<>0 then raise exception 'HR_NHSP_NATIVE_VAT_ZERO_FAILED';end if;
 if exists(select 1 from unnest(array['private.weekly_source_nhsp_paid_overlap_basis_v1(uuid,uuid,uuid)','private.bpay_next_source_nhsp_magnitude_v1(jsonb,jsonb,jsonb)','private.weekly_source_nhsp_negative_population_v1(uuid,uuid,uuid,uuid)','private.weekly_source_native_line_vat_v1(bigint,boolean,numeric)','private.weekly_source_nhsp_price_admission_v1(uuid,uuid)','private.weekly_source_nhsp_invoice_values_v1(uuid,uuid)']) id cross join unnest(array['anon','authenticated','service_role']) r where has_function_privilege(r,id,'EXECUTE')) then raise exception 'HR_NHSP_PRIVATE_HELPER_ACL_FAILED';end if;
end $vat$;
select current_database() as physical_database,true as six_new_private_declarations_compiled,true as exact_native_vat_vectors_passed;
