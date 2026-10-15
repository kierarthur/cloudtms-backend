-- Current-final Source observation owners, connected to genuine CURRENT
-- writers by 0505. No protected-context replacement/historical bootstrap.
-- Retained active_movements through-PREPARED predecessor/UUID-cutoff semantics
-- are NOT replaced by these current-final-only functions.
-- Hook order: actual Source current-pointer CAS -> produced origin IDs;
-- under the writer's already-held sorted booking locks -> exact scope row ->
-- captured origin -> its revision counter -> NHSP magnitude -> 129 PK nodes.
-- Initial finalisation's prior-only CANCEL/expense-omission roots must join its
-- affected-root inventory BEFORE booking locks, not by a new tail lock.
-- CorrectFinal: exact old/new revisions after CAS, point retract old captured
-- origins using the real COMMITTING/APPLIED session, then activate PREPARED
-- new origins. No reader sees the intermediate cache state in that transaction.
-- Protected financial decision INSERT RETURNING event ID is a separate hook;
-- ordinary audit events must never call decision capture.
begin;

create or replace function private.bpay_next_source_order_key_v1(
  p_week date,p_finalised_at timestamptz,p_revision integer
) returns bit(128) language plpgsql immutable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_week bytea;v_at bytea;v_revision bytea;
begin
  if p_week is null or p_finalised_at is null or p_revision is null then
    raise exception 'BPAY_NEXT_SOURCE_ORDER_NULL' using errcode='22023';
  end if;
  v_week:=date_send(p_week);v_at:=timestamptz_send(p_finalised_at);v_revision:=int4send(p_revision);
  -- Each signed field has its OWN sign bit. PostgreSQL send preserves its
  -- native infinity sentinels and exact microseconds; no epoch/text round trip.
  return ('x'||encode(set_byte(v_week,0,get_byte(v_week,0)#128)
    ||set_byte(v_at,0,get_byte(v_at,0)#128)
    ||set_byte(v_revision,0,get_byte(v_revision,0)#128),'hex'))::bit(128);
end $f$;

create or replace function private.bpay_next_source_prefix_v1(p_key bit(128),p_depth integer)
returns bit(128) language plpgsql immutable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
begin
  if p_key is null or p_depth is null or p_depth not between 0 and 128 then
    raise exception 'BPAY_NEXT_SOURCE_PREFIX_INVALID' using errcode='22023';
  end if;
  return (substring(p_key from 1 for p_depth)::text||repeat('0',128-p_depth))::bit(128);
end $f$;

create or replace function private.bpay_next_source_current_revision_v1(p_revision uuid)
returns table(order_key bit(128),is_current boolean,source_group_id uuid)
language plpgsql stable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_revision public.weekly_source_final_revisions%rowtype;v_cycle public.weekly_source_cycles%rowtype;
begin
  select r.* into v_revision from public.weekly_source_final_revisions r where r.id=p_revision;
  if not found then raise exception 'BPAY_NEXT_SOURCE_ORIGIN_REVISION_MISSING' using errcode='55000';end if;
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_revision.source_cycle_id;
  if v_revision.state='CURRENT' then
    if (v_revision.authority_scope_kind='CYCLE' and v_cycle.current_final_revision_id is distinct from p_revision)
      or (v_revision.authority_scope_kind='NHSP_REPORT_SCOPE' and not exists(
        select 1 from public.weekly_source_report_scopes s where s.id=v_revision.report_scope_id
          and s.source_cycle_id=v_cycle.id and s.current_final_revision_id=p_revision)) then
      raise exception 'BPAY_NEXT_SOURCE_CURRENT_POINTER_MISMATCH' using errcode='55000';
    end if;
  elsif v_revision.state='PREPARED' then
    if not exists(select 1 from public.weekly_final_source_correction_sessions s
      where s.prepared_final_revision_id=p_revision and s.source_cycle_id=v_revision.source_cycle_id
        and s.authority_scope_kind=v_revision.authority_scope_kind
        and s.report_scope_id is not distinct from v_revision.report_scope_id
        and coalesce(s.report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid)
          =coalesce(v_revision.report_scope_id,'00000000-0000-0000-0000-000000000000'::uuid)
        and s.state in ('PREPARED','COMMITTING')) then
      raise exception 'BPAY_NEXT_SOURCE_PREPARED_ORIGIN_UNBOUND' using errcode='55000';
    end if;
  else raise exception 'BPAY_NEXT_SOURCE_ORIGIN_NOT_CURRENT' using errcode='55000';end if;
  return query select private.bpay_next_source_order_key_v1(v_cycle.finalisation_week_ending,
    v_revision.finalised_at_utc,v_revision.revision_number),v_revision.state='CURRENT',v_cycle.source_group_id;
end $f$;

-- Actual lineage supplies membership. The only Timesheet query is indexed
-- canonical CURRENT LIMIT 2, never physical-family enumeration/history.
create or replace function private.bpay_next_source_current_scope_v1(
  p_resolution uuid,p_group uuid,p_event uuid,p_contract uuid
) returns uuid language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_lineage public.weekly_source_row_timesheet_lineages%rowtype;v_event public.weekly_work_events%rowtype;
  v_root public.timesheets%rowtype;v_contract public.contracts%rowtype;v_scope uuid;v_ids uuid[];
begin
  select l.* into v_lineage from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=p_resolution;
  select e.* into v_event from public.weekly_work_events e where e.id=p_event;
  select c.* into v_contract from public.contracts c where c.id=p_contract;
  if v_lineage.id is null or v_event.id is null or v_contract.id is null
    or v_lineage.work_event_id is distinct from p_event or v_lineage.contract_id is distinct from p_contract
    or v_lineage.candidate_id is distinct from v_event.candidate_id or v_lineage.client_id is distinct from v_event.client_id
    or v_contract.candidate_id is distinct from v_event.candidate_id or v_contract.client_id is distinct from v_event.client_id
    or v_event.first_source_group_id is distinct from p_group then
    raise exception 'BPAY_NEXT_SOURCE_LINEAGE_UNBOUND' using errcode='55000';
  end if;
  select array_agg(x.timesheet_id) into v_ids from (
    select t.timesheet_id from public.timesheets t
    where t.is_current and btrim(t.booking_id)=btrim(v_lineage.family_booking_id)
      and t.booking_id is not null and btrim(t.booking_id)<>'' and char_length(btrim(t.booking_id)) between 1 and 200
    limit 2) x;
  if coalesce(array_length(v_ids,1),0)<>1 then
    raise exception 'BPAY_NEXT_SOURCE_CURRENT_ROOT_AMBIGUOUS' using errcode='55000';end if;
  select t.* into strict v_root from public.timesheets t where t.timesheet_id=v_ids[1];
  if v_root.booking_id is distinct from v_lineage.family_booking_id
    or v_root.contract_id is distinct from p_contract or v_root.week_ending_date is distinct from v_lineage.week_ending_date
    or not exists(select 1 from public.contract_weeks cw where cw.timesheet_id=v_root.timesheet_id
      and cw.contract_id=p_contract and cw.week_ending_date=v_lineage.week_ending_date and coalesce(cw.additional_seq,0)=0) then
    raise exception 'BPAY_NEXT_SOURCE_CURRENT_ROOT_UNBOUND' using errcode='55000';end if;
  insert into private.bpay_next_source_current_scopes(root_timesheet_id,family_booking_id,source_group_id,
    candidate_id,client_id,contract_id,week_ending_date)
    values(v_root.timesheet_id,v_lineage.family_booking_id,p_group,v_event.candidate_id,v_event.client_id,p_contract,v_lineage.week_ending_date)
    on conflict(source_group_id,family_booking_id,candidate_id,client_id,contract_id,week_ending_date)
    do update set root_timesheet_id=excluded.root_timesheet_id;
  select s.id into strict v_scope from private.bpay_next_source_current_scopes s
    where s.source_group_id=p_group and s.family_booking_id=v_lineage.family_booking_id
      and s.candidate_id=v_event.candidate_id and s.client_id=v_event.client_id and s.contract_id=p_contract
      and s.week_ending_date=v_lineage.week_ending_date for update;
  return v_scope;
end $f$;

create or replace function private.bpay_next_source_nhsp_delta_v1(
  p_scope uuid,p_event uuid,p_hash bytea,p_key bit(128),p_positive integer,p_negative integer
) returns void language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_depth integer;v_prefix bit(128);v_left private.bpay_next_source_current_nodes%rowtype;
  v_right private.bpay_next_source_current_nodes%rowtype;
begin
  if p_positive is null or p_negative is null or p_key is null
    or p_positive not in (-1,0,1) or p_negative not in (-1,0,1)
    or abs(p_positive)+abs(p_negative)<>1 then raise exception 'BPAY_NEXT_SOURCE_DELTA_INVALID' using errcode='22023';end if;
  -- Magnitude row is locked by the caller; all node probes are full PKs.
  for v_depth in 0..128 loop
    v_prefix:=private.bpay_next_source_prefix_v1(p_key,v_depth);
    insert into private.bpay_next_source_current_nodes(scope_id,work_event_id,magnitude_hash,depth,prefix)
      values(p_scope,p_event,p_hash,v_depth,v_prefix) on conflict do nothing;
  end loop;
  update private.bpay_next_source_current_nodes n set
    positive_count=n.positive_count+p_positive,negative_count=n.negative_count+p_negative,
    net=n.positive_count+p_positive-n.negative_count-p_negative,
    max_positive_suffix=case when n.positive_count+p_positive>0
      then n.positive_count+p_positive-n.negative_count-p_negative else null end
    where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash and n.depth=128 and n.prefix=p_key;
  for v_depth in reverse 127..0 loop
    v_prefix:=private.bpay_next_source_prefix_v1(p_key,v_depth);
    select n.* into v_left from private.bpay_next_source_current_nodes n
      where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash
        and n.depth=v_depth+1 and n.prefix=v_prefix;
    select n.* into v_right from private.bpay_next_source_current_nodes n
      where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash
        and n.depth=v_depth+1 and n.prefix=set_bit(v_prefix,v_depth,1);
    update private.bpay_next_source_current_nodes n set
      net=coalesce(v_left.net,0)+coalesce(v_right.net,0),
      max_positive_suffix=greatest(v_right.max_positive_suffix,
        case when v_left.max_positive_suffix is not null then coalesce(v_right.net,0)+v_left.max_positive_suffix else null end)
      where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash and n.depth=v_depth and n.prefix=v_prefix;
  end loop;
end $f$;

create or replace function private.bpay_next_source_nhsp_head_v1(p_scope uuid,p_event uuid,p_hash bytea)
returns bit(128) language plpgsql stable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_depth integer;v_prefix bit(128):=repeat('0',128)::bit(128);v_later bigint:=0;
  v_node private.bpay_next_source_current_nodes%rowtype;v_right private.bpay_next_source_current_nodes%rowtype;
begin
  select n.* into v_node from private.bpay_next_source_current_nodes n
    where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash and n.depth=0 and n.prefix=v_prefix;
  if not found or v_node.max_positive_suffix is null or v_node.max_positive_suffix<=0 then return null;end if;
  for v_depth in 0..127 loop
    select n.* into v_right from private.bpay_next_source_current_nodes n
      where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash
        and n.depth=v_depth+1 and n.prefix=set_bit(v_prefix,v_depth,1);
    if v_right.max_positive_suffix is not null and v_later+v_right.max_positive_suffix>0 then
      v_prefix:=set_bit(v_prefix,v_depth,1);
    else
      v_later:=v_later+coalesce(v_right.net,0);
      select n.* into v_node from private.bpay_next_source_current_nodes n
        where n.scope_id=p_scope and n.work_event_id=p_event and n.magnitude_hash=p_hash
          and n.depth=v_depth+1 and n.prefix=v_prefix;
      if not found or v_node.max_positive_suffix is null or v_later+v_node.max_positive_suffix<=0 then
        raise exception 'BPAY_NEXT_SOURCE_TREE_CORRUPT' using errcode='55000';end if;
    end if;
  end loop;
  return v_prefix;
end $f$;

create or replace function private.bpay_next_source_current_refresh_v1(p_scope uuid,p_event uuid)
returns void language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_heads record;v_first_key bit(128);v_movement uuid;v_transition uuid;v_source text;v_ambiguous boolean:=false;
  v_count integer:=0;v_present boolean:=false;
begin
  select e.source_kind into v_source from private.bpay_next_source_current_events e
    where e.scope_id=p_scope and e.work_event_id=p_event;
  if v_source='NHSP' then
    -- Global latest K FIRST; do not emit all independent magnitude survivors.
    for v_heads in select m.surviving_order_key,m.selected_movement_id
      from private.bpay_next_source_current_magnitudes m where m.scope_id=p_scope and m.work_event_id=p_event
        and m.surviving_order_key is not null order by m.surviving_order_key desc,m.magnitude_hash limit 2 loop
      v_count:=v_count+1;
      if v_count=1 then v_first_key:=v_heads.surviving_order_key;v_movement:=v_heads.selected_movement_id;
      elsif v_heads.surviving_order_key=v_first_key then v_ambiguous:=true;end if;
    end loop;
    v_present:=v_movement is not null and not v_ambiguous;
  elsif v_source='HR' then
    for v_heads in select o.order_key,o.selected_movement_id,o.transition_id
      from private.bpay_next_source_current_origins o where o.scope_id=p_scope and o.work_event_id=p_event
        and o.active and o.origin_kind='TRANSITION' order by o.order_key desc,o.origin_id::text desc limit 2 loop
      v_count:=v_count+1;
      if v_count=1 then v_first_key:=v_heads.order_key;v_movement:=v_heads.selected_movement_id;v_transition:=v_heads.transition_id;
      elsif v_heads.order_key=v_first_key then v_ambiguous:=true;end if;
    end loop;
    v_present:=v_movement is not null and not v_ambiguous;
  else raise exception 'BPAY_NEXT_SOURCE_EVENT_KIND_MISSING' using errcode='55000';end if;
  update private.bpay_next_source_current_events e set source_present=v_present,ambiguous=v_ambiguous,
    selected_movement_id=case when v_present then v_movement else null end,selected_transition_id=v_transition,selected_order_key=v_first_key
    where e.scope_id=p_scope and e.work_event_id=p_event;
end $f$;

-- Internal state toggle reads the immutable captured tuple, never fresh Source
-- lineage/magnitude classification. Public entry points alone choose activation.
create or replace function private.bpay_next_source_current_toggle_v1(p_kind text,p_id uuid,p_active boolean)
returns void language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_origin private.bpay_next_source_current_origins%rowtype;v_delta integer;v_key bit(128);v_movement uuid;
begin
  if p_active is null then raise exception 'BPAY_NEXT_SOURCE_ACTIVE_NULL' using errcode='22023';end if;
  select o.* into strict v_origin from private.bpay_next_source_current_origins o where o.origin_kind=p_kind and o.origin_id=p_id for update;
  if v_origin.active=p_active then return;end if;
  v_delta:=case when p_active then 1 else -1 end;
  insert into private.bpay_next_source_current_revisions(scope_id,final_revision_id,order_key,active_origin_count)
    values(v_origin.scope_id,v_origin.final_revision_id,v_origin.order_key,greatest(v_delta,0))
    on conflict(scope_id,final_revision_id) do update
      set active_origin_count=bpay_next_source_current_revisions.active_origin_count+v_delta;
  update private.bpay_next_source_current_origins o set active=p_active where o.origin_kind=p_kind and o.origin_id=p_id;
  if p_kind='MOVEMENT' then
    perform 1 from private.bpay_next_source_current_magnitudes m where m.scope_id=v_origin.scope_id
      and m.work_event_id=v_origin.work_event_id and m.magnitude_hash=v_origin.magnitude_hash for update;
    perform private.bpay_next_source_nhsp_delta_v1(v_origin.scope_id,v_origin.work_event_id,v_origin.magnitude_hash,
      v_origin.order_key,case when v_origin.contribution=1 then v_delta else 0 end,case when v_origin.contribution=-1 then v_delta else 0 end);
    v_key:=private.bpay_next_source_nhsp_head_v1(v_origin.scope_id,v_origin.work_event_id,v_origin.magnitude_hash);
    if v_key is not null then
      select o.movement_id into v_movement from private.bpay_next_source_current_origins o
        where o.scope_id=v_origin.scope_id and o.work_event_id=v_origin.work_event_id
          and o.magnitude_hash=v_origin.magnitude_hash and o.order_key=v_key and o.active
          and o.origin_kind='MOVEMENT' and o.contribution=1
        order by o.representative_created_at desc,o.origin_id::text desc limit 1;
      if v_movement is null then raise exception 'BPAY_NEXT_SOURCE_REPRESENTATIVE_MISSING' using errcode='55000';end if;
    end if;
    update private.bpay_next_source_current_magnitudes m set surviving_order_key=v_key,selected_movement_id=v_movement
      where m.scope_id=v_origin.scope_id and m.work_event_id=v_origin.work_event_id and m.magnitude_hash=v_origin.magnitude_hash;
    perform private.bpay_next_source_current_refresh_v1(v_origin.scope_id,v_origin.work_event_id);
  elsif p_kind='TRANSITION' then
    perform private.bpay_next_source_current_refresh_v1(v_origin.scope_id,v_origin.work_event_id);
  elsif p_kind='EXPENSE' and not p_active then
    delete from private.bpay_next_source_current_expenses e where e.scope_id=v_origin.scope_id
      and e.work_event_id=v_origin.work_event_id and e.authority_id=p_id;
  end if;
end $f$;

create or replace function private.bpay_next_source_current_capture_movement_v1(p_movement uuid)
returns jsonb language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_m public.weekly_source_billing_movements%rowtype;v_origin private.bpay_next_source_current_origins%rowtype;
  v_revision record;v_scope uuid;v_resolution uuid;v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_economic uuid;v_magnitude jsonb;v_hash bytea;v_bucket text;v_hours jsonb:='{}';v_inserted boolean;
begin
  select m.* into v_m from public.weekly_source_billing_movements m where m.id=p_movement;
  if not found or v_m.source_profile_kind<>'NHSP_TRUST_BACKING_REPORT' or v_m.prior_movement_id is not null
    or v_m.source_line_kind not in ('NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE')
    or v_m.movement_role not in ('POSITIVE','REVERSAL','REPLACEMENT') or v_m.expense_authority_generation_id is not null then
    raise exception 'BPAY_NEXT_SOURCE_MOVEMENT_NOT_ADMITTED' using errcode='55000';end if;
  select o.* into v_origin from private.bpay_next_source_current_origins o where o.origin_kind='MOVEMENT' and o.origin_id=p_movement;
  if found and v_origin.source_hash is distinct from v_m.movement_economic_hash then
    raise exception 'BPAY_NEXT_SOURCE_ORIGIN_CHANGED' using errcode='55000';end if;
  if found and (v_origin.active or v_origin.retracted_by_session_id is not null) then
    return jsonb_build_object('origin_id',p_movement,'active',v_origin.active,'idempotent',true);end if;
  select * into strict v_revision from private.bpay_next_source_current_revision_v1(v_m.final_revision_id);
  v_resolution:=(v_m.source_facts_json->>'row_resolution_id')::uuid;
  v_scope:=private.bpay_next_source_current_scope_v1(v_resolution,v_revision.source_group_id,v_m.work_event_id,v_m.contract_id);
  select l.* into strict v_lineage from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=v_resolution;
  if v_m.candidate_id is distinct from v_lineage.candidate_id or v_m.actual_client_id is distinct from v_lineage.client_id then
    raise exception 'BPAY_NEXT_SOURCE_MOVEMENT_LINEAGE_UNBOUND' using errcode='55000';end if;
  select e.id into strict v_economic from public.weekly_source_row_economic_snapshots e where e.row_resolution_id=v_resolution;
  foreach v_bucket in array array['day','night','sat','sun','bh'] loop
    v_hours:=v_hours||jsonb_build_object('hours_'||v_bucket,round(abs((v_m.canonical_pay_vector_json#>>array['hours',v_bucket])::numeric),2)::text);
  end loop;
  v_magnitude:=jsonb_build_object('work_date',v_m.source_facts_json->>'work_date',
    'start_at_local',v_m.source_facts_json->>'start_at_local','end_at_local',v_m.source_facts_json->>'end_at_local',
    'break_minutes',v_m.source_facts_json->>'break_minutes',
    'pay_pence',abs((v_m.canonical_pay_vector_json->>'total_pence')::bigint)::text,
    'charge_pence',abs((v_m.canonical_charge_vector_json->>'total_pence')::bigint)::text)||v_hours;
  v_hash:=private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_NHSP_MAGNITUDE_V1',v_magnitude);
  insert into private.bpay_next_source_current_magnitudes(scope_id,work_event_id,magnitude_hash,economic_magnitude)
    values(v_scope,v_m.work_event_id,v_hash,v_magnitude) on conflict do nothing;
  if not exists(select 1 from private.bpay_next_source_current_magnitudes m where m.scope_id=v_scope
    and m.work_event_id=v_m.work_event_id and m.magnitude_hash=v_hash and m.economic_magnitude=v_magnitude) then
    raise exception 'BPAY_NEXT_SOURCE_MAGNITUDE_HASH_COLLISION' using errcode='55000';end if;
  insert into private.bpay_next_source_current_events(scope_id,work_event_id,source_kind,source_present,source_group_id,client_id,work_date)
    select v_scope,v_m.work_event_id,'NHSP',false,v_revision.source_group_id,w.client_id,w.work_date
    from public.weekly_work_events w where w.id=v_m.work_event_id on conflict do nothing;
  if not exists(select 1 from private.bpay_next_source_current_events e where e.scope_id=v_scope
    and e.work_event_id=v_m.work_event_id and e.source_kind='NHSP') then
    raise exception 'BPAY_NEXT_SOURCE_EVENT_KIND_CHANGED' using errcode='55000';end if;
  insert into private.bpay_next_source_current_origins(origin_kind,origin_id,movement_id,scope_id,work_event_id,
    final_revision_id,order_key,source_hash,row_resolution_id,lineage_id,economic_snapshot_id,
    magnitude_hash,contribution,representative_created_at)
    values('MOVEMENT',v_m.id,v_m.id,v_scope,v_m.work_event_id,v_m.final_revision_id,v_revision.order_key,
      v_m.movement_economic_hash,v_resolution,v_lineage.id,v_economic,v_hash,
      case when v_m.movement_role='REVERSAL' then -1 else 1 end,v_m.created_at_utc) on conflict do nothing;
  v_inserted:=found;
  perform private.bpay_next_source_current_toggle_v1('MOVEMENT',p_movement,v_revision.is_current);
  return jsonb_build_object('origin_id',p_movement,'active',v_revision.is_current,'idempotent',not v_inserted and not v_revision.is_current);
end $f$;

create or replace function private.bpay_next_source_current_capture_transition_v1(p_transition uuid)
returns jsonb language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_t public.weekly_source_state_transitions%rowtype;v_line public.weekly_source_final_snapshot_lines%rowtype;
  v_origin private.bpay_next_source_current_origins%rowtype;v_revision record;v_scope uuid;v_movement uuid;
  v_lineage uuid;v_economic uuid;v_inserted boolean;v_reversal public.weekly_source_billing_movements%rowtype;
begin
  select t.* into v_t from public.weekly_source_state_transitions t where t.id=p_transition;
  if not found then raise exception 'BPAY_NEXT_SOURCE_TRANSITION_MISSING' using errcode='55000';end if;
  select o.* into v_origin from private.bpay_next_source_current_origins o where o.origin_kind='TRANSITION' and o.origin_id=p_transition;
  if found and v_origin.source_hash is distinct from v_t.transition_fingerprint then
    raise exception 'BPAY_NEXT_SOURCE_ORIGIN_CHANGED' using errcode='55000';end if;
  if found and (v_origin.active or v_origin.retracted_by_session_id is not null) then
    return jsonb_build_object('origin_id',p_transition,'active',v_origin.active,'idempotent',true);end if;
  select * into strict v_revision from private.bpay_next_source_current_revision_v1(v_t.final_revision_id);
  select l.* into strict v_line from public.weekly_source_final_snapshot_lines l
    where l.id=coalesce(v_t.new_snapshot_line_id,v_t.previous_snapshot_line_id);
  v_scope:=private.bpay_next_source_current_scope_v1(v_line.row_resolution_id,v_revision.source_group_id,v_t.work_event_id,v_line.contract_id);
  select l.id into strict v_lineage from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=v_line.row_resolution_id;
  select e.id into strict v_economic from public.weekly_source_row_economic_snapshots e where e.row_resolution_id=v_line.row_resolution_id;
  if v_t.outcome in ('ADD','AMEND') then
    select m.id into strict v_movement from public.weekly_source_billing_movements m
      where m.transition_id=p_transition and m.movement_role=case when v_t.outcome='ADD' then 'POSITIVE' else 'REPLACEMENT' end;
  elsif v_t.outcome='NO_CHANGE' then
    -- The real transition gives an exact previous snapshot, not an invitation
    -- to rank the ledger. Its producer-maintained mapping preserves the prior
    -- original movement even though the new report restates equivalent money.
    select s.movement_id into v_movement from private.bpay_next_source_current_snapshots s
      where s.snapshot_line_id=v_t.previous_snapshot_line_id and s.scope_id=v_scope and s.work_event_id=v_t.work_event_id;
    if v_movement is null then raise exception 'BPAY_NEXT_SOURCE_PREVIOUS_SNAPSHOT_UNCAPTURED' using errcode='55000';end if;
  end if;
  if v_t.outcome in ('AMEND','CANCEL') then
    select m.* into strict v_reversal from public.weekly_source_billing_movements m
      where m.transition_id=p_transition and m.movement_role='REVERSAL';
    if v_reversal.prior_movement_id is null or not exists(
      select 1 from private.bpay_next_source_current_snapshots s where s.snapshot_line_id=v_t.previous_snapshot_line_id
        and s.scope_id=v_scope and s.work_event_id=v_t.work_event_id and s.movement_id=v_reversal.prior_movement_id) then
      raise exception 'BPAY_NEXT_SOURCE_PRIOR_MOVEMENT_UNBOUND' using errcode='55000';end if;
  end if;
  if v_movement is not null and not exists(select 1 from public.weekly_source_billing_movements m
    where m.id=v_movement and m.work_event_id=v_t.work_event_id and m.contract_id=v_line.contract_id
      and m.candidate_id=v_line.candidate_id and m.actual_client_id=v_line.client_id
      and m.movement_role in ('POSITIVE','REPLACEMENT')) then
    raise exception 'BPAY_NEXT_SOURCE_TRANSITION_MOVEMENT_UNBOUND' using errcode='55000';end if;
  if v_t.new_snapshot_line_id is not null then
    insert into private.bpay_next_source_current_snapshots(snapshot_line_id,scope_id,work_event_id,movement_id)
      values(v_t.new_snapshot_line_id,v_scope,v_t.work_event_id,v_movement) on conflict do nothing;
    if not exists(select 1 from private.bpay_next_source_current_snapshots s where s.snapshot_line_id=v_t.new_snapshot_line_id
      and s.scope_id=v_scope and s.work_event_id=v_t.work_event_id and s.movement_id=v_movement) then
      raise exception 'BPAY_NEXT_SOURCE_SNAPSHOT_MAPPING_CHANGED' using errcode='55000';end if;
  end if;
  insert into private.bpay_next_source_current_events(scope_id,work_event_id,source_kind,source_present,source_group_id,client_id,work_date)
    select v_scope,v_t.work_event_id,'HR',false,v_revision.source_group_id,w.client_id,w.work_date
    from public.weekly_work_events w where w.id=v_t.work_event_id on conflict do nothing;
  if not exists(select 1 from private.bpay_next_source_current_events e where e.scope_id=v_scope
    and e.work_event_id=v_t.work_event_id and e.source_kind='HR') then
    raise exception 'BPAY_NEXT_SOURCE_EVENT_KIND_CHANGED' using errcode='55000';end if;
  insert into private.bpay_next_source_current_origins(origin_kind,origin_id,transition_id,scope_id,work_event_id,
    final_revision_id,order_key,source_hash,row_resolution_id,lineage_id,economic_snapshot_id,
    selected_movement_id,snapshot_line_id,representative_created_at)
    values('TRANSITION',p_transition,p_transition,v_scope,v_t.work_event_id,v_t.final_revision_id,v_revision.order_key,
      v_t.transition_fingerprint,v_line.row_resolution_id,v_lineage,v_economic,v_movement,v_t.new_snapshot_line_id,v_t.created_at_utc)
    on conflict do nothing;
  v_inserted:=found;
  perform private.bpay_next_source_current_toggle_v1('TRANSITION',p_transition,v_revision.is_current);
  return jsonb_build_object('origin_id',p_transition,'active',v_revision.is_current,'idempotent',not v_inserted and not v_revision.is_current);
end $f$;

-- A zero observation does not require (or manufacture) an own positive-row
-- lineage. It may bind only the actual event's exact existing base week/root.
-- A never-root zero is valid Source evidence but has no root cache to publish.
create or replace function private.bpay_next_source_zero_scope_v1(p_group uuid,p_event uuid,p_contract uuid)
returns uuid language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_event public.weekly_work_events%rowtype;v_contract public.contracts%rowtype;
  v_roots uuid[];v_root public.timesheets%rowtype;v_scope uuid;v_week date;
begin
  select e.* into strict v_event from public.weekly_work_events e where e.id=p_event;
  select c.* into strict v_contract from public.contracts c where c.id=p_contract;
  if v_event.first_source_group_id is distinct from p_group
    or v_contract.candidate_id is distinct from v_event.candidate_id or v_contract.client_id is distinct from v_event.client_id then
    raise exception 'BPAY_NEXT_SOURCE_ZERO_EXPENSE_UNBOUND' using errcode='55000';end if;
  v_week:=v_event.work_date+((coalesce(v_contract.week_ending_weekday_snapshot,0)-extract(dow from v_event.work_date)::integer+7)%7);
  select array_agg(x.timesheet_id) into v_roots from (
    select cw.timesheet_id from public.contract_weeks cw
    where cw.contract_id=p_contract and cw.week_ending_date=v_week and cw.additional_seq=0 limit 2
  ) x;
  if coalesce(cardinality(v_roots),0)=0 or (cardinality(v_roots)=1 and v_roots[1] is null) then return null;end if;
  if cardinality(v_roots)<>1 then raise exception 'BPAY_NEXT_SOURCE_ZERO_EXPENSE_ROOT_AMBIGUOUS' using errcode='55000';end if;
  select t.* into v_root from public.timesheets t where t.timesheet_id=v_roots[1];
  if not found or not v_root.is_current or v_root.contract_id is distinct from p_contract
    or v_root.week_ending_date is distinct from v_week or v_root.booking_id is null
    or char_length(v_root.booking_id) not between 1 and 200 then
    raise exception 'BPAY_NEXT_SOURCE_ZERO_EXPENSE_UNBOUND' using errcode='55000';end if;
  select array_agg(x.timesheet_id) into v_roots from (select t.timesheet_id from public.timesheets t
    where t.is_current and btrim(t.booking_id)=btrim(v_root.booking_id) and t.booking_id is not null
      and btrim(t.booking_id)<>'' and char_length(btrim(t.booking_id)) between 1 and 200 limit 2) x;
  if coalesce(cardinality(v_roots),0)<>1 or v_roots[1]<>v_root.timesheet_id then
    raise exception 'BPAY_NEXT_SOURCE_CURRENT_ROOT_AMBIGUOUS' using errcode='55000';end if;
  insert into private.bpay_next_source_current_scopes(root_timesheet_id,family_booking_id,source_group_id,candidate_id,client_id,contract_id,week_ending_date)
    values(v_root.timesheet_id,v_root.booking_id,p_group,v_event.candidate_id,v_event.client_id,p_contract,v_week)
    on conflict(source_group_id,family_booking_id,candidate_id,client_id,contract_id,week_ending_date)
    do update set root_timesheet_id=excluded.root_timesheet_id;
  select s.id into strict v_scope from private.bpay_next_source_current_scopes s where s.root_timesheet_id=v_root.timesheet_id
    and s.source_group_id=p_group and s.family_booking_id=v_root.booking_id
    and s.contract_id=p_contract and s.candidate_id=v_event.candidate_id and s.client_id=v_event.client_id
    and s.week_ending_date=v_week for update;
  return v_scope;
end $f$;

create or replace function private.bpay_next_source_current_capture_expense_v1(p_authority uuid)
returns jsonb language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_a public.weekly_expense_authority_generations%rowtype;v_origin private.bpay_next_source_current_origins%rowtype;
  v_revision record;v_scope uuid;v_resolution uuid;v_lineage uuid;v_inserted boolean;v_old uuid;
  v_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_cached private.bpay_next_source_current_expenses%rowtype;
begin
  select a.* into v_a from public.weekly_expense_authority_generations a where a.id=p_authority;
  if not found then raise exception 'BPAY_NEXT_SOURCE_EXPENSE_MISSING' using errcode='55000';end if;
  select o.* into v_origin from private.bpay_next_source_current_origins o where o.origin_kind='EXPENSE' and o.origin_id=p_authority;
  if found and v_origin.source_hash is distinct from v_a.authority_hash then
    raise exception 'BPAY_NEXT_SOURCE_ORIGIN_CHANGED' using errcode='55000';end if;
  if found and (v_origin.active or v_origin.retracted_by_session_id is not null) then
    return jsonb_build_object('origin_id',p_authority,'active',v_origin.active,'idempotent',true,'root_bound',true);end if;
  select * into strict v_revision from private.bpay_next_source_current_revision_v1(v_a.final_revision_id);
  if (v_revision.is_current and v_a.state<>'CURRENT') or (not v_revision.is_current and v_a.state<>'PREPARED') then
    raise exception 'BPAY_NEXT_SOURCE_EXPENSE_NOT_CURRENT' using errcode='55000';end if;
  if v_a.source_observation_kind='ROW_PRESENT' then
    select s.* into strict v_policy from public.weekly_source_row_expense_policy_snapshots s
      where s.id=v_a.row_expense_policy_snapshot_id;
    v_resolution:=v_policy.row_resolution_id;
    if v_policy.work_event_id is distinct from v_a.work_event_id or v_policy.contract_id is distinct from v_a.contract_id
      or v_policy.source_expense_pence is distinct from v_a.source_expense_pence
      or v_policy.source_expense_vat_enabled is distinct from v_a.source_expense_vat_enabled
      or not exists(select 1 from public.weekly_source_row_resolutions r
        join public.weekly_source_upload_rows u on u.id=r.upload_row_id
        join public.weekly_source_final_revisions f on f.id=v_a.final_revision_id and f.upload_id=u.upload_id
        where r.id=v_resolution and r.generation=v_policy.generation and r.mapping_state='RESOLVED'
          and u.id=v_policy.upload_row_id and r.work_event_id=v_a.work_event_id
          and r.contract_id=v_a.contract_id and r.candidate_id=v_policy.candidate_id and r.client_id=v_policy.client_id) then
      raise exception 'BPAY_NEXT_SOURCE_EXPENSE_POLICY_UNBOUND' using errcode='55000';end if;
    select l.id into v_lineage from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=v_resolution;
    if v_a.source_expense_pence>0 or v_lineage is not null then
      -- Genuine positive expenses retain their mandatory own immutable lineage.
      v_scope:=private.bpay_next_source_current_scope_v1(v_resolution,v_revision.source_group_id,v_a.work_event_id,v_a.contract_id);
      if v_lineage is null then raise exception 'BPAY_NEXT_SOURCE_LINEAGE_UNBOUND' using errcode='55000';end if;
    else
      v_scope:=private.bpay_next_source_zero_scope_v1(v_revision.source_group_id,v_a.work_event_id,v_a.contract_id);
    end if;
  else
    -- A real zero omission has NO own row snapshot. Bind the existing exact
    -- event/date/group + Contract + current base Contract-week, never recurse
    -- through old zero authority generations to invent a physical lineage.
    v_scope:=private.bpay_next_source_zero_scope_v1(v_revision.source_group_id,v_a.work_event_id,v_a.contract_id);
  end if;
  if v_scope is null then
    if v_a.source_expense_pence<>0 then raise exception 'BPAY_NEXT_SOURCE_LINEAGE_UNBOUND' using errcode='55000';end if;
    return jsonb_build_object('origin_id',p_authority,'active',false,'idempotent',false,'root_bound',false);
  end if;
  insert into private.bpay_next_source_current_origins(origin_kind,origin_id,expense_authority_id,scope_id,work_event_id,
    final_revision_id,order_key,source_hash,row_resolution_id,lineage_id,representative_created_at)
    values('EXPENSE',p_authority,p_authority,v_scope,v_a.work_event_id,v_a.final_revision_id,v_revision.order_key,
      v_a.authority_hash,v_resolution,v_lineage,v_a.created_at_utc) on conflict do nothing;
  v_inserted:=found;
  if v_revision.is_current then
    select e.* into v_cached from private.bpay_next_source_current_expenses e
      where e.scope_id=v_scope and e.work_event_id=v_a.work_event_id for update;
    v_old:=v_cached.authority_id;
    if v_old is not null and v_old<>p_authority then
      if v_old is distinct from v_a.prior_expense_authority_generation_id then
        raise exception 'BPAY_NEXT_SOURCE_EXPENSE_PREDECESSOR_UNBOUND' using errcode='55000';end if;
      if exists(select 1 from private.bpay_next_source_current_origins o where o.origin_kind='EXPENSE' and o.origin_id=v_old) then
        perform private.bpay_next_source_current_toggle_v1('EXPENSE',v_old,false);
      else
        -- A valid never-root 0/0 row has no origin/counter. A later exact
        -- accepted row can observe its retained zero authority after another
        -- event creates the shared real root. Prove ONLY that zero predecessor
        -- by its immutable point-bound facts; do not invent an origin or
        -- decrement a counter that was never incremented. Absence of the
        -- exact scope/revision counter row is mandatory; a zero count is NOT
        -- proof of no previous contribution. Missing positive or otherwise
        -- unbound origins remain a hard refusal; toggle stays STRICT.
        if v_cached.positive is distinct from false or not exists(
          select 1 from public.weekly_expense_authority_generations a
          join public.weekly_source_row_expense_policy_snapshots p on p.id=a.row_expense_policy_snapshot_id
          join public.weekly_source_row_resolutions r on r.id=p.row_resolution_id
          join public.weekly_source_upload_rows u on u.id=r.upload_row_id
          join public.weekly_source_final_revisions f on f.id=a.final_revision_id
          join public.weekly_source_cycles c on c.id=f.source_cycle_id
          join public.weekly_work_events w on w.id=a.work_event_id
          join private.bpay_next_source_current_scopes s on s.id=v_scope
          where a.id=v_old and a.id=v_a.prior_expense_authority_generation_id
            and a.state='SUPERSEDED' and a.source_observation_kind='ROW_PRESENT' and a.source_expense_pence=0
            and a.work_event_id=v_a.work_event_id and a.contract_id=v_a.contract_id
            and p.work_event_id=a.work_event_id and p.contract_id=a.contract_id
            and p.source_expense_pence=0 and p.source_expense_vat_enabled=a.source_expense_vat_enabled
            and r.generation=p.generation and r.mapping_state='RESOLVED'
            and r.work_event_id=a.work_event_id and r.contract_id=a.contract_id
            and r.candidate_id=p.candidate_id and r.client_id=p.client_id
            and p.upload_row_id=u.id and f.upload_id=u.upload_id and u.actual_net_minutes=0
            and f.authority_scope_kind='CYCLE' and c.source_group_id=v_revision.source_group_id
            and w.first_source_group_id=v_revision.source_group_id and w.work_date=u.work_date
            and w.candidate_id=s.candidate_id and w.client_id=s.client_id
            and p.candidate_id=s.candidate_id and p.client_id=s.client_id and a.contract_id=s.contract_id
            and v_cached.source_group_id=s.source_group_id and v_cached.client_id=w.client_id
            and v_cached.work_date=w.work_date
            and not exists(select 1 from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=r.id)
            and not exists(select 1 from public.weekly_source_row_economic_snapshots e where e.row_resolution_id=r.id)
            and not exists(select 1 from private.bpay_next_source_current_revisions n
              where n.scope_id=v_scope and n.final_revision_id=a.final_revision_id)
        ) then raise exception 'BPAY_NEXT_SOURCE_EXPENSE_PREDECESSOR_ORIGIN_MISSING' using errcode='55000';end if;
      end if;
    end if;
    perform private.bpay_next_source_current_toggle_v1('EXPENSE',p_authority,true);
    insert into private.bpay_next_source_current_expenses(scope_id,work_event_id,authority_id,positive,source_group_id,client_id,work_date)
      select v_scope,v_a.work_event_id,p_authority,v_a.source_expense_pence>0,v_revision.source_group_id,w.client_id,w.work_date
      from public.weekly_work_events w where w.id=v_a.work_event_id
      on conflict(scope_id,work_event_id) do update set authority_id=excluded.authority_id,positive=excluded.positive;
  end if;
  return jsonb_build_object('origin_id',p_authority,'active',v_revision.is_current,'idempotent',not v_inserted and not v_revision.is_current,'root_bound',true);
end $f$;

create or replace function private.bpay_next_source_current_retract_origin_v1(p_kind text,p_origin uuid,p_session uuid)
returns jsonb language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_origin private.bpay_next_source_current_origins%rowtype;v_s public.weekly_final_source_correction_sessions%rowtype;
  v_old public.weekly_source_final_revisions%rowtype;v_new public.weekly_source_final_revisions%rowtype;v_revision record;
begin
  if p_kind is null or p_kind not in ('MOVEMENT','TRANSITION','EXPENSE') or p_origin is null or p_session is null then
    raise exception 'BPAY_NEXT_SOURCE_RETRACT_INPUT_INVALID' using errcode='22023';end if;
  select o.* into v_origin from private.bpay_next_source_current_origins o where o.origin_kind=p_kind and o.origin_id=p_origin;
  if not found then raise exception 'BPAY_NEXT_SOURCE_RETRACT_ORIGIN_MISSING' using errcode='55000';end if;
  perform 1 from private.bpay_next_source_current_scopes s where s.id=v_origin.scope_id for update;
  select s.* into v_s from public.weekly_final_source_correction_sessions s where s.id=p_session;
  select r.* into v_old from public.weekly_source_final_revisions r where r.id=v_origin.final_revision_id;
  select r.* into v_new from public.weekly_source_final_revisions r where r.id=v_s.prepared_final_revision_id;
  if v_s.id is null or v_s.state not in ('COMMITTING','APPLIED')
    or v_s.expected_current_final_revision_id is distinct from v_old.id or v_old.state<>'SUPERSEDED'
    or v_new.id is null or v_new.predecessor_revision_id is distinct from v_old.id
    or v_old.source_cycle_id is distinct from v_s.source_cycle_id or v_new.source_cycle_id is distinct from v_s.source_cycle_id
    or v_old.authority_scope_kind is distinct from v_s.authority_scope_kind or v_new.authority_scope_kind is distinct from v_s.authority_scope_kind
    or v_old.report_scope_id is distinct from v_s.report_scope_id or v_new.report_scope_id is distinct from v_s.report_scope_id
    or (v_s.state='APPLIED' and v_s.applied_final_revision_id is distinct from v_new.id)
    or v_s.expected_final_manifest_hash is distinct from v_old.manifest_hash then
    raise exception 'BPAY_NEXT_SOURCE_RETRACT_SESSION_UNBOUND' using errcode='55000';end if;
  if v_origin.retracted_by_session_id is not null then
    if v_origin.retracted_by_session_id<>p_session then raise exception 'BPAY_NEXT_SOURCE_RETRACT_REPLAY_MISMATCH' using errcode='55000';end if;
    return jsonb_build_object('origin_id',p_origin,'active',false,'idempotent',true);
  end if;
  if v_new.state<>'CURRENT' then raise exception 'BPAY_NEXT_SOURCE_RETRACT_SESSION_UNBOUND' using errcode='55000';end if;
  select * into strict v_revision from private.bpay_next_source_current_revision_v1(v_new.id);
  perform private.bpay_next_source_current_toggle_v1(p_kind,p_origin,false);
  update private.bpay_next_source_current_origins o set retracted_by_session_id=p_session where o.origin_kind=p_kind and o.origin_id=p_origin;
  return jsonb_build_object('origin_id',p_origin,'active',false,'idempotent',false);
end $f$;

-- Fixed factual projection. Money/rates are text; immutable referenced economic
-- snapshot is the authority, not current Contract rates or a new calculator.
create or replace function private.bpay_next_source_current_event_v1(p_root uuid,p_event uuid)
returns jsonb language plpgsql stable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_scope private.bpay_next_source_current_scopes%rowtype;v_ids uuid[];
  v_event private.bpay_next_source_current_events%rowtype;v_m public.weekly_source_billing_movements%rowtype;
  v_resolution uuid;v_lineage public.weekly_source_row_timesheet_lineages%rowtype;
  v_e public.weekly_source_row_economic_snapshots%rowtype;v_exp public.weekly_expense_authority_generations%rowtype;
  v_result jsonb;v_observation jsonb;v_family uuid;v_decision private.bpay_next_protected_current_decisions%rowtype;
begin
  if p_root is null or p_event is null then raise exception 'BPAY_NEXT_SOURCE_READ_INPUT_INVALID' using errcode='22023';end if;
  select array_agg(x.id) into v_ids from (select s.id from private.bpay_next_source_current_scopes s where s.root_timesheet_id=p_root limit 2) x;
  if coalesce(array_length(v_ids,1),0)<>1 then raise exception 'BPAY_NEXT_SOURCE_SCOPE_UNCAPTURED_OR_AMBIGUOUS' using errcode='55000';end if;
  select s.* into strict v_scope from private.bpay_next_source_current_scopes s where s.id=v_ids[1];
  if not exists(select 1 from public.weekly_work_events w where w.id=p_event and w.candidate_id=v_scope.candidate_id
    and w.client_id=v_scope.client_id and w.first_source_group_id=v_scope.source_group_id
    and w.work_date between v_scope.week_ending_date-6 and v_scope.week_ending_date) then
    raise exception 'BPAY_NEXT_SOURCE_EVENT_OUTSIDE_SCOPE' using errcode='55000';end if;
  -- Latest genuine root observation is NOT selected money provenance. In
  -- particular an empty CorrectFinal has a CURRENT observation but no new
  -- origin and may restore an older immutable surviving positive movement.
  v_observation:=private.bpay_next_source_current_observation_v1(p_root);
  select e.* into v_event from private.bpay_next_source_current_events e where e.scope_id=v_scope.id and e.work_event_id=p_event;
  if coalesce(v_event.ambiguous,false) then raise exception 'BPAY_NEXT_SOURCE_CURRENT_MAGNITUDE_AMBIGUOUS' using errcode='55000';end if;
  select a.* into v_exp from private.bpay_next_source_current_expenses c join public.weekly_expense_authority_generations a on a.id=c.authority_id
    where c.scope_id=v_scope.id and c.work_event_id=p_event;
  if v_exp.id is not null and v_exp.state<>'CURRENT' then raise exception 'BPAY_NEXT_SOURCE_EXPENSE_CACHE_STALE' using errcode='55000';end if;
  select f.id into v_family from public.weekly_exceptional_pay_target_families f
    where btrim(f.root_family_booking_id)=btrim(v_scope.family_booking_id) and f.root_family_booking_id=v_scope.family_booking_id;
  if v_family is not null then select d.* into v_decision from private.bpay_next_protected_current_decisions d where d.family_id=v_family and d.work_event_id=p_event;end if;
  v_result:=jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_CURRENT_EVENT_V1','root_timesheet_id',p_root,'work_event_id',p_event,
    'source_present',coalesce(v_event.source_present,false),'movement_id',v_event.selected_movement_id,
    'transition_id',v_event.selected_transition_id,'source_kind',v_event.source_kind,'protected_decision_id',v_decision.event_id,
    'protected_state',v_decision.state,'expense_authority_id',v_exp.id,
    'source_expense_pence',v_exp.source_expense_pence::text,
    'candidate_reimbursement_ex_vat',v_exp.candidate_reimbursement_ex_vat::text,'client_charge_ex_vat',v_exp.client_charge_ex_vat::text)||v_observation;
  if coalesce(v_event.source_present,false) then
    select m.* into strict v_m from public.weekly_source_billing_movements m where m.id=v_event.selected_movement_id;
    if v_m.placement_state='VOIDED_BY_CORRECT_FINAL' then raise exception 'BPAY_NEXT_SOURCE_MOVEMENT_CACHE_STALE' using errcode='55000';end if;
    v_resolution:=(v_m.source_facts_json->>'row_resolution_id')::uuid;
    select l.* into strict v_lineage from public.weekly_source_row_timesheet_lineages l where l.row_resolution_id=v_resolution;
    select e.* into strict v_e from public.weekly_source_row_economic_snapshots e where e.row_resolution_id=v_resolution;
    v_result:=v_result||jsonb_build_object('final_revision_id',v_m.final_revision_id,'row_resolution_id',v_resolution,
      'lineage_id',v_lineage.id,'economic_snapshot_id',v_e.id,'original_invoice_timesheet_id',v_m.invoice_timesheet_id,
      'source_facts',jsonb_build_object('work_date',v_m.source_facts_json->>'work_date','start_at_local',v_m.source_facts_json->>'start_at_local',
        'end_at_local',v_m.source_facts_json->>'end_at_local','break_minutes',v_m.source_facts_json->>'break_minutes'),
      'hours',jsonb_build_object('day',v_e.hours_day::text,'night',v_e.hours_night::text,'sat',v_e.hours_sat::text,'sun',v_e.hours_sun::text,'bh',v_e.hours_bh::text),
      'pay_rates',jsonb_build_object('day',v_e.pay_day::text,'night',v_e.pay_night::text,'sat',v_e.pay_sat::text,'sun',v_e.pay_sun::text,'bh',v_e.pay_bh::text),
      'charge_rates',jsonb_build_object('day',v_e.charge_day::text,'night',v_e.charge_night::text,'sat',v_e.charge_sat::text,'sun',v_e.charge_sun::text,'bh',v_e.charge_bh::text),
      'pay_pence',v_m.canonical_pay_vector_json->>'total_pence','charge_pence',v_m.canonical_charge_vector_json->>'total_pence');
  end if;
  if octet_length(v_result::text)>122880 then raise exception 'BPAY_NEXT_SOURCE_CURRENT_BYTE_BOUND' using errcode='54000';end if;
  return v_result;
end $f$;

create or replace function private.bpay_next_source_current_page_v1(p_root uuid,p_after_event uuid,p_limit integer default 100)
returns jsonb language plpgsql stable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_scope uuid;v_row record;v_rows jsonb:='[]';v_last uuid;v_more boolean:=false;v_count integer:=0;
begin
  if p_limit is null or p_limit not between 1 and 100 or p_root is null then raise exception 'BPAY_NEXT_SOURCE_PAGE_INPUT_INVALID' using errcode='22023';end if;
  select s.id into strict v_scope from private.bpay_next_source_current_scopes s where s.root_timesheet_id=p_root;
  -- <=101 active SHIFT/positive EXPENSE keys, never all absent old events.
  for v_row in select x.work_event_id from (
    (select e.work_event_id from private.bpay_next_source_current_events e where e.scope_id=v_scope
      and (e.source_present or e.ambiguous) and e.work_event_id>=coalesce(p_after_event,'00000000-0000-0000-0000-000000000000'::uuid)
      and (p_after_event is null or e.work_event_id<>p_after_event)
      order by e.work_event_id limit p_limit+1)
    union
    (select e.work_event_id from private.bpay_next_source_current_expenses e where e.scope_id=v_scope
      and e.positive and e.work_event_id>=coalesce(p_after_event,'00000000-0000-0000-0000-000000000000'::uuid)
      and (p_after_event is null or e.work_event_id<>p_after_event)
      order by e.work_event_id limit p_limit+1)) x order by x.work_event_id limit p_limit+1 loop
    v_count:=v_count+1;if v_count>p_limit then v_more:=true;exit;end if;
    v_rows:=v_rows||jsonb_build_array(private.bpay_next_source_current_event_v1(p_root,v_row.work_event_id));v_last:=v_row.work_event_id;
    if octet_length(v_rows::text)>122000 then raise exception 'BPAY_NEXT_SOURCE_CURRENT_BYTE_BOUND' using errcode='54000';end if;
  end loop;
  return jsonb_build_object('rows',v_rows,'complete',not v_more,'next_event_id',case when v_more then v_last else null end);
end $f$;

create or replace function private.bpay_next_protected_current_decision_capture_v1(p_event uuid)
returns jsonb language plpgsql security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_e public.weekly_exceptional_pay_family_events%rowtype;v_f public.weekly_exceptional_pay_target_families%rowtype;
  v_existing private.bpay_next_protected_current_decisions%rowtype;
begin
  select e.* into v_e from public.weekly_exceptional_pay_family_events e where e.id=p_event;
  if not found or v_e.state not in ('WAIT','ACCEPTED_SOURCE','NOT_WORKED','FIRST_AUTHORISATION_WITHDRAWN') then
    raise exception 'BPAY_NEXT_PROTECTED_DECISION_NOT_ADMITTED' using errcode='55000';end if;
  select f.* into strict v_f from public.weekly_exceptional_pay_target_families f where f.id=v_e.family_id for update;
  if not exists(select 1 from public.weekly_exceptional_payment_approvals a where a.id=v_e.evidence_approval_id)
    or not exists(select 1 from public.weekly_work_events w join public.contracts c on c.id=v_f.contract_id
      where w.id=v_e.durable_work_event_id and w.candidate_id=v_f.candidate_id
      and c.candidate_id=v_f.candidate_id and c.client_id=w.client_id
      and w.work_date between v_f.week_start_date and v_f.week_ending_date) then
    raise exception 'BPAY_NEXT_PROTECTED_DECISION_UNBOUND' using errcode='55000';end if;
  select d.* into v_existing from private.bpay_next_protected_current_decisions d where d.family_id=v_e.family_id and d.work_event_id=v_e.durable_work_event_id;
  if v_existing.event_id=p_event then
    if v_existing.event_hash is distinct from v_e.event_hash then raise exception 'BPAY_NEXT_PROTECTED_DECISION_CHANGED' using errcode='55000';end if;
    return jsonb_build_object('event_id',p_event,'idempotent',true);end if;
  if v_existing.event_sequence is null or v_e.event_sequence>v_existing.event_sequence then
    insert into private.bpay_next_protected_current_decisions(family_id,work_event_id,event_id,event_sequence,state,event_hash)
      values(v_e.family_id,v_e.durable_work_event_id,v_e.id,v_e.event_sequence,v_e.state,v_e.event_hash)
      on conflict(family_id,work_event_id) do update set event_id=excluded.event_id,event_sequence=excluded.event_sequence,state=excluded.state,event_hash=excluded.event_hash;
  end if;
  return jsonb_build_object('event_id',p_event,'idempotent',false);
end $f$;

create or replace function private.bpay_next_protected_current_decision_page_v1(p_family uuid,p_after_event uuid,p_limit integer default 100)
returns jsonb language plpgsql stable security definer
set search_path='pg_catalog','private','public','pg_temp' as $f$
declare v_row record;v_rows jsonb:='[]';v_last uuid;v_count integer:=0;v_more boolean:=false;
begin
  if p_family is null or p_limit is null or p_limit not between 1 and 100 then raise exception 'BPAY_NEXT_PROTECTED_PAGE_INPUT_INVALID' using errcode='22023';end if;
  if not exists(select 1 from public.weekly_exceptional_pay_target_families f where f.id=p_family) then
    raise exception 'BPAY_NEXT_PROTECTED_FAMILY_MISSING' using errcode='55000';end if;
  for v_row in select d.work_event_id,d.event_id,d.event_sequence,d.state,encode(d.event_hash,'hex') as event_hash
    from private.bpay_next_protected_current_decisions d where d.family_id=p_family and d.state='WAIT'
      and d.work_event_id>=coalesce(p_after_event,'00000000-0000-0000-0000-000000000000'::uuid)
      and (p_after_event is null or d.work_event_id<>p_after_event) order by d.work_event_id limit p_limit+1 loop
    v_count:=v_count+1;if v_count>p_limit then v_more:=true;exit;end if;
    v_rows:=v_rows||jsonb_build_array(jsonb_build_object('work_event_id',v_row.work_event_id,'event_id',v_row.event_id,
      'event_sequence',v_row.event_sequence::text,'state',v_row.state,'event_hash',v_row.event_hash));v_last:=v_row.work_event_id;
  end loop;
  return jsonb_build_object('rows',v_rows,'complete',not v_more,'next_event_id',case when v_more then v_last else null end);
end $f$;

-- Exact finite signatures only. Helpers are owner-only too; no public RPC,
-- caller-supplied economics, service/browser grants or arbitrary SQL dispatch.
do $acl$
declare v_signature text;
begin
  foreach v_signature in array array[
    'private.bpay_next_source_order_key_v1(date,timestamptz,integer)',
    'private.bpay_next_source_prefix_v1(bit,integer)',
    'private.bpay_next_source_current_revision_v1(uuid)',
    'private.bpay_next_source_current_scope_v1(uuid,uuid,uuid,uuid)',
    'private.bpay_next_source_nhsp_delta_v1(uuid,uuid,bytea,bit,integer,integer)',
    'private.bpay_next_source_nhsp_head_v1(uuid,uuid,bytea)',
    'private.bpay_next_source_current_refresh_v1(uuid,uuid)',
    'private.bpay_next_source_current_toggle_v1(text,uuid,boolean)',
    'private.bpay_next_source_current_capture_movement_v1(uuid)',
    'private.bpay_next_source_current_capture_transition_v1(uuid)',
    'private.bpay_next_source_zero_scope_v1(uuid,uuid,uuid)',
    'private.bpay_next_source_current_capture_expense_v1(uuid)',
    'private.bpay_next_source_current_retract_origin_v1(text,uuid,uuid)',
    'private.bpay_next_source_current_event_v1(uuid,uuid)',
    'private.bpay_next_source_current_page_v1(uuid,uuid,integer)',
    'private.bpay_next_protected_current_decision_capture_v1(uuid)',
    'private.bpay_next_protected_current_decision_page_v1(uuid,uuid,integer)'
  ] loop
    execute format('alter function %s owner to %I',v_signature,current_user);
    execute format('revoke all on function %s from public,anon,authenticated,service_role',v_signature);
  end loop;
end $acl$;

commit;
