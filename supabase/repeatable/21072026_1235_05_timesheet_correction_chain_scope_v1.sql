create or replace function public.timesheet_correction_chain_scope_v1(
  p_timesheet_id uuid,
  p_lock_rows boolean default false,
  p_max_depth integer default 32,
  p_max_members integer default 100
)
returns jsonb
language plpgsql
security definer
set search_path to 'public', 'extensions', 'pg_temp'
set plpgsql_check.mode to 'disabled'
as $function$
declare
  v_root_id uuid;
  v_member_ids uuid[] := array[]::uuid[];
  v_member_count integer := 0;
  v_cycle boolean := false;
  v_truncated boolean := false;
  v_units jsonb := '[]'::jsonb;
  v_members jsonb := '[]'::jsonb;
  v_errors jsonb := '[]'::jsonb;
  v_latest_positive uuid;
  v_fingerprint_payload jsonb;
  v_chain_fingerprint text;
  v_requested_unit jsonb;
  v_requested_operation_id text;
begin
  if p_timesheet_id is null then
    raise exception 'CORRECTION_CHAIN_TIMESHEET_ID_REQUIRED' using errcode='22023';
  end if;
  if p_max_depth < 1 or p_max_depth > 32 then
    raise exception 'CORRECTION_CHAIN_MAX_DEPTH_OUT_OF_RANGE' using errcode='22023';
  end if;
  if p_max_members < 1 or p_max_members > 100 then
    raise exception 'CORRECTION_CHAIN_MAX_MEMBERS_OUT_OF_RANGE' using errcode='22023';
  end if;
  if not exists (select 1 from public.timesheets where timesheet_id=p_timesheet_id) then
    raise exception 'CORRECTION_CHAIN_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;

  with recursive ancestors as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id=p_timesheet_id
    union all
    select p.timesheet_id, p.parent_timesheet_id, a.depth+1,
           a.path||p.timesheet_id, p.timesheet_id=any(a.path)
    from ancestors a
    join public.timesheets p on p.timesheet_id=a.parent_timesheet_id
    where a.parent_timesheet_id is not null and not a.cycle and a.depth<p_max_depth
  )
  select a.timesheet_id,
         coalesce(bool_or(a.cycle) over (),false)
  into v_root_id, v_cycle
  from ancestors a
  order by a.depth desc
  limit 1;

  with recursive descendants as (
    select ts.timesheet_id, ts.parent_timesheet_id, 0 depth,
           array[ts.timesheet_id]::uuid[] path, false cycle
    from public.timesheets ts where ts.timesheet_id=v_root_id
    union all
    select c.timesheet_id, c.parent_timesheet_id, d.depth+1,
           d.path||c.timesheet_id, c.timesheet_id=any(d.path)
    from descendants d
    join public.timesheets c on c.parent_timesheet_id=d.timesheet_id
    where not d.cycle and d.depth<p_max_depth
  ), picked as (
    select distinct d.timesheet_id from descendants d where not d.cycle
  )
  select coalesce(array_agg(p.timesheet_id order by p.timesheet_id),array[]::uuid[]),
         count(*)::integer,
         exists(select 1 from descendants where cycle),
         exists(
           select 1 from public.timesheets child
           join descendants edge on edge.timesheet_id=child.parent_timesheet_id
           where edge.depth=p_max_depth
         )
  into v_member_ids,v_member_count,v_cycle,v_truncated
  from picked p;

  if v_member_count>p_max_members then
    raise exception 'CORRECTION_CHAIN_MEMBER_LIMIT_EXCEEDED'
      using errcode='22023', detail=jsonb_build_object('count',v_member_count,'max',p_max_members)::text;
  end if;
  if p_lock_rows then
    if not pg_try_advisory_xact_lock(hashtextextended('CORRECTION_CHAIN|'||v_root_id::text,21072026)) then
      raise exception 'CORRECTION_CHAIN_LOCK_BUSY' using errcode='55P03';
    end if;
    perform 1 from public.timesheets ts
    where ts.timesheet_id=any(v_member_ids) order by ts.timesheet_id for update;
  end if;

  with member_rows as (
    select ts.*,
      public._ctms_import_correction_classify_v1(ts.timesheet_id) as class_json,
      case when upper(btrim(coalesce(ts.adjustment_origin,''))) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
        then public._ctms_correction_policy_envelope_read_v1(ts.timesheet_id) else null end as envelope
    from public.timesheets ts where ts.timesheet_id=any(v_member_ids)
  ), unit_members as (
    select mr.*,
      mr.envelope#>>'{operation,operation_id}' operation_id,
      mr.envelope->>'correction_chain_id' correction_chain_id,
      case
        when upper(btrim(coalesce(mr.correction_kind,''))) in
          ('CHANGED_HOURS_REVERSAL','CANCELLATION_REVERSAL') then 'REVERSAL'
        when upper(btrim(coalesce(mr.correction_kind,''))) in
          ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT') then 'REPLACEMENT'
        else 'INVALID'
      end member_role
    from member_rows mr
    where coalesce((mr.class_json->>'is_import_authoritative_correction')::boolean,false)=true
  ), unit_rows as (
    select operation_id,correction_chain_id,
      min(correction_id) correction_id,
      count(distinct correction_id)::integer correction_id_count,
      count(*) filter (where member_role='REVERSAL')::integer reversal_count,
      count(*) filter (where member_role='REPLACEMENT')::integer replacement_count,
      count(*)::integer actual_member_count,
      jsonb_agg(member_role order by case member_role when 'REVERSAL' then 1 when 'REPLACEMENT' then 2 else 3 end) actual_member_roles,
      array_agg(timesheet_id order by timesheet_id) member_ids,
      count(distinct envelope->>'envelope_fingerprint')::integer envelope_count,
      count(distinct envelope->>'root_timesheet_id')::integer envelope_root_count,
      min(envelope->>'envelope_fingerprint') envelope_fingerprint,
      min(envelope::text)::jsonb envelope
    from unit_members
    where nullif(btrim(coalesce(operation_id,'')),'') is not null
      and nullif(btrim(coalesce(correction_chain_id,'')),'') is not null
    group by operation_id,correction_chain_id
  )
  select coalesce(jsonb_agg(jsonb_build_object(
    'correction_id',u.correction_id,
    'correction_operation_id',u.operation_id,
    'correction_chain_id',u.correction_chain_id,
    'correction_shape',u.envelope->>'correction_shape',
    'expected_member_count',(u.envelope->>'expected_member_count')::integer,
    'expected_member_roles',u.envelope->'expected_member_roles',
    'actual_member_count',u.actual_member_count,
    'actual_member_roles',u.actual_member_roles,
    'member_ids',to_jsonb(u.member_ids),
    'reversal_count',u.reversal_count,
    'replacement_count',u.replacement_count,
    'envelope_fingerprint',u.envelope_fingerprint,
    'policy_envelope',u.envelope,
    'valid',u.operation_id is not null
      and u.correction_chain_id is not null
      and u.correction_id_count=1
      and u.envelope_count=1
      and u.envelope_root_count=1
      and u.envelope->>'root_timesheet_id' is not distinct from v_root_id::text
      and u.envelope->>'correction_shape' in ('REVERSAL_ONLY','REVERSAL_REPLACEMENT')
      and u.actual_member_count=(u.envelope->>'expected_member_count')::integer
      and u.actual_member_roles=u.envelope->'expected_member_roles'
      and u.reversal_count=1
      and u.replacement_count=case when u.envelope->>'correction_shape'='REVERSAL_ONLY' then 0 else 1 end
  ) order by u.operation_id),'[]'::jsonb)
  into v_units
  from unit_rows u;

  select coalesce(jsonb_agg(jsonb_build_object(
    'timesheet_id',ts.timesheet_id,
    'parent_timesheet_id',ts.parent_timesheet_id,
    'booking_id',ts.booking_id,
    'is_current',ts.is_current,
    'status',ts.status,
    'correction_id',ts.correction_id,
    'correction_kind',ts.correction_kind,
    'adjustment_origin',ts.adjustment_origin,
    'policy_envelope_fingerprint',coalesce(
      ts.candidate_hint_text#>>'{correction_financials_policy_envelope,envelope_fingerprint}',
      tf.policy_snapshot_json#>>'{correction_financials_policy_envelope,envelope_fingerprint}'
    ),
    'current_tsfin_id',tf.id,
    'processing_status',tf.processing_status,
    'paid_at_utc',tf.paid_at_utc,
    'locked_by_invoice_id',tf.locked_by_invoice_id
  ) order by ts.created_at,ts.timesheet_id),'[]'::jsonb)
  into v_members
  from public.timesheets ts
  left join public.timesheets_financials tf on tf.timesheet_id=ts.timesheet_id and tf.is_current=true
  where ts.timesheet_id=any(v_member_ids);

  if v_cycle then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_CHAIN_CYCLE')); end if;
  if v_truncated then v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_CHAIN_DEPTH_EXCEEDED')); end if;
  if exists(select 1 from jsonb_array_elements(v_units) u where coalesce((u->>'valid')::boolean,false)=false) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_UNIT_INVALID'));
  end if;
  if exists (
    select 1
    from public.timesheets ts
    where ts.timesheet_id=any(v_member_ids)
      and upper(btrim(coalesce(ts.adjustment_origin,''))) in ('IMPORT_CORRECTION','IMPORT_CANCELLATION')
      and not coalesce((public._ctms_import_correction_classify_v1(ts.timesheet_id)->>'is_import_authoritative_correction')::boolean,false)
  ) then
    v_errors:=v_errors||jsonb_build_array(jsonb_build_object('code','CORRECTION_MEMBER_CONTRACT_INVALID'));
  end if;

  select ts.timesheet_id into v_latest_positive
  from public.timesheets ts
  where ts.timesheet_id=any(v_member_ids)
    and (
      ts.timesheet_id=v_root_id
      or upper(btrim(coalesce(ts.correction_kind,''))) in ('CHANGED_HOURS_REPLACEMENT','CANCELLATION_REPLACEMENT')
    )
  order by ts.created_at desc,ts.timesheet_id desc limit 1;

  if coalesce((public._ctms_import_correction_classify_v1(p_timesheet_id)->>'is_import_authoritative_correction')::boolean,false) then
    v_requested_operation_id:=public._ctms_correction_policy_envelope_read_v1(p_timesheet_id)#>>'{operation,operation_id}';
    select u into v_requested_unit
    from jsonb_array_elements(v_units) u
    where u->>'correction_operation_id'=v_requested_operation_id
    limit 1;
  end if;

  v_fingerprint_payload:=jsonb_build_object(
    'root_timesheet_id',v_root_id,'member_ids',to_jsonb(v_member_ids),
    'correction_units',v_units,'latest_positive_timesheet_id',v_latest_positive
  );
  v_chain_fingerprint:=encode(extensions.digest(convert_to(v_fingerprint_payload::text,'UTF8'),'sha256'::text),'hex');

  return jsonb_build_object(
    'ok',jsonb_array_length(v_errors)=0,
    'valid',jsonb_array_length(v_errors)=0,
    'requested_timesheet_id',p_timesheet_id,
    'root_timesheet_id',v_root_id,
    'latest_positive_timesheet_id',v_latest_positive,
    'member_count',v_member_count,'member_ids',to_jsonb(v_member_ids),
    'member_timesheet_ids',to_jsonb(v_member_ids),'members',v_members,
    'correction_units',v_units,'pairs',v_units,'requested_correction_unit',v_requested_unit,
    'correction_shape',v_requested_unit->>'correction_shape',
    'correction_financials_policy_required',v_requested_unit is not null,
    'correction_financials_policy_envelope_required',v_requested_unit is not null,
    'correction_financials_policy_envelope',v_requested_unit->'policy_envelope',
    'correction_financials_policy_envelope_fingerprint',v_requested_unit->>'envelope_fingerprint',
    'chain_fingerprint',v_chain_fingerprint,'errors',v_errors
  );
end;
$function$;

comment on function public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer) is
  'Resolves a bounded correction chain as independent correction units. Supports reversal/replacement and reversal-only without requiring one universal policy across later corrections.';
revoke all on function public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer) from public,anon,authenticated;
grant execute on function public.timesheet_correction_chain_scope_v1(uuid,boolean,integer,integer) to service_role;
