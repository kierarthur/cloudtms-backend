-- Genuine Source writer hooks. No historic bootstrap, generic dispatch,
-- PREPARED visibility, financial recomputation or public/browser grants.
\set ON_ERROR_STOP on
begin;

-- All current coverage reads START from the partial maintained-current
-- indexes. The exact event PK validates identity; absent old event history is
-- never the range-scan input. New/removed revision work is payload-local.
create or replace function private.bpay_next_source_current_inventory_v1(
  p_upload uuid,p_generation integer,p_old_revision uuid,p_group uuid,p_client uuid,p_start date,p_end date
) returns table(root_timesheet_id uuid) language sql stable security definer
set search_path=pg_catalog,private,public as $f$
  select cw.timesheet_id
  from public.weekly_source_upload_rows u
  join public.weekly_source_row_resolutions r on r.upload_row_id=u.id and r.generation=p_generation
  join public.contracts c on c.id=r.contract_id
  join public.contract_weeks cw on cw.contract_id=r.contract_id and cw.additional_seq=0
    and cw.week_ending_date=u.work_date+((coalesce(c.week_ending_weekday_snapshot,0)-extract(dow from u.work_date)::integer+7)%7)
  where u.upload_id=p_upload and cw.timesheet_id is not null
  union
  select s.root_timesheet_id from private.bpay_next_source_current_events e
  join private.bpay_next_source_current_scopes s on s.id=e.scope_id
  join public.weekly_work_events w on w.id=e.work_event_id
  where e.source_group_id=p_group and e.client_id=p_client and e.work_date between p_start and p_end
    and e.source_kind='HR' and (e.source_present or e.ambiguous)
    and w.first_source_group_id=e.source_group_id and w.client_id=e.client_id and w.work_date=e.work_date
  union
  select s.root_timesheet_id from private.bpay_next_source_current_expenses e
  join private.bpay_next_source_current_scopes s on s.id=e.scope_id
  join public.weekly_work_events w on w.id=e.work_event_id
  where e.source_group_id=p_group and e.client_id=p_client and e.work_date between p_start and p_end and e.positive
    and w.first_source_group_id=e.source_group_id and w.client_id=e.client_id and w.work_date=e.work_date
  union
  select s.root_timesheet_id from private.bpay_next_source_current_observations o
  join private.bpay_next_source_current_scopes s on s.id=o.scope_id
  where o.final_revision_id=p_old_revision and o.active
  union
  select r.root_timesheet_id from public.weekly_source_ordinary_pay_projection_receipts r
  where r.final_revision_id=p_old_revision and r.outcome in ('PREPARED_FOR_AUTHORISATION','PROPOSED')
$f$;

-- Re-read exact Source membership and raw booking identity relationally. No
-- whole-operation array/JSON/digest stands in for actual root membership.
create or replace function private.bpay_next_source_current_lock_recheck_v1(p_operation uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_op private.bpay_next_source_current_lock_operations%rowtype;
begin
  select o.* into strict v_op from private.bpay_next_source_current_lock_operations o
    where o.id=p_operation and o.transaction_id=pg_current_xact_id();
  if exists(
    select i.root_timesheet_id from private.bpay_next_source_current_inventory_v1(
      v_op.upload_id,v_op.generation,v_op.old_revision_id,v_op.source_group_id,v_op.client_id,v_op.coverage_start,v_op.coverage_end) i
    except select m.root_timesheet_id from private.bpay_next_source_current_lock_members m where m.operation_id=p_operation
  ) or exists(
    select m.root_timesheet_id from private.bpay_next_source_current_lock_members m where m.operation_id=p_operation
    except select i.root_timesheet_id from private.bpay_next_source_current_inventory_v1(
      v_op.upload_id,v_op.generation,v_op.old_revision_id,v_op.source_group_id,v_op.client_id,v_op.coverage_start,v_op.coverage_end) i
  ) or exists(select 1 from private.bpay_next_source_current_lock_members m
    left join public.timesheets t on t.timesheet_id=m.root_timesheet_id
    where m.operation_id=p_operation and (t.timesheet_id is null or t.booking_id is distinct from m.raw_booking_id)) then
    raise exception 'BPAY_NEXT_SOURCE_LOCK_INVENTORY_CHANGED' using errcode='40001';
  end if;
  if v_op.upload_id is not null and not exists(select 1 from public.weekly_source_projection_publications p
    join public.weekly_source_uploads u on u.id=p.upload_id
    join public.weekly_source_cycles c on c.id=u.source_cycle_id
    where p.id=v_op.projection_publication_id and u.id=v_op.upload_id
      and coalesce(p.projection_generation,p.authority_scope_version::integer)=v_op.generation
      and p.state in ('CURRENT','CORRECTION_READY') and c.source_group_id=v_op.source_group_id
      and u.confirmed_coverage_start_local_date is not distinct from v_op.coverage_start
      and u.confirmed_coverage_end_local_date is not distinct from v_op.coverage_end
      and (u.correction_session_id is null or exists(select 1 from public.weekly_final_source_correction_sessions s
        where s.id=u.correction_session_id and s.expected_current_final_revision_id=v_op.old_revision_id))
  ) then raise exception 'BPAY_NEXT_SOURCE_LOCK_OPERATION_CHANGED' using errcode='40001';end if;
  if v_op.old_revision_id is not null and not exists(select 1 from public.weekly_source_final_revisions r
    join public.weekly_source_cycles c on c.id=r.source_cycle_id
    join public.weekly_source_client_manifests m on m.final_revision_id=r.id
    where r.id=v_op.old_revision_id and r.state='CURRENT' and c.source_group_id=v_op.source_group_id and m.client_id=v_op.client_id
  ) then raise exception 'BPAY_NEXT_SOURCE_LOCK_OPERATION_CHANGED' using errcode='40001';end if;
end $f$;

create or replace function private.bpay_next_source_current_lock_release_v1(p_operation uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private as $f$
begin
  delete from private.bpay_next_source_current_lock_operations o
    where o.id=p_operation and o.transaction_id=pg_current_xact_id();
  if not found then raise exception 'BPAY_NEXT_SOURCE_LOCK_OPERATION_UNBOUND' using errcode='55000';end if;
end $f$;

-- Typed transaction-local inventory. The actual Source payload/maintained
-- current coverage can contain many roots; it has no invented global bound.
-- Stream ALL canonical/raw keys in the retained order before ANY physical row
-- guard. Only then pass <=100 roots per call to the unchanged physical guard.
create or replace function private.bpay_next_source_current_lock_inventory_v1(
  p_upload uuid,p_generation integer,p_old_revision uuid,p_group uuid,p_client uuid,p_start date,p_end date
) returns uuid language plpgsql security definer
set search_path=pg_catalog,private,public as $f$
declare v_operation uuid;v_publication uuid;v_key record;v_page uuid[];v_after uuid;v_result jsonb;
begin
  -- Inert for the retained default LEGACY / DISABLED paths. The module row
  -- share lock keeps one actual writer's ownership decision stable to COMMIT.
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then return null;end if;
  if p_group is null or p_client is null or (p_upload is null)<>(p_generation is null)
    or (p_upload is null and p_old_revision is null) then
    raise exception 'BPAY_NEXT_SOURCE_LOCK_OPERATION_INPUT_INVALID' using errcode='22023';end if;
  if p_upload is not null then
    select p.id into strict v_publication from public.weekly_source_projection_publications p
      join public.weekly_source_uploads u on u.id=p.upload_id
      join public.weekly_source_cycles c on c.id=u.source_cycle_id
      where u.id=p_upload and c.source_group_id=p_group and p.state in ('CURRENT','CORRECTION_READY')
        and coalesce(p.projection_generation,p.authority_scope_version::integer)=p_generation
        and u.confirmed_coverage_start_local_date is not distinct from p_start
        and u.confirmed_coverage_end_local_date is not distinct from p_end
        and (u.correction_session_id is null or exists(select 1 from public.weekly_final_source_correction_sessions s
          where s.id=u.correction_session_id and s.expected_current_final_revision_id=p_old_revision));
  end if;
  insert into private.bpay_next_source_current_lock_operations(upload_id,projection_publication_id,generation,old_revision_id,
    source_group_id,client_id,coverage_start,coverage_end)
    values(p_upload,v_publication,p_generation,p_old_revision,p_group,p_client,p_start,p_end) returning id into v_operation;
  if exists(select 1 from private.bpay_next_source_current_inventory_v1(p_upload,p_generation,p_old_revision,p_group,p_client,p_start,p_end) i
    left join public.timesheets t on t.timesheet_id=i.root_timesheet_id
    where t.timesheet_id is null or t.booking_id is null or btrim(t.booking_id)='') then
    raise exception 'BPAY_NEXT_SOURCE_LOCK_ROOT_UNBOUND' using errcode='55000';end if;
  insert into private.bpay_next_source_current_lock_members(operation_id,root_timesheet_id,raw_booking_id,canonical_booking_id)
    select v_operation,t.timesheet_id,t.booking_id,btrim(t.booking_id)
    from private.bpay_next_source_current_inventory_v1(p_upload,p_generation,p_old_revision,p_group,p_client,p_start,p_end) i
    join public.timesheets t on t.timesheet_id=i.root_timesheet_id;
  for v_key in select m.canonical_booking_id as canonical,m.raw_booking_id as raw
    from private.bpay_next_source_current_lock_members m where m.operation_id=v_operation
    order by m.canonical_booking_id,m.raw_booking_id,m.root_timesheet_id loop
    perform pg_advisory_xact_lock(hashtext(v_key.canonical));
    if v_key.raw<>v_key.canonical then perform pg_advisory_xact_lock(hashtext(v_key.raw));end if;
  end loop;
  perform private.bpay_next_source_current_lock_recheck_v1(v_operation);
  loop
    select array_agg(x.root_timesheet_id order by x.root_timesheet_id) into v_page from (
      select m.root_timesheet_id from private.bpay_next_source_current_lock_members m where m.operation_id=v_operation
        and (v_after is null or m.root_timesheet_id>v_after) order by m.root_timesheet_id limit 100
    ) x;
    exit when v_page is null;
    v_result:=private.weekly_source_lock_family_rows_v1(v_page,null);
    if coalesce((v_result->>'ok')::boolean,false) is not true then
      raise exception '%',v_result->>'code' using errcode='55000',detail=v_result::text;end if;
    v_after:=v_page[cardinality(v_page)];
  end loop;
  perform private.bpay_next_source_current_lock_recheck_v1(v_operation);
  return v_operation;
end $f$;

-- Ordinary association is an exact newly captured CURRENT origin. An empty
-- correction association instead carries one exact old observation through
-- the real session's old/new authority CAS. Neither invents money/lineage.
create or replace function private.bpay_next_source_current_observe_v1(p_scope uuid,p_revision uuid,p_session uuid default null)
returns void language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_r public.weekly_source_final_revisions%rowtype;v_scope private.bpay_next_source_current_scopes%rowtype;
  v_s public.weekly_final_source_correction_sessions%rowtype;v_existing private.bpay_next_source_current_observations%rowtype;
  v_authority record;v_old private.bpay_next_source_current_observations%rowtype;
begin
  select s.* into strict v_scope from private.bpay_next_source_current_scopes s where s.id=p_scope for update;
  select r.* into strict v_r from public.weekly_source_final_revisions r where r.id=p_revision;
  select * into strict v_authority from private.bpay_next_source_current_revision_v1(p_revision);
  if not v_authority.is_current or v_scope.source_group_id<>v_authority.source_group_id then
    raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_NOT_CURRENT' using errcode='55000';end if;
  if p_session is null then
    if not exists(select 1 from private.bpay_next_source_current_origins o where o.scope_id=p_scope
      and o.final_revision_id=p_revision and o.active) then
      raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_UNBOUND' using errcode='55000';end if;
  else
    select s.* into strict v_s from public.weekly_final_source_correction_sessions s where s.id=p_session;
    select o.* into strict v_old from private.bpay_next_source_current_observations o
      where o.scope_id=p_scope and o.final_revision_id=v_s.expected_current_final_revision_id;
    if v_s.state<>'COMMITTING' or v_s.prepared_final_revision_id is distinct from p_revision
      or v_r.predecessor_revision_id is distinct from v_s.expected_current_final_revision_id
      or v_r.source_cycle_id is distinct from v_s.source_cycle_id
      or v_r.authority_scope_kind is distinct from v_s.authority_scope_kind or v_r.report_scope_id is distinct from v_s.report_scope_id
      or v_old.manifest_hash is distinct from v_s.expected_final_manifest_hash
      or v_old.retracted_by_session_id is distinct from p_session or v_old.active then
      raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_SESSION_UNBOUND' using errcode='55000';end if;
  end if;
  select o.* into v_existing from private.bpay_next_source_current_observations o where o.scope_id=p_scope and o.final_revision_id=p_revision;
  if found then
    if v_existing.order_key is distinct from v_authority.order_key or v_existing.manifest_hash is distinct from v_r.manifest_hash
      or not v_existing.active or v_existing.retracted_by_session_id is not null then
      raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_CHANGED' using errcode='55000';end if;
    return;
  end if;
  insert into private.bpay_next_source_current_observations(scope_id,final_revision_id,order_key,manifest_hash,active,accepted_by_session_id)
    values(p_scope,p_revision,v_authority.order_key,v_r.manifest_hash,true,p_session);
end $f$;

create or replace function private.bpay_next_source_current_observation_v1(p_root uuid)
returns jsonb language plpgsql stable security definer set search_path=pg_catalog,private,public as $f$
declare v_scopes uuid[];v_scope uuid;v_o record;v_first record;v_count integer:=0;v_authority record;
begin
  if p_root is null then raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_INPUT_INVALID' using errcode='22023';end if;
  select array_agg(x.id) into v_scopes from (select s.id from private.bpay_next_source_current_scopes s where s.root_timesheet_id=p_root limit 2) x;
  if coalesce(cardinality(v_scopes),0)=0 then return jsonb_build_object('source_observed',false,'current_observation_final_revision_id',null);end if;
  if cardinality(v_scopes)<>1 then raise exception 'BPAY_NEXT_SOURCE_SCOPE_UNCAPTURED_OR_AMBIGUOUS' using errcode='55000';end if;
  v_scope:=v_scopes[1];
  for v_o in select o.* from private.bpay_next_source_current_observations o where o.scope_id=v_scope and o.active
    order by o.order_key desc,o.final_revision_id limit 2 loop
    v_count:=v_count+1;if v_count=1 then v_first:=v_o;
    elsif v_o.order_key=v_first.order_key then raise exception 'BPAY_NEXT_SOURCE_REVISION_HEAD_AMBIGUOUS' using errcode='55000';end if;
  end loop;
  if v_count=0 then raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_UNCAPTURED' using errcode='55000';end if;
  select * into strict v_authority from private.bpay_next_source_current_revision_v1(v_first.final_revision_id);
  if not v_authority.is_current or v_authority.order_key is distinct from v_first.order_key
    or not exists(select 1 from public.weekly_source_final_revisions r where r.id=v_first.final_revision_id and r.manifest_hash=v_first.manifest_hash) then
    raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_STALE' using errcode='55000';end if;
  return jsonb_build_object('source_observed',true,'current_observation_final_revision_id',v_first.final_revision_id,
    'current_observation_manifest_hash',encode(v_first.manifest_hash,'hex'));
end $f$;

-- A current complete report can restate an unchanged expense without emitting
-- a new authority generation. Advance only its root observation/current
-- pointer from this exact accepted row; keep original authority/money IDs.
-- Every proof below is a PK/current-unique lookup, not a scope/history scan.
create or replace function private.bpay_next_source_current_observe_expense_row_v1(p_revision uuid,p_policy uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_final public.weekly_source_final_revisions%rowtype;v_upload public.weekly_source_uploads%rowtype;
  v_publication public.weekly_source_projection_publications%rowtype;v_policy public.weekly_source_row_expense_policy_snapshots%rowtype;
  v_authority public.weekly_expense_authority_generations%rowtype;v_existing private.bpay_next_source_current_observations%rowtype;
  v_revision record;v_scope uuid;v_generation integer;
begin
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then return;end if;
  select * into strict v_revision from private.bpay_next_source_current_revision_v1(p_revision);
  select f.* into strict v_final from public.weekly_source_final_revisions f where f.id=p_revision;
  select u.* into strict v_upload from public.weekly_source_uploads u where u.id=v_final.upload_id;
  select p.* into strict v_publication from public.weekly_source_cycles c
    join public.weekly_source_projection_publications p on p.id=c.current_projection_publication_id
    where c.id=v_final.source_cycle_id and c.current_complete_upload_id=v_upload.id;
  if not v_revision.is_current or v_final.authority_scope_kind<>'CYCLE'
    or v_upload.state<>'CURRENT' or v_publication.state<>'CURRENT'
    or v_upload.source_cycle_id is distinct from v_final.source_cycle_id
    or v_publication.upload_id is distinct from v_upload.id
    or v_publication.source_cycle_id is distinct from v_final.source_cycle_id
    or v_publication.authority_scope_kind is distinct from v_final.authority_scope_kind
    or v_publication.report_scope_id is distinct from v_final.report_scope_id
    or v_final.coverage_start_local_date is distinct from v_upload.confirmed_coverage_start_local_date
    or v_final.coverage_end_local_date is distinct from v_upload.confirmed_coverage_end_local_date
    or not exists(select 1 from public.weekly_source_format_profiles p where p.id=v_upload.source_format_profile_id
      and p.final_authority_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS')) then
    raise exception 'BPAY_NEXT_SOURCE_EXPENSE_OBSERVATION_NOT_CURRENT' using errcode='55000';end if;
  v_generation:=coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer);
  select p.* into strict v_policy from public.weekly_source_row_expense_policy_snapshots p
    join public.weekly_source_row_resolutions r on r.id=p.row_resolution_id
    join public.weekly_source_upload_rows u on u.id=r.upload_row_id
    join public.weekly_work_events w on w.id=p.work_event_id
    where p.id=p_policy and u.upload_id=v_upload.id and p.upload_row_id=u.id
      and r.generation=v_generation and p.generation=r.generation and r.mapping_state='RESOLVED'
      and p.work_event_id=r.work_event_id and p.contract_id=r.contract_id
      and p.candidate_id=r.candidate_id and p.client_id=r.client_id
      and w.candidate_id=p.candidate_id and w.client_id=p.client_id and w.work_date=u.work_date
      and w.first_source_group_id=v_revision.source_group_id
      and exists(select 1 from public.weekly_source_client_manifests m
        where m.final_revision_id=v_final.id and m.client_id=p.client_id)
      and w.work_date between v_final.coverage_start_local_date and v_final.coverage_end_local_date;
  select a.* into strict v_authority from public.weekly_expense_authority_generations a
    where a.work_event_id=v_policy.work_event_id and a.state='CURRENT';
  -- Exact retained finaliser equality. Do not create a new generation merely
  -- because a new row-policy snapshot/restatement has a new immutable ID.
  if v_authority.source_observation_kind<>'ROW_PRESENT' or v_authority.contract_id is distinct from v_policy.contract_id
    or v_authority.source_expense_pence is distinct from v_policy.source_expense_pence
    or v_authority.source_expense_vat_enabled is distinct from v_policy.source_expense_vat_enabled then
    raise exception 'BPAY_NEXT_SOURCE_EXPENSE_OBSERVATION_UNBOUND' using errcode='55000';end if;
  if v_policy.source_expense_pence>0 then
    v_scope:=private.bpay_next_source_current_scope_v1(v_policy.row_resolution_id,v_revision.source_group_id,v_policy.work_event_id,v_policy.contract_id);
  else
    v_scope:=private.bpay_next_source_zero_scope_v1(v_revision.source_group_id,v_policy.work_event_id,v_policy.contract_id);
  end if;
  if v_scope is null then return;end if;
  insert into private.bpay_next_source_current_expenses(scope_id,work_event_id,authority_id,positive,source_group_id,client_id,work_date)
    select v_scope,v_policy.work_event_id,v_authority.id,v_authority.source_expense_pence>0,v_revision.source_group_id,w.client_id,w.work_date
    from public.weekly_work_events w where w.id=v_policy.work_event_id
    on conflict(scope_id,work_event_id) do update set authority_id=excluded.authority_id,positive=excluded.positive;
  select o.* into v_existing from private.bpay_next_source_current_observations o where o.scope_id=v_scope and o.final_revision_id=p_revision;
  if found then
    if v_existing.order_key is distinct from v_revision.order_key or v_existing.manifest_hash is distinct from v_final.manifest_hash
      or not v_existing.active or v_existing.retracted_by_session_id is not null then
      raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_CHANGED' using errcode='55000';end if;
    return;
  end if;
  insert into private.bpay_next_source_current_observations(scope_id,final_revision_id,order_key,manifest_hash,active)
    values(v_scope,p_revision,v_revision.order_key,v_final.manifest_hash,true);
end $f$;

create or replace function private.bpay_next_source_current_publish_revision_v1(p_revision uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_authority record;v_row record;v_after uuid;v_count integer;v_scope uuid;v_capture jsonb;
  v_upload uuid;v_generation integer;v_after_row integer;
begin
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then return;end if;
  select * into strict v_authority from private.bpay_next_source_current_revision_v1(p_revision);
  if not v_authority.is_current then raise exception 'BPAY_NEXT_SOURCE_PUBLISH_NOT_CURRENT' using errcode='55000';end if;
  -- Each loop reads <=100 NEW payload IDs through its exact emission index.
  -- It does not drain an asynchronous job or rebuild a Candidate/history.
  loop
    v_count:=0;
    for v_row in select m.id from public.weekly_source_billing_movements m where m.final_revision_id=p_revision
      and m.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
      and m.source_line_kind in ('NHSP_PHYSICAL_POSITIVE','NHSP_PHYSICAL_FULL_NEGATIVE')
      and m.id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or m.id<>v_after)
      order by m.id limit 100 loop
      perform private.bpay_next_source_current_capture_movement_v1(v_row.id);
      select o.scope_id into strict v_scope from private.bpay_next_source_current_origins o where o.origin_kind='MOVEMENT' and o.origin_id=v_row.id;
      perform private.bpay_next_source_current_observe_v1(v_scope,p_revision,null);
      v_after:=v_row.id;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
  v_after:=null;
  loop
    v_count:=0;
    for v_row in select t.id from public.weekly_source_state_transitions t where t.final_revision_id=p_revision
      and t.id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or t.id<>v_after)
      order by t.id limit 100 loop
      perform private.bpay_next_source_current_capture_transition_v1(v_row.id);
      select o.scope_id into strict v_scope from private.bpay_next_source_current_origins o where o.origin_kind='TRANSITION' and o.origin_id=v_row.id;
      perform private.bpay_next_source_current_observe_v1(v_scope,p_revision,null);
      v_after:=v_row.id;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
  v_after:=null;
  loop
    v_count:=0;
    for v_row in select a.id from public.weekly_expense_authority_generations a where a.final_revision_id=p_revision and a.state='CURRENT'
      and a.id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or a.id<>v_after)
      order by a.id limit 100 loop
      v_capture:=private.bpay_next_source_current_capture_expense_v1(v_row.id);
      if (v_capture->>'root_bound')::boolean then
        select o.scope_id into strict v_scope from private.bpay_next_source_current_origins o where o.origin_kind='EXPENSE' and o.origin_id=v_row.id;
        perform private.bpay_next_source_current_observe_v1(v_scope,p_revision,null);
      end if;
      v_after:=v_row.id;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
  -- Current upload payload only. The old authority may legitimately remain
  -- CURRENT with its ORIGINAL revision when expense money is unchanged.
  select r.upload_id,coalesce(p.projection_generation,p.authority_scope_version::integer)
    into strict v_upload,v_generation from public.weekly_source_final_revisions r
    join public.weekly_source_cycles c on c.id=r.source_cycle_id
    left join public.weekly_source_report_scopes s on s.id=r.report_scope_id
    join public.weekly_source_projection_publications p on p.id=case when r.authority_scope_kind='CYCLE'
      then c.current_projection_publication_id else s.current_projection_publication_id end
    where r.id=p_revision and p.upload_id=r.upload_id and p.state='CURRENT';
  loop
    v_count:=0;
    for v_row in select u.source_row_ordinal,p.id as policy_id
      from public.weekly_source_upload_rows u
      join public.weekly_source_row_resolutions r on r.upload_row_id=u.id and r.generation=v_generation
      join public.weekly_source_row_expense_policy_snapshots p on p.row_resolution_id=r.id
      where u.upload_id=v_upload and (v_after_row is null or u.source_row_ordinal>v_after_row)
      order by u.source_row_ordinal limit 100 loop
      perform private.bpay_next_source_current_observe_expense_row_v1(p_revision,v_row.policy_id);
      v_after_row:=v_row.source_row_ordinal;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
end $f$;

create or replace function private.bpay_next_source_current_apply_correction_v1(p_session uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public as $f$
declare v_s public.weekly_final_source_correction_sessions%rowtype;v_old public.weekly_source_final_revisions%rowtype;
  v_new public.weekly_source_final_revisions%rowtype;v_authority record;v_row record;v_after uuid;v_count integer;v_kind text;
begin
  perform 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if not found then return;end if;
  select s.* into strict v_s from public.weekly_final_source_correction_sessions s where s.id=p_session;
  select r.* into strict v_old from public.weekly_source_final_revisions r where r.id=v_s.expected_current_final_revision_id;
  select r.* into strict v_new from public.weekly_source_final_revisions r where r.id=v_s.prepared_final_revision_id;
  if v_s.state<>'COMMITTING' or v_old.state<>'SUPERSEDED' or v_new.state<>'CURRENT'
    or v_old.manifest_hash is distinct from v_s.expected_final_manifest_hash
    or v_new.predecessor_revision_id is distinct from v_old.id
    or v_old.source_cycle_id is distinct from v_s.source_cycle_id or v_new.source_cycle_id is distinct from v_s.source_cycle_id
    or v_old.authority_scope_kind is distinct from v_s.authority_scope_kind or v_new.authority_scope_kind is distinct from v_s.authority_scope_kind
    or v_old.report_scope_id is distinct from v_s.report_scope_id or v_new.report_scope_id is distinct from v_s.report_scope_id then
    raise exception 'BPAY_NEXT_SOURCE_CORRECTION_HOOK_UNBOUND' using errcode='55000';end if;
  select * into strict v_authority from private.bpay_next_source_current_revision_v1(v_new.id);
  if not v_authority.is_current then raise exception 'BPAY_NEXT_SOURCE_CORRECTION_HOOK_NOT_CURRENT' using errcode='55000';end if;
  foreach v_kind in array array['MOVEMENT','TRANSITION','EXPENSE'] loop
    v_after:=null;
    loop
      v_count:=0;
      for v_row in select o.origin_id from private.bpay_next_source_current_origins o where o.final_revision_id=v_old.id
        and o.origin_kind=v_kind and o.active
        and o.origin_id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or o.origin_id<>v_after)
        order by o.origin_id limit 100 loop
        perform private.bpay_next_source_current_retract_origin_v1(v_kind,v_row.origin_id,p_session);
        v_after:=v_row.origin_id;v_count:=v_count+1;
      end loop;
      exit when v_count<100;
    end loop;
  end loop;
  -- Carry genuine old-root membership even when new revision has ZERO origins.
  v_after:=null;
  loop
    v_count:=0;
    for v_row in select o.scope_id from private.bpay_next_source_current_observations o where o.final_revision_id=v_old.id and o.active
      and o.scope_id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or o.scope_id<>v_after)
      order by o.scope_id limit 100 loop
      perform 1 from private.bpay_next_source_current_scopes s where s.id=v_row.scope_id for update;
      update private.bpay_next_source_current_observations o set active=false,retracted_by_session_id=p_session
        where o.scope_id=v_row.scope_id and o.final_revision_id=v_old.id and o.active and o.manifest_hash=v_old.manifest_hash;
      if not found then raise exception 'BPAY_NEXT_SOURCE_OBSERVATION_CAS_LOST' using errcode='40001';end if;
      perform private.bpay_next_source_current_observe_v1(v_row.scope_id,v_new.id,p_session);
      v_after:=v_row.scope_id;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
  perform private.bpay_next_source_current_publish_revision_v1(v_new.id);
  -- Existing CorrectFinal may restore the exact predecessor authority rather
  -- than emit a replacement generation. Inspect only THIS old revision and
  -- that explicit prior FK; never rank expense generation history.
  v_after:=null;
  loop
    v_count:=0;
    for v_row in select a.id,a.prior_expense_authority_generation_id from public.weekly_expense_authority_generations a
      where a.final_revision_id=v_old.id
        and a.id>=coalesce(v_after,'00000000-0000-0000-0000-000000000000'::uuid) and (v_after is null or a.id<>v_after)
      order by a.id limit 100 loop
      if v_row.prior_expense_authority_generation_id is not null and exists(select 1 from public.weekly_expense_authority_generations a
        where a.id=v_row.prior_expense_authority_generation_id and a.state='CURRENT') then
        perform private.bpay_next_source_current_capture_expense_v1(v_row.prior_expense_authority_generation_id);
      end if;
      v_after:=v_row.id;v_count:=v_count+1;
    end loop;
    exit when v_count<100;
  end loop;
end $f$;

do $acl$
declare v_signature text;
begin
  foreach v_signature in array array[
    'private.bpay_next_source_current_inventory_v1(uuid,integer,uuid,uuid,uuid,date,date)',
    'private.bpay_next_source_current_lock_recheck_v1(uuid)',
    'private.bpay_next_source_current_lock_release_v1(uuid)',
    'private.bpay_next_source_current_lock_inventory_v1(uuid,integer,uuid,uuid,uuid,date,date)',
    'private.bpay_next_source_current_observe_v1(uuid,uuid,uuid)',
    'private.bpay_next_source_current_observation_v1(uuid)',
    'private.bpay_next_source_current_observe_expense_row_v1(uuid,uuid)',
    'private.bpay_next_source_current_publish_revision_v1(uuid)',
    'private.bpay_next_source_current_apply_correction_v1(uuid)'
  ] loop
    execute format('alter function %s owner to %I',v_signature,current_user);
    execute format('revoke all on function %s from public,anon,authenticated,service_role',v_signature);
  end loop;
end $acl$;
commit;
