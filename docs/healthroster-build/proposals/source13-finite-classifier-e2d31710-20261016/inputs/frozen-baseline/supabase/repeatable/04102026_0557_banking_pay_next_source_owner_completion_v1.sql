-- A8 Source transaction-local cleanup timing and first-authorise tail repair.
-- Install AFTER all earlier Source / Banking Pay repeatables (including1709,
--0202,0505,0555,0556):0557 is the final authority for these TWO identities.
-- No private first-authorise core, protected withdrawal, token producer,
-- inventory assertion, trigger, schema, grants or financial policy is replaced.
-- Complete accepted0202 public body and0505 inventory are retained below;
-- only the marked additions differ. CREATE OR REPLACE preserves existing ACL.
\set ON_ERROR_STOP on
begin;

do $installed$
begin
  if to_regprocedure('private.bpay_next_source_current_lock_inventory_v1(uuid,integer,uuid,uuid,uuid,date,date)') is null
     or to_regprocedure('public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)') is null
     or to_regprocedure('private.bpay_source_authorise_identity_v1(uuid)') is null
     or to_regprocedure('private.bpay_source_authorise_lock_v1(uuid,uuid,uuid)') is null
     or to_regprocedure('private.bpay_next_stage_source_current_v1(uuid,uuid,uuid)') is null
     or to_regprocedure('private.bpay_next_publish_source_staged_pair_v1(uuid[],uuid[],uuid[])') is null
     or not exists(select 1 from pg_catalog.pg_trigger t
       where t.tgrelid='private.bpay_next_source_current_lock_operations'::regclass
         and t.tgname='bp_next_source_lock_operation_cleanup'
         and t.tgfoid='private.bpay_next_source_lock_operation_cleanup_assert_v1()'::regprocedure
         and t.tgdeferrable and t.tginitdeferred and t.tgenabled='O'
         and not t.tgisinternal) then
    raise exception 'BPAY_NEXT_SOURCE_BOUNDARY_DEPENDENCY_MISSING' using errcode='55000';
  end if;
end $installed$;

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
  -- BEGIN A8 SOURCE OPERATION CLEANUP TIMING
  -- This row is transaction-local and the genuine outer Source owner removes
  -- it before returning. An IMMEDIATE caller cannot require that removal at
  -- this inner INSERT. Defer ONLY its own cleanup assertion; an orphan still
  -- fails at COMMIT or an explicit named IMMEDIATE check.
  set constraints private.bp_next_source_lock_operation_cleanup deferred;
  -- END A8 SOURCE OPERATION CLEANUP TIMING
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

CREATE OR REPLACE FUNCTION public.weekly_source_first_authorise_v1(p_timesheet_id uuid, p_expected_timesheet_id uuid, p_expected_row_signature text, p_actor_user_id uuid)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public', 'private', 'extensions', 'pg_catalog', 'pg_temp'
AS $function$
declare
  v_context jsonb;
  v_lock jsonb;
  v_core jsonb;
  v_canonical uuid;
  -- BEGIN A8 SOURCE FIRST AUTHORISE DECLARATIONS
  v_work uuid;
  v_revision uuid;
  v_event uuid;
  -- END A8 SOURCE FIRST AUTHORISE DECLARATIONS
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  -- One plain read before the gate, because the gate is pinned to a Candidate.
  -- Nothing is written by it and the identity is re-resolved under the locks.
  v_context:=private.bpay_source_authorise_identity_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    return v_context;
  end if;

  -- BEGIN QUERY V2 FIRST AUTHORISE ADMISSION
  -- Admit before the first Source/Candidate lock; contention remains exact
  -- 55P03, not a financial REVIEW or an automatic owner retry.
  perform private.weekly_source_pay_query_admit_v2();
  -- END QUERY V2 FIRST AUTHORISE ADMISSION

  v_lock:=private.bpay_source_authorise_lock_v1(
    (v_context->>'candidate_id')::uuid,p_timesheet_id,pg_catalog.gen_random_uuid());
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock;
  end if;

  select (family_element.value->>'canonical_timesheet_id')::uuid
    into v_canonical
  from pg_catalog.jsonb_array_elements(coalesce(v_lock->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_canonical then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical);
  end if;

  v_core:=private.weekly_source_first_authorise_core_v1(
    p_timesheet_id,p_expected_row_signature,p_actor_user_id,v_lock);
  -- BEGIN A8 SOURCE FIRST AUTHORISE NEXT TAIL
  -- Capture only after the real outer first-authorise owner succeeds. The
  -- unchanged private core also runs inside two-root head publication and must
  -- never publish an intermediate B revision.
  if coalesce((v_core->>'ok')::boolean,false) is true
     and (select active_owner from private.bpay_next_module_control
          where id=1)='NEXT' then
    v_event:=(v_core->>'root_authorisation_id')::uuid;
    select s.work_id,s.revision_id into strict v_work,v_revision
      from private.bpay_next_stage_source_current_v1(
        p_timesheet_id,v_event,null) s;
    perform * from private.bpay_next_publish_source_staged_pair_v1(
      array[v_work],array[v_revision],array[v_event]);
  end if;
  -- END A8 SOURCE FIRST AUTHORISE NEXT TAIL
  return v_core||pg_catalog.jsonb_build_object('gate',v_lock->>'gate');
end;
$function$;

notify pgrst, 'reload schema';
commit;

