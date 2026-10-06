-- Source owns its factual authorisation and complete head. This owner-only
-- handoff accepts at most the two exact works in one Source publication, after
-- Source has finished all its own writes. No Source/history discovery occurs
-- after the first agency command-clock receipt.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_source_command_id_v1(
  p_source_event_id uuid
) returns uuid
language sql immutable strict
set search_path = pg_catalog
as $function$
  select (
    pg_catalog.substr(h,1,8)||'-'||pg_catalog.substr(h,9,4)||'-'||
    pg_catalog.substr(h,13,4)||'-'||pg_catalog.substr(h,17,4)||'-'||
    pg_catalog.substr(h,21,12))::uuid
  from (select pg_catalog.md5('BPAY-NEXT-SOURCE-V1:'||p_source_event_id::text) h) x
$function$;

create or replace function private.bpay_next_publish_source_staged_pair_v1(
  p_work_ids uuid[], p_revision_ids uuid[], p_source_event_ids uuid[]
) returns table(work_id uuid,revision_id uuid,command_id uuid,agency_sequence bigint)
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_count integer;
  v_candidate uuid;
  v_seen integer;
  v_existing integer;
  v_row record;
  v_command_id uuid;
  v_existing_work uuid;
  v_existing_revision uuid;
  v_existing_sequence bigint;
begin
  v_count:=coalesce(pg_catalog.array_length(p_work_ids,1),0);
  if v_count not between 1 and 2
     or v_count<>coalesce(pg_catalog.array_length(p_revision_ids,1),0)
     or v_count<>coalesce(pg_catalog.array_length(p_source_event_ids,1),0)
     or pg_catalog.array_position(p_work_ids,null) is not null
     or pg_catalog.array_position(p_revision_ids,null) is not null
     or pg_catalog.array_position(p_source_event_ids,null) is not null
     or (select pg_catalog.count(distinct x.id) from pg_catalog.unnest(p_work_ids) x(id))<>v_count
     or (select pg_catalog.count(distinct x.id) from pg_catalog.unnest(p_source_event_ids) x(id))<>v_count then
    raise exception using errcode='22023', message='BPAY_NEXT_SOURCE_PAIR_INPUT_INVALID';
  end if;
  perform 1 from private.bpay_next_module_control
    where id=1 and active_owner='NEXT' for share;
  if not found then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- FK Candidate protection and worker-control ordering precede every work
  -- lock. The position worker takes worker-control before its work row too.
  select pg_catalog.min(w.candidate_id::text)::uuid,
         pg_catalog.count(distinct w.candidate_id)::integer,
         pg_catalog.count(*)::integer
    into v_candidate,v_seen,v_existing
    from private.bpay_next_work w where w.id=any(p_work_ids);
  if v_existing<>v_count or v_seen<>1 then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_PAIR_WORK_MISMATCH';
  end if;
  perform 1 from public.candidates where id=v_candidate for key share;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate for update;

  -- All potentially contended old/new work and revision rows are acquired
  -- before the clock. There can be no work lock taken only for the second
  -- root after the first command has been received.
  for v_row in
    select w.id,w.current_revision_id,w.applied_revision_id
      from private.bpay_next_work w
      where w.id=any(p_work_ids) order by w.id for update
  loop
    null;
  end loop;
  for v_row in
    select r.id from private.bpay_next_work_revision r
    where r.id=any(p_revision_ids)
       or r.id in (select w.current_revision_id from private.bpay_next_work w
                    where w.id=any(p_work_ids))
       or r.id in (select w.applied_revision_id from private.bpay_next_work w
                    where w.id=any(p_work_ids))
    order by r.id for share
  loop
    null;
  end loop;
  for v_row in
    select t.timesheet_id from public.timesheets t
      join private.bpay_next_work_revision r
        on r.physical_timesheet_id=t.timesheet_id
      where r.id=any(p_revision_ids)
      order by t.timesheet_id for key share of t
  loop
    null;
  end loop;

  -- Exact replay is all-or-none. A partial pair would mean an earlier Source
  -- operation was not atomic, so never silently fill in the missing member.
  v_existing:=0;
  for v_row in
    select input.work_id,input.revision_id,input.source_event_id,
           r.source_kind,r.source_event_id as stored_event,r.sealed_at_utc,
           r.work_id as stored_work
      from unnest(p_work_ids,p_revision_ids,p_source_event_ids)
        as input(work_id,revision_id,source_event_id)
      left join private.bpay_next_work_revision r on r.id=input.revision_id
      order by input.work_id
  loop
    if v_row.stored_work is distinct from v_row.work_id
       or v_row.stored_event is distinct from v_row.source_event_id
       or v_row.source_kind not in ('SOURCE','PROTECTED') then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_PAIR_REVISION_MISMATCH';
    end if;
    v_command_id:=private.bpay_next_source_command_id_v1(v_row.source_event_id);
    select p.work_id,p.revision_id,c.agency_sequence
      into v_existing_work,v_existing_revision,v_existing_sequence
      from private.bpay_next_publication p
      join private.bpay_next_command c on c.id=p.command_id
      where p.command_id=v_command_id;
    if found then
      if (v_existing_work,v_existing_revision) is distinct from
         (v_row.work_id,v_row.revision_id) then
        raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_PAIR_REPLAY_CONFLICT';
      end if;
      v_existing:=v_existing+1;
    elsif v_row.sealed_at_utc is not null then
      raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_PAIR_ALREADY_SEALED';
    end if;
  end loop;
  if v_existing not in (0,v_count) then
    raise exception using errcode='23514', message='BPAY_NEXT_SOURCE_PAIR_PARTIAL_REPLAY';
  end if;

  for v_row in
    select input.work_id,input.revision_id,input.source_event_id
      from unnest(p_work_ids,p_revision_ids,p_source_event_ids)
        as input(work_id,revision_id,source_event_id)
      order by input.work_id
  loop
    work_id:=v_row.work_id;
    revision_id:=v_row.revision_id;
    command_id:=private.bpay_next_source_command_id_v1(v_row.source_event_id);
    agency_sequence:=private.bpay_next_publish_staged_revision_v1(
      work_id,revision_id,command_id);
    return next;
  end loop;
end
$function$;

alter function private.bpay_next_source_command_id_v1(uuid) owner to postgres;
alter function private.bpay_next_publish_source_staged_pair_v1(uuid[],uuid[],uuid[]) owner to postgres;
revoke all on function
  private.bpay_next_source_command_id_v1(uuid),
  private.bpay_next_publish_source_staged_pair_v1(uuid[],uuid[],uuid[])
  from public, anon, authenticated, service_role;

commit;
