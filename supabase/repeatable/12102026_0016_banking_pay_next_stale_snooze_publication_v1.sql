-- WB36/WB44 A2 / BD128: native bounded publication cancellation.
\set ON_ERROR_STOP on
begin;

-- Native immutable basis lookup: NULL means positively absent stable membership.
create or replace function private.bpay_next_snooze_revision_basis_v1(p_revision_id uuid,p_scope text,p_key text)
returns bytea language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_basis private.bpay_next_revision_worker_basis%rowtype;v_id uuid;v_result bytea;
begin
  select b.* into v_basis from private.bpay_next_revision_worker_basis b
    join private.bpay_next_work_revision r on r.id=b.revision_id and r.sealed_at_utc is not null
    where b.revision_id=p_revision_id;
  if not found or octet_length(v_basis.whole_basis) is distinct from 32
    or octet_length(v_basis.expense_basis) is distinct from 32 then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_BASIS_MISSING';end if;
  if p_scope='WHOLE_TIMESHEET' then return v_basis.whole_basis;
  elsif p_scope='EXPENSES_ALL' then return v_basis.expense_basis;
  elsif p_scope='SEGMENT' and p_key is not null then
    select s.id into v_id from private.bpay_next_approved_segment s
      where s.revision_id=p_revision_id and s.segment_key=p_key;
    if not found then return null;end if;
    v_result:=private.bpay_next_segment_pay_basis_v1(v_id);
  elsif p_scope='EXPENSE_LINE' and p_key is not null then
    select a.id into v_id from private.bpay_next_approved_line a
      where a.revision_id=p_revision_id and a.component_key=p_key and a.component_kind in ('EXPENSE','MILEAGE');
    if not found then return null;end if;
    v_result:=private.bpay_next_line_pay_basis_v1(v_id);
  else raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_SCOPE_INVALID';end if;
  if octet_length(v_result) is distinct from 32 then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REVISION_BASIS_MISSING';end if;
  return v_result;
end
$function$;

-- Exact cancellation point only; immediate checks the lease, deferred its checkpoint.
create or replace function private.bpay_next_snooze_publication_cancel_valid_v1(
  p_before jsonb,p_after jsonb,p_checkpoint boolean,p_occurred_at timestamptz)
returns boolean language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare c private.bpay_next_command%rowtype;p private.bpay_next_publication%rowtype;
  j private.bpay_next_job%rowtype;w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
  v_id uuid;v_basis bytea;v_epoch bigint;
begin
  if p_before is null or p_after is null or p_checkpoint is null or p_occurred_at is null
    or coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or (p_before->>'active')::boolean is distinct from true or (p_after->>'active')::boolean is distinct from false
    or p_before->>'cleared_reason' is not null or p_after->>'cleared_reason' is not null
    or (p_after->>'version')::bigint is distinct from (p_before->>'version')::bigint+1
    or (p_after->>'updated_at_utc')::timestamptz is distinct from p_occurred_at
    or (p_after-array['active','version','control_sequence','updated_at_utc','last_request_id'])
      is distinct from (p_before-array['active','version','control_sequence','updated_at_utc','last_request_id']) then return false;end if;
  v_id:=(p_after->>'id')::uuid;
  select x.* into c from private.bpay_next_command x where x.id=(p_after->>'last_request_id')::uuid;
  if not found or c.command_kind<>'POSITION_APPLY' or c.expected_member_count is distinct from 1
    or c.enrolled_member_count<>1 or c.sealed_at_utc is null
    or (p_after->>'control_sequence')::bigint is distinct from c.agency_sequence
    or coalesce((p_before->>'control_sequence')::bigint<c.agency_sequence,false) is not true
    or not exists(select 1 from private.bpay_next_module_control m where m.id=1
      and m.active_owner='NEXT' and m.owner_epoch=c.module_epoch)
    or not exists(select 1 from private.bpay_next_command_member m where m.command_id=c.id
      and m.candidate_id=(p_after->>'candidate_id')::uuid and m.member_no=1) then return false;end if;
  select x.* into p from private.bpay_next_publication x where x.command_id=c.id;
  if not found or p.work_id is distinct from (p_after->>'work_id')::uuid
    or p.candidate_id is distinct from (p_after->>'candidate_id')::uuid then return false;end if;
  select x.* into j from private.bpay_next_job x where x.command_id=c.id
    and x.candidate_id=p.candidate_id and x.job_kind='POSITION_APPLY';
  if not found or j.command_sequence<>c.agency_sequence or j.module_epoch<>c.module_epoch then return false;end if;
  select x.active_owner_epoch into v_epoch from private.bpay_next_worker_control x where x.candidate_id=p.candidate_id;
  if j.owner_epoch is distinct from v_epoch then return false;end if;
  select x.* into w from private.bpay_next_work x where x.id=p.work_id;
  select x.* into r from private.bpay_next_work_revision x where x.id=p.revision_id and x.work_id=p.work_id;
  if w.candidate_id is distinct from p.candidate_id or r.id is null or r.sealed_at_utc is null
    or r.revision_no<>p.revision_no or j.applied_line_count<>r.expected_line_count then return false;end if;
  v_basis:=private.bpay_next_snooze_revision_basis_v1(p.revision_id,p_after->>'scope_kind',
    case when p_after->>'scope_kind'='SEGMENT' then p_after->>'segment_key' else p_after->>'component_key' end);
  if v_basis is not distinct from (p_before->>'worker_pay_basis')::bytea then return false;end if;
  if not p_checkpoint then
    return coalesce(c.status='SEALED' and p.status='APPLYING' and p.phase='SNOOZE'
      and j.status='LEASED' and j.phase='SNOOZE' and j.lease_nonce is not null
      and j.lease_until_utc>p_occurred_at and p.cursor_key is not distinct from j.cursor_key
      and (p.cursor_key is null or (p.cursor_key~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' and p.cursor_key::uuid<v_id))
      and w.applied_revision_id is not distinct from p.predecessor_revision_id,false);
  elsif p.status='APPLIED' then
    return coalesce(c.status='COMPLETE' and p.phase='DONE' and p.cursor_key is null and p.applied_at_utc is not null
      and j.status='DONE' and j.phase='DONE' and j.cursor_key is null and j.lease_nonce is null and j.lease_until_utc is null
      and w.applied_revision_id=p.revision_id,false);
  end if;
  return coalesce(c.status='SEALED' and p.status='APPLYING' and p.phase='SNOOZE' and j.phase='SNOOZE'
    and ((j.status='READY' and j.lease_nonce is null and j.lease_until_utc is null)
      or (j.status='LEASED' and j.lease_nonce is not null and j.lease_until_utc>p_occurred_at))
    and p.cursor_key is not distinct from j.cursor_key
    and p.cursor_key~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' and p.cursor_key::uuid>=v_id
    and exists(select 1 from private.bpay_next_snooze s where s.work_id=p.work_id and s.id=p.cursor_key::uuid)
    and w.applied_revision_id is not distinct from p.predecessor_revision_id,false);
end
$function$;

create or replace function private.bpay_next_apply_position_page_v1(
  p_job_id uuid, p_lease_nonce uuid, p_owner_epoch bigint,
  p_expected_phase text, p_expected_cursor text, p_limit integer default 100
) returns table(next_phase text,next_cursor text,rows_visited integer,done boolean)
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_candidate uuid;
  v_job private.bpay_next_job%rowtype;
  v_publication private.bpay_next_publication%rowtype;
  v_work private.bpay_next_work%rowtype;
  v_source_pay_channel text;
  v_epoch bigint;
  v_row record;
  v_seen integer:=0;
  v_cursor text;
  v_snooze private.bpay_next_snooze%rowtype;v_before jsonb;v_after jsonb;v_basis bytea;
  v_sequence bigint;v_changed integer:=0;v_now timestamptz;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023', message='BPAY_NEXT_POSITION_STEP_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate from private.bpay_next_job
    where id=p_job_id;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  -- A lost final page reply is an exact terminal readback, not a second
  -- position write. A later approval may already have changed the WORK;
  -- retain this publication's own completed identity rather than inspecting
  -- or rebuilding the latest revision. No lease authority is adopted.
  if v_job.job_kind='POSITION_APPLY' and v_job.module_epoch=v_epoch
     and v_job.status='DONE' and v_job.phase='DONE'
     and exists(select 1 from private.bpay_next_publication p
       where p.command_id=v_job.command_id and p.candidate_id=v_candidate
         and p.status='APPLIED' and p.phase='DONE') then
    next_phase:='DONE';next_cursor:=null;rows_visited:=0;done:=true;
    return next;return;
  end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_job.module_epoch<>v_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() then
    raise exception using errcode='55000', message='BPAY_NEXT_POSITION_LEASE_STALE';
  end if;
  if v_job.phase is distinct from p_expected_phase
     or v_job.cursor_key is distinct from p_expected_cursor then
    -- A lost response is a readback, never a repeated financial write.
    next_phase:=v_job.phase; next_cursor:=v_job.cursor_key;
    rows_visited:=0; done:=false; return next;
    return;
  end if;
  select * into strict v_publication from private.bpay_next_publication
    where command_id=v_job.command_id for update;
  select * into strict v_work from private.bpay_next_work
    where id=v_publication.work_id for update;
  if v_publication.candidate_id<>v_candidate
     or v_publication.status='APPLIED'
     or v_work.applied_revision_id is distinct from v_publication.predecessor_revision_id then
    raise exception using errcode='55000', message='BPAY_NEXT_POSITION_PREDECESSOR_MISMATCH';
  end if;
  select r.source_pay_channel into strict v_source_pay_channel
    from private.bpay_next_work_revision r
    where r.id=v_publication.revision_id and r.work_id=v_work.id;
  v_cursor:=v_job.cursor_key;
  if v_job.phase='NEW' then
    for v_row in
      select l.component_key,l.source_pay_ex_vat
        from private.bpay_next_approved_line l
        where l.revision_id=v_publication.revision_id
          and (v_cursor is null or l.component_key>v_cursor)
        order by l.component_key limit p_limit
    loop
      insert into private.bpay_next_position
        (work_id,component_key,applied_revision_id,
         source_basis_channel,approved_source_ex_vat)
        values (v_work.id,v_row.component_key,v_publication.revision_id,
                v_source_pay_channel,v_row.source_pay_ex_vat)
      on conflict (work_id,component_key) do update
        set applied_revision_id=excluded.applied_revision_id,
            source_basis_channel=case
              when private.bpay_next_position.realised_source_ex_vat<>0
                or private.bpay_next_position.held_source_ex_vat<>0
                or private.bpay_next_position.realised_target_ex_vat<>0
                or private.bpay_next_position.realised_target_vat<>0
                or private.bpay_next_position.realised_target_inc_vat<>0
                or private.bpay_next_position.held_target_ex_vat<>0
                or private.bpay_next_position.held_target_vat<>0
                or private.bpay_next_position.held_target_inc_vat<>0
                then private.bpay_next_position.source_basis_channel
              else excluded.source_basis_channel end,
            approved_source_ex_vat=excluded.approved_source_ex_vat,
            updated_at_utc=pg_catalog.transaction_timestamp();
      perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_work.id,v_row.component_key);
      v_cursor:=v_row.component_key;
      v_seen:=v_seen+1;
    end loop;
    if v_seen<p_limit then
      if v_job.applied_line_count+v_seen <>
         (select r.expected_line_count
            from private.bpay_next_work_revision r
            where r.id=v_publication.revision_id) then
        raise exception using errcode='23514',
          message='BPAY_NEXT_POSITION_APPROVED_LINE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_publication
        set status='APPLYING',phase='REMOVED',cursor_key=null
        where command_id=v_publication.command_id;
      update private.bpay_next_job
        set phase='REMOVED',cursor_key=null,
            applied_line_count=applied_line_count+v_seen
        where id=p_job_id;
      next_phase:='REMOVED'; next_cursor:=null;
    else
      update private.bpay_next_publication
        set status='APPLYING',cursor_key=v_cursor
        where command_id=v_publication.command_id;
      update private.bpay_next_job
        set cursor_key=v_cursor,
            applied_line_count=applied_line_count+v_seen
        where id=p_job_id;
      next_phase:='NEW'; next_cursor:=v_cursor;
    end if;
  elsif v_job.phase='REMOVED' then
    for v_row in
      select p.component_key from private.bpay_next_position p
        where p.work_id=v_work.id
          and (p.approved_source_ex_vat<>0
            or p.realised_source_ex_vat<>0 or p.held_source_ex_vat<>0
            or p.realised_target_ex_vat<>0 or p.realised_target_vat<>0
            or p.realised_target_inc_vat<>0
            or p.held_target_ex_vat<>0 or p.held_target_vat<>0
            or p.held_target_inc_vat<>0)
          and (v_cursor is null or p.component_key>v_cursor)
        order by p.component_key limit p_limit
    loop
      if not exists (
        select 1 from private.bpay_next_approved_line l
          where l.revision_id=v_publication.revision_id
            and l.component_key=v_row.component_key
      ) then
        update private.bpay_next_position
          set applied_revision_id=v_publication.revision_id,
              approved_source_ex_vat=0,
              updated_at_utc=pg_catalog.transaction_timestamp()
          where work_id=v_work.id and component_key=v_row.component_key;
        perform private.bpay_next_reconcile_work_collection_v1(p_job_id,v_work.id,v_row.component_key);
      end if;
      v_cursor:=v_row.component_key;
      v_seen:=v_seen+1;
    end loop;
    if v_seen<p_limit then
      update private.bpay_next_publication set status='APPLYING',phase='SNOOZE',cursor_key=null
        where command_id=v_publication.command_id;
      update private.bpay_next_job set phase='SNOOZE',cursor_key=null where id=p_job_id;
      next_phase:='SNOOZE';next_cursor:=null;
    else
      update private.bpay_next_publication
        set status='APPLYING',cursor_key=v_cursor
        where command_id=v_publication.command_id;
      update private.bpay_next_job set cursor_key=v_cursor where id=p_job_id;
      next_phase:='REMOVED'; next_cursor:=v_cursor;
    end if;
  elsif v_job.phase='SNOOZE' then
    if v_publication.status<>'APPLYING' or v_publication.phase<>'SNOOZE'
      or v_publication.cursor_key is distinct from v_cursor
      or (v_cursor is not null and v_cursor!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
      raise exception using errcode='23514',message='BPAY_NEXT_POSITION_SNOOZE_CHECKPOINT_INVALID';end if;
    perform private.bpay_next_snooze_revision_basis_v1(v_publication.revision_id,'WHOLE_TIMESHEET',null);
    select c.agency_sequence into strict v_sequence from private.bpay_next_command c
      where c.id=v_publication.command_id and c.command_kind='POSITION_APPLY' and c.status='SEALED';
    for v_snooze in select s.* from private.bpay_next_snooze s
      where s.work_id=v_work.id and s.active and (v_cursor is null or s.id>v_cursor::uuid)
      order by s.id limit p_limit for update
    loop
      if v_snooze.control_sequence<v_sequence then
        v_basis:=private.bpay_next_snooze_revision_basis_v1(v_publication.revision_id,v_snooze.scope_kind,
          case when v_snooze.scope_kind='SEGMENT' then v_snooze.segment_key else v_snooze.component_key end);
        if v_basis is distinct from v_snooze.worker_pay_basis then
          v_before:=to_jsonb(v_snooze);v_now:=pg_catalog.clock_timestamp();
          update private.bpay_next_snooze set active=false,version=version+1,control_sequence=v_sequence,
            updated_at_utc=v_now,last_request_id=v_publication.command_id where id=v_snooze.id
            returning to_jsonb(private.bpay_next_snooze) into v_after;
          insert into private.bpay_next_snooze_event(snooze_id,candidate_id,version,control_sequence,event_kind,
            actor_id,reason,request_id,before_state,after_state,occurred_at_utc)
          values(v_snooze.id,v_snooze.candidate_id,(v_after->>'version')::bigint,v_sequence,'CANCELLED',
            null,'WORKER_PAY_CHANGED',v_publication.command_id,v_before,v_after,v_now);
          v_changed:=v_changed+1;
        end if;
      end if;
      v_cursor:=v_snooze.id::text;v_seen:=v_seen+1;
    end loop;
    if v_seen<p_limit then
      update private.bpay_next_publication
        set status='APPLIED',phase='DONE',cursor_key=null,
            applied_at_utc=pg_catalog.transaction_timestamp()
        where command_id=v_publication.command_id;
      update private.bpay_next_work
        set applied_revision_id=v_publication.revision_id,
            updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_work.id;
      update private.bpay_next_job
        set status='DONE',phase='DONE',cursor_key=null,
            lease_nonce=null,lease_until_utc=null
        where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE'
        where id=v_publication.command_id;
      next_phase:='DONE'; next_cursor:=null;
    else
      update private.bpay_next_publication set cursor_key=v_cursor where command_id=v_publication.command_id;
      update private.bpay_next_job set cursor_key=v_cursor where id=p_job_id;
      next_phase:='SNOOZE';next_cursor:=v_cursor;
    end if;
  else
    raise exception using errcode='23514', message='BPAY_NEXT_POSITION_PHASE_INVALID';
  end if;
  if (v_seen>0 and v_job.phase<>'SNOOZE') or v_changed>0 or next_phase='DONE' then
    update private.bpay_next_worker_control
      set financial_view_revision=financial_view_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp()
      where candidate_id=v_candidate;
  end if;
  if next_phase='DONE' and not (v_work.approval_state='APPROVED'
      and v_work.current_revision_id is distinct from v_publication.revision_id) then
    -- Native APPLIED pointer and financial revision are already final.
    -- This bounded owner preholds the exact linked Cases and replaces once.
    perform private.bpay_next_workbench_refresh_many_v1(array[v_work.id],null);
  else
    -- Intermediate pages and an older publication finishing behind a newer
    -- approved current pointer retain the genuine dirty Processing barrier.
    perform private.bpay_next_workbench_mark_processing_v1(v_work.id);
  end if;
  rows_visited:=v_seen;
  done:=(next_phase='DONE');
  return next;
end
$function$;

create or replace function private.bpay_next_snooze_metadata_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_command private.bpay_next_command%rowtype;v_snooze private.bpay_next_snooze%rowtype;
  v_previous private.bpay_next_snooze_event%rowtype;v_context jsonb;v_args jsonb;v_request uuid;v_actor uuid;
  v_ack private.bpay_next_snooze_warning_ack%rowtype;v_due boolean:=false;
begin
  if tg_op in ('DELETE','TRUNCATE') then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;v_actor:=new.updated_by;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    v_request:=case when tg_op='UPDATE' then new.consumed_by_request_id else new.request_id end;v_actor:=new.actor_id;
  else v_request:=new.request_id;v_actor:=new.actor_id;end if;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  if v_command.command_kind='POSITION_APPLY' and tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event') then
    if tg_table_name='bpay_next_snooze' then
      if tg_op<>'UPDATE' or not private.bpay_next_snooze_publication_cancel_valid_v1(
          to_jsonb(old),to_jsonb(new),false,new.updated_at_utc) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_PUBLICATION_CANCEL_INVALID';end if;
    else
      if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
      select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=new.snooze_id;
      select e.* into v_previous from private.bpay_next_snooze_event e where e.snooze_id=new.snooze_id and e.version=new.version-1;
      if new.event_kind<>'CANCELLED' or new.actor_id is not null or new.reason is distinct from 'WORKER_PAY_CHANGED'
        or v_previous.id is null or new.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',
          coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
        or (new.candidate_id,new.version,new.control_sequence,new.request_id,new.after_state)
          is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,v_snooze.last_request_id,to_jsonb(v_snooze))
        or not private.bpay_next_snooze_publication_cancel_valid_v1(new.before_state,new.after_state,false,new.occurred_at_utc) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_PUBLICATION_EVENT_INVALID';end if;
    end if;
    return new;
  end if;
  v_due:=coalesce((tg_table_name='bpay_next_snooze' and tg_op='UPDATE' and not (to_jsonb(new)->>'active')::boolean
      and to_jsonb(new)->>'cleared_reason'='DUE')
    or (tg_table_name='bpay_next_snooze_event' and tg_op='INSERT' and to_jsonb(new)->>'event_kind'='WOKEN'),false);
  if v_due and (coalesce(current_setting('request.jwt.claim.role',true),
      nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or v_command.command_kind<>'SNOOZE_CLEAR') then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_DUE_OWNER_INVALID';
  end if;
  if v_command.status<>'RECEIVED' or not exists(select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' and m.owner_epoch=v_command.module_epoch)
    or (not v_due and not exists(select 1 from public.tms_users u where u.id=v_actor and u.is_active and u.role::text='admin')) then
    raise exception using errcode='42501',message='BPAY_NEXT_SNOOZE_METADATA_OWNER_INVALID';
  end if;
  if tg_table_name='bpay_next_snooze' then
    if (v_due and (not old.active or old.until_date is null
        or old.until_date >= (clock_timestamp() at time zone 'Europe/London')::date
        or new.updated_by is distinct from old.updated_by or old.cleared_reason is not null))
      or (not v_due and new.cleared_reason is not null) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_SCOPE_INVALID';
    end if;
    if new.control_sequence<>v_command.agency_sequence or v_command.command_kind not in ('SNOOZE_SET','SNOOZE_CLEAR')
      or (tg_op='INSERT' and (new.version<>1 or not new.active or new.created_by<>new.updated_by or v_command.command_kind<>'SNOOZE_SET'))
      or (tg_op='UPDATE' and (not old.active or new.version<>old.version+1 or new.control_sequence<=old.control_sequence
        or (new.id,new.candidate_id,new.work_id,new.scope_kind,new.scope_identity,new.segment_key,new.case_id,new.case_component_id,
          new.created_at_utc,new.created_by) is distinct from (old.id,old.candidate_id,old.work_id,old.scope_kind,old.scope_identity,
          old.segment_key,old.case_id,old.case_component_id,old.created_at_utc,old.created_by))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_VERSION_INVALID';
    end if;
    if not new.active then
      if tg_op<>'UPDATE' or v_command.command_kind<>'SNOOZE_CLEAR'
        or (to_jsonb(new)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id','cleared_reason'])
          is distinct from (to_jsonb(old)-array['active','version','control_sequence','updated_at_utc','updated_by','last_request_id','cleared_reason']) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_CLEAR_FINANCIAL_TUPLE_CHANGED';
      end if;
      return new;
    end if;
    if v_command.command_kind<>'SNOOZE_SET' then raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_KIND_INVALID';end if;
    v_args:=jsonb_strip_nulls(jsonb_build_object('candidate_id',new.candidate_id,'work_id',new.work_id,
      'expected_revision_id',new.expected_revision_id,'scope_kind',new.scope_kind,'segment_key',new.segment_key,
      'component_key',case when new.scope_kind='EXPENSE_LINE' then new.component_key end,'case_id',new.case_id,
      'case_component_id',new.case_component_id,'snooze_kind',new.snooze_kind,'until_date',new.until_date,
      'indefinite',new.until_date is null,'note',new.note));
    v_context:=private.bpay_next_snooze_context_v1(new.updated_by,v_args);
    if (new.scope_identity,new.expected_revision_id,new.physical_timesheet_id,new.approved_segment_id,
        new.expense_approved_line_id,new.component_key,new.case_kind,new.worker_pay_basis)
      is distinct from (v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
        (v_context->>'physical_timesheet_id')::uuid,(v_context->>'approved_segment_id')::uuid,
        (v_context->>'expense_approved_line_id')::uuid,v_context->>'component_key',v_context->>'case_kind',
        decode(v_context->>'worker_pay_basis','hex')) or v_context->>'conflict' is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_METADATA_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_event' then
    if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';end if;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=new.snooze_id;
    select e.* into v_previous from private.bpay_next_snooze_event e where e.snooze_id=new.snooze_id and e.version=new.version-1;
    if v_due then
      if new.actor_id is not null or new.reason<>'DUE' or v_snooze.active or v_snooze.cleared_reason<>'DUE'
        or v_previous.id is null or new.version<=1
        or new.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',
          coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
        or (new.candidate_id,new.version,new.control_sequence,new.request_id,new.after_state)
          is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,
            v_snooze.last_request_id,to_jsonb(v_snooze)) or new.control_sequence<>v_command.agency_sequence then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_EVENT_INVALID';
      end if;
      return new;
    end if;
    if (new.candidate_id,new.version,new.control_sequence,new.actor_id,new.request_id,new.after_state)
      is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,v_snooze.updated_by,
        v_snooze.last_request_id,to_jsonb(v_snooze)) or new.control_sequence<>v_command.agency_sequence
      or (new.version=1 and (new.event_kind<>'SET' or new.before_state is not null or not v_snooze.active))
      or (new.version>1 and (v_previous.id is null or new.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
        or new.event_kind is distinct from (case when v_snooze.active then 'AMENDED'::text else 'CLEARED'::text end)))
      or v_command.command_kind is distinct from (case when new.event_kind='CLEARED' then 'SNOOZE_CLEAR'::text else 'SNOOZE_SET'::text end) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_SCOPE_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_command.command_kind<>'SNOOZE_WARNING_ACK_ISSUE' or new.module_epoch<>v_command.module_epoch
        or new.consumed_by_request_id is not null or jsonb_typeof(new.warning_context)<>'object' then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUE_INVALID';
      end if;
    elsif tg_op<>'UPDATE' or old.consumed_by_request_id is not null or new.consumed_by_request_id is null
      or new.consumed_at_utc is null or new.consumed_at_utc>=old.expires_at_utc or new.consumed_at_utc<old.issued_at_utc
      or v_command.command_kind<>'SNOOZE_SET' or v_command.module_epoch<>old.module_epoch
      or (to_jsonb(new)-array['consumed_by_request_id','consumed_at_utc'])
        is distinct from (to_jsonb(old)-array['consumed_by_request_id','consumed_at_utc']) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMPTION_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name='bpay_next_snooze_request' and tg_op='INSERT' then
    if new.action<>v_command.command_kind or (new.request_body->>'request_id')::uuid is distinct from new.request_id
      or jsonb_typeof(new.request_body)<>'object' or octet_length(new.request_body::text)>4096
      or jsonb_typeof(new.result_body)<>'object' then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_COMMAND_INVALID';
    end if;
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      select a.* into strict v_ack from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id;
      v_context:=private.bpay_next_snooze_context_v1(new.actor_id,new.request_body);
      if (v_ack.actor_id,v_ack.candidate_id,v_ack.module_epoch,v_ack.scope_kind,v_ack.scope_identity,
          v_ack.expected_revision_id,v_ack.expected_version,v_ack.worker_pay_basis,v_ack.warning_context)
        is distinct from (new.actor_id,(v_context->>'candidate_id')::uuid,(v_context->>'module_epoch')::bigint,
          v_context->>'scope_kind',v_context->>'scope_identity',(v_context->>'expected_revision_id')::uuid,
          (v_context->>'expected_version')::bigint,decode(v_context->>'worker_pay_basis','hex'),v_context->'warning_context') then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_ISSUED_SCOPE_MISMATCH';
      end if;
    end if;
    return new;
  end if;
  raise exception using errcode='55000',message='BPAY_NEXT_SNOOZE_METADATA_IMMUTABLE';
end
$function$;

create or replace function private.bpay_next_snooze_receipt_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_request uuid;v_receipt private.bpay_next_snooze_request%rowtype;v_command private.bpay_next_command%rowtype;
  v_context jsonb;v_ack private.bpay_next_snooze_warning_ack%rowtype;
  v_event private.bpay_next_snooze_event%rowtype;v_previous private.bpay_next_snooze_event%rowtype;v_snooze private.bpay_next_snooze%rowtype;v_id uuid;
begin
  if tg_table_name='bpay_next_snooze' then v_request:=new.last_request_id;
  elsif tg_table_name='bpay_next_snooze_warning_ack' and tg_op='UPDATE' then v_request:=new.consumed_by_request_id;
  else v_request:=new.request_id;end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event')
    and ((tg_table_name='bpay_next_snooze' and to_jsonb(new)->>'cleared_reason'='DUE')
      or (tg_table_name='bpay_next_snooze_event' and to_jsonb(new)->>'event_kind'='WOKEN')) then
    if not private.bpay_next_snooze_due_receipt_valid_v1(v_request) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_DUE_RECEIPT_INVALID';
    end if;
    return new;
  end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event') and exists(
      select 1 from private.bpay_next_command c where c.id=v_request and c.command_kind='POSITION_APPLY') then
    v_id:=case when tg_table_name='bpay_next_snooze' then (to_jsonb(new)->>'id')::uuid else (to_jsonb(new)->>'snooze_id')::uuid end;
    select s.* into strict v_snooze from private.bpay_next_snooze s where s.id=v_id;
    select e.* into v_event from private.bpay_next_snooze_event e where e.snooze_id=v_id and e.version=new.version;
    select e.* into v_previous from private.bpay_next_snooze_event e where e.snooze_id=v_id and e.version=new.version-1;
    if v_event.id is null or v_previous.id is null or v_event.event_kind<>'CANCELLED'
      or v_event.actor_id is not null or v_event.reason is distinct from 'WORKER_PAY_CHANGED'
      or v_event.request_id is distinct from v_request
      or (v_event.candidate_id,v_event.version,v_event.control_sequence,v_event.request_id,v_event.after_state)
        is distinct from (v_snooze.candidate_id,v_snooze.version,v_snooze.control_sequence,v_snooze.last_request_id,to_jsonb(v_snooze))
      or v_event.before_state is distinct from jsonb_set(v_previous.after_state,'{cleared_reason}',
        coalesce(v_previous.after_state->'cleared_reason','null'::jsonb),true)
      or not private.bpay_next_snooze_publication_cancel_valid_v1(v_event.before_state,v_event.after_state,true,v_event.occurred_at_utc) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_PUBLICATION_RECEIPT_INVALID';end if;
    return new;
  end if;
  select r.* into strict v_receipt from private.bpay_next_snooze_request r where r.request_id=v_request;
  select c.* into strict v_command from private.bpay_next_command c where c.id=v_request;
  if v_command.status<>'COMPLETE' or v_command.expected_member_count is distinct from 0 or v_command.sealed_at_utc is null
    or v_command.command_kind<>v_receipt.action then
    raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_COMMAND_RECEIPT_INCOMPLETE';
  end if;
  if tg_table_name in ('bpay_next_snooze','bpay_next_snooze_event') then
    if not exists(select 1 from private.bpay_next_snooze_event e
      where e.snooze_id=case when tg_table_name='bpay_next_snooze' then (to_jsonb(new)->>'id')::uuid else (to_jsonb(new)->>'snooze_id')::uuid end
        and e.version=new.version and e.request_id=v_request and e.actor_id=v_receipt.actor_id
        and e.candidate_id=(v_receipt.request_body->>'candidate_id')::uuid
        and (v_receipt.result_body->>'snooze_id')::uuid=e.snooze_id
        and (v_receipt.result_body->>'version')::bigint=e.version
        and (v_receipt.result_body->>'control_sequence')::bigint=e.control_sequence
        and (v_receipt.request_body->>'expected_version')::bigint=e.version-1
        and (v_receipt.action<>'SNOOZE_CLEAR' or (v_receipt.request_body->>'snooze_id')::uuid=e.snooze_id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_EVENT_RECEIPT_MISMATCH';
    end if;
  elsif tg_table_name='bpay_next_snooze_warning_ack' then
    if tg_op='INSERT' then
      if v_receipt.action<>'SNOOZE_WARNING_ACK_ISSUE' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.result_body->>'acknowledgement_token')::uuid is distinct from new.token
        or (v_receipt.result_body->>'acknowledgement_expires_at_utc')::timestamptz is distinct from new.expires_at_utc
        or (v_receipt.result_body->>'scope_kind',v_receipt.result_body->>'scope_identity',
          (v_receipt.result_body->>'expected_revision_id')::uuid,(v_receipt.result_body->>'expected_version')::bigint,
          decode(v_receipt.result_body->>'worker_pay_basis','hex'),v_receipt.result_body->'warning_context')
          is distinct from (new.scope_kind,new.scope_identity,new.expected_revision_id,new.expected_version,new.worker_pay_basis,new.warning_context) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISMATCH';
      end if;
    else
      if v_receipt.action<>'SNOOZE_SET' or v_receipt.actor_id<>new.actor_id
        or (v_receipt.request_body->>'candidate_id')::uuid is distinct from new.candidate_id
        or (v_receipt.request_body->>'acknowledgement_token')::uuid is distinct from new.token
        or v_receipt.request_body->>'scope_kind' is distinct from new.scope_kind
        or (v_receipt.request_body->>'expected_revision_id')::uuid is distinct from new.expected_revision_id
        or (v_receipt.request_body->>'expected_version')::bigint is distinct from new.expected_version
        or decode(v_receipt.request_body->>'worker_pay_basis','hex') is distinct from new.worker_pay_basis
        or not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=v_request
          and e.actor_id=new.actor_id and e.after_state->>'scope_identity'=new.scope_identity
          and (e.after_state->>'candidate_id')::uuid=new.candidate_id) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_CONSUMED_RECEIPT_MISMATCH';
      end if;
    end if;
  elsif tg_table_name='bpay_next_snooze_request' then
    if new.action='SNOOZE_WARNING_ACK_ISSUE' then
      if not exists(select 1 from private.bpay_next_snooze_warning_ack a where a.request_id=new.request_id
        and a.actor_id=new.actor_id and a.token=(new.result_body->>'acknowledgement_token')::uuid) then
        raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_ACK_RECEIPT_MISSING';
      end if;
    elsif not exists(select 1 from private.bpay_next_snooze_event e where e.request_id=new.request_id
      and e.actor_id=new.actor_id and e.candidate_id=(new.request_body->>'candidate_id')::uuid
      and e.snooze_id=(new.result_body->>'snooze_id')::uuid and e.version=(new.result_body->>'version')::bigint
      and e.control_sequence=(new.result_body->>'control_sequence')::bigint
      and (e.after_state->>'active')::boolean is not distinct from (new.result_body->>'active')::boolean
      and (new.request_body->>'expected_version')::bigint=e.version-1
      and ((new.action='SNOOZE_CLEAR' and e.event_kind='CLEARED' and e.snooze_id=(new.request_body->>'snooze_id')::uuid)
        or (new.action='SNOOZE_SET' and e.event_kind in ('SET','AMENDED')
          and (e.after_state->>'work_id')::uuid is not distinct from (new.request_body->>'work_id')::uuid
          and (e.after_state->>'expected_revision_id')::uuid is not distinct from (new.request_body->>'expected_revision_id')::uuid
          and e.after_state->>'scope_kind'=new.request_body->>'scope_kind'
          and e.after_state->>'segment_key' is not distinct from new.request_body->>'segment_key'
          and (new.request_body->>'scope_kind'<>'EXPENSE_LINE' or e.after_state->>'component_key'=new.request_body->>'component_key')
          and (e.after_state->>'case_id')::uuid is not distinct from (new.request_body->>'case_id')::uuid
          and (e.after_state->>'case_component_id')::uuid is not distinct from (new.request_body->>'case_component_id')::uuid
          and e.after_state->>'snooze_kind'=case upper(btrim(new.request_body->>'snooze_kind'))
            when 'BLOCKED' then 'BLOCKED_TIMESHEET' when 'LOAN_REPAYMENT' then 'PAYMENT_ADVANCE_REPAYMENT'
            else upper(btrim(new.request_body->>'snooze_kind')) end
          and (e.after_state->>'worker_pay_basis')::bytea=decode(new.request_body->>'worker_pay_basis','hex')
          and (e.after_state->>'until_date')::date is not distinct from (new.request_body->>'until_date')::date
          and e.after_state->>'note'=coalesce(new.request_body->>'note','')))) then
      raise exception using errcode='23514',message='BPAY_NEXT_SNOOZE_REQUEST_EVENT_MISMATCH';
    end if;
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_retry_work_job_v1(p_actor_user_id uuid,p_args jsonb,p_readback boolean)
returns jsonb language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[]:=array['request_id','candidate_id','work_id','job_id','module_epoch','expected_owner_epoch','expected_phase','expected_cursor'];
  v_request uuid;v_candidate uuid;v_work_id uuid;v_job_id uuid;v_epoch bigint;
  v_saved private.bpay_next_workbench_command_receipt%rowtype;
  v_job private.bpay_next_job%rowtype;v_publication private.bpay_next_publication%rowtype;
  v_work private.bpay_next_work%rowtype;v_command private.bpay_next_command%rowtype;
  v_revision private.bpay_next_work_revision%rowtype;v_projection private.bpay_next_workbench_revision%rowtype;
  v_repaired boolean:=false;v_result jsonb;v_now timestamptz;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_actor_user_id is null or p_readback is null or p_args is null or jsonb_typeof(p_args)<>'object'
    or pg_catalog.octet_length(p_args::text)>2048
    or (select count(*) from jsonb_object_keys(p_args))<>8
    or exists(select 1 from jsonb_object_keys(p_args) k where not(k=any(v_keys)))
    or exists(select 1 from unnest(array['request_id','candidate_id','job_id']) k
      where jsonb_typeof(p_args->k) is distinct from 'string'
        or (p_args->>k)!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')
    or exists(select 1 from unnest(array['module_epoch','expected_owner_epoch']) k
      where jsonb_typeof(p_args->k) is distinct from 'string'
        or case when (p_args->>k)~'^[1-9][0-9]{0,18}$'
          then (p_args->>k)::numeric>9223372036854775807 else true end)
    or jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
    or pg_catalog.octet_length(p_args->>'expected_phase') not between 1 and 64
    or jsonb_typeof(p_args->'expected_cursor') not in ('string','null')
    or pg_catalog.octet_length(p_args->>'expected_cursor')>512 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  if p_args->'work_id'<>'null'::jsonb and (jsonb_typeof(p_args->'work_id') is distinct from 'string'
    or(p_args->>'work_id')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$') then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID';end if;
  v_request:=(p_args->>'request_id')::uuid;v_candidate:=(p_args->>'candidate_id')::uuid;
  v_work_id:=(p_args->>'work_id')::uuid;v_job_id:=(p_args->>'job_id')::uuid;
  select r.* into v_saved from private.bpay_next_workbench_command_receipt r where r.request_id=v_request;
  if found then
    if v_saved.actor_user_id<>p_actor_user_id or v_saved.request_body is distinct from p_args then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT';end if;
    return v_saved.result||jsonb_build_object('replay',true);
  end if;
  if p_readback then
    return jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'work_id',v_work_id,
      'job_id',v_job_id,'received',false,'replay',false);
  end if;
  select m.owner_epoch into strict v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch<>(p_args->>'module_epoch')::bigint then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_COMMAND_EPOCH_STALE';end if;
  if v_work_id is null then return private.bpay_next_retry_prepare_job_v1(p_actor_user_id,p_args);end if;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  if not found then raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  -- Recheck after the Candidate lock. Concurrent identical requests replay
  -- even when the first request has already made this job claimable.
  select r.* into v_saved from private.bpay_next_workbench_command_receipt r where r.request_id=v_request;
  if found then
    if v_saved.actor_user_id<>p_actor_user_id or v_saved.request_body is distinct from p_args then
      raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_COMMAND_REPLAY_CONFLICT';end if;
    return v_saved.result||jsonb_build_object('replay',true);
  end if;
  select j.* into v_job from private.bpay_next_job j where j.id=v_job_id for update;
  if not found or v_job.candidate_id<>v_candidate or v_job.module_epoch<>v_epoch or v_job.job_kind<>'POSITION_APPLY'
    or v_job.owner_epoch<>(p_args->>'expected_owner_epoch')::bigint
    or v_job.phase is distinct from p_args->>'expected_phase'
    or v_job.cursor_key is distinct from p_args->>'expected_cursor' then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_STATE_CHANGED';end if;
  select c.* into v_command from private.bpay_next_command c where c.id=v_job.command_id;
  if not found or v_command.module_epoch<>v_epoch or v_command.agency_sequence<>v_job.command_sequence
    or v_command.command_kind<>'POSITION_APPLY'
    or v_command.expected_member_count is distinct from 1 or v_command.enrolled_member_count<>1
    or not exists(select 1 from private.bpay_next_command_member cm
      where cm.command_id=v_job.command_id and cm.candidate_id=v_candidate and cm.member_no=1) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  select p.* into v_publication from private.bpay_next_publication p where p.command_id=v_job.command_id for update;
  if not found or v_publication.work_id<>v_work_id or v_publication.candidate_id<>v_candidate then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  select w.* into v_work from private.bpay_next_work w where w.id=v_work_id for update;
  select r.* into v_revision from private.bpay_next_work_revision r
    where r.id=v_publication.revision_id and r.work_id=v_work_id;
  if v_work.candidate_id is distinct from v_candidate or v_revision.id is null
    or v_revision.revision_no<>v_publication.revision_no or v_job.applied_line_count>v_revision.expected_line_count
    or (v_publication.phase in ('REMOVED','SNOOZE','DONE') and v_job.applied_line_count<>v_revision.expected_line_count)
    or (v_publication.phase='NEW' and v_publication.cursor_key is null and v_job.applied_line_count<>0)
    or (v_publication.phase='NEW' and v_publication.cursor_key is not null and
      (v_job.applied_line_count=0 or not exists(select 1 from private.bpay_next_approved_line l
        join private.bpay_next_position p on p.work_id=v_work_id and p.component_key=l.component_key
        and p.applied_revision_id=v_revision.id
        where l.revision_id=v_revision.id and l.component_key=v_publication.cursor_key))) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  if v_job.status='DONE' and v_publication.status='APPLIED' and v_publication.phase='DONE' then
    if v_job.phase<>'DONE' or v_job.cursor_key is not null or v_command.status<>'COMPLETE' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
  elsif v_job.status<>'FAILED' or v_job.last_error_code is null then
    raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_NOT_DEFINITELY_FAILED';
  elsif v_publication.status='APPLIED' and v_publication.phase='DONE' then
    if v_command.status<>'COMPLETE' then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
    -- Proven canonical publication completion repairs only job metadata.
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,
      lease_nonce=null,lease_until_utc=null where id=v_job_id returning * into v_job;
    v_repaired:=true;
  else
    if v_publication.phase not in ('NEW','REMOVED','SNOOZE') or v_publication.status not in ('QUEUED','APPLYING')
      or v_work.applied_revision_id is distinct from v_publication.predecessor_revision_id
      or pg_catalog.octet_length(v_publication.cursor_key)>512
      or (v_publication.phase='SNOOZE' and (v_publication.status<>'APPLYING' or v_command.status<>'SEALED'
        or (v_publication.cursor_key is not null and (v_publication.cursor_key!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
          or not exists(select 1 from private.bpay_next_snooze s where s.work_id=v_work_id and s.id=v_publication.cursor_key::uuid))))) then
      raise exception using errcode='55000',message='BPAY_NEXT_WORK_JOB_REPAIR_REQUIRED';end if;
    -- The native publication is the atomic twin checkpoint, not a guessed
    -- default or latest Work revision. Counts and original inputs are retained.
    v_repaired:=v_job.phase is distinct from v_publication.phase
      or v_job.cursor_key is distinct from v_publication.cursor_key;
    v_now:=pg_catalog.clock_timestamp();
    update private.bpay_next_job set status='READY',phase=v_publication.phase,cursor_key=v_publication.cursor_key,
      lease_nonce=null,lease_until_utc=null,available_at_utc=v_now,queue_wake_after_utc=v_now
      where id=v_job_id returning * into v_job;
  end if;
  select r.* into strict v_projection from private.bpay_next_workbench_revision r where r.id=1;
  v_result:=jsonb_build_object('request_id',v_request,'candidate_id',v_candidate,'work_id',v_work_id,
    'job_id',v_job_id,'command_id',v_job.command_id,'command_sequence',v_job.command_sequence::text,
    'module_epoch',v_epoch::text,'owner_epoch',v_job.owner_epoch::text,'status',v_job.status,
    'phase',v_job.phase,'cursor',v_job.cursor_key,'first_recorded_error_code',v_job.first_recorded_error_code,
    'last_error_code',v_job.last_error_code,'workbench_revision',v_projection.workbench_revision::text,
    'adoption_revision',v_projection.adoption_revision::text,'repaired',v_repaired,'received',true,'replay',false,
    'captured_at_utc',to_char(clock_timestamp() at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'));
  insert into private.bpay_next_workbench_command_receipt(request_id,actor_user_id,candidate_id,work_id,job_id,
    module_epoch,request_body,result) values(v_request,p_actor_user_id,v_candidate,v_work_id,v_job_id,v_epoch,p_args,v_result);
  return v_result;
end
$function$;

create or replace function public.bpay_next_financial_job_v1(
  p_action text,p_job_id uuid,p_job_kind text,p_args jsonb
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_keys text[];
  v_key text;
  v_paged boolean;
  v_integer_cursor boolean;
  v_phase text;
  v_lease_seconds integer;
  v_nonce uuid;
  v_owner_epoch bigint;
  v_cursor bigint;
  v_text_cursor text;
  v_work_cursor uuid;
  v_component_cursor text;
  v_limit integer;
  v_claim record;
  v_claim_state record;
  v_page record;
  v_worker private.bpay_next_run_worker%rowtype;
  v_result jsonb;
  v_output jsonb;
  v_lease_until text;
  v_claim_failure text;
  v_case_net boolean:=false;
  v_expired_prepare boolean:=false;
  v_discarded_prepare boolean:=false;
  v_defer_pin_module boolean:=false;
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
      nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_JOB_FORBIDDEN';
  end if;
  if p_action is null or p_action not in ('CLAIM','STEP','FAILURE') or p_job_id is null
     or p_job_kind is null or p_job_kind not in
       ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN','CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL','INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','PREPARATION_DISCARD','CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF')
     or p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object'
     or pg_catalog.octet_length(p_args::text)>2048 then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='FAILURE' then
    if p_job_kind<>'PREPARE' then raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_failure_v1(p_job_id,p_args);
  end if;
  v_phase:=p_args->>'expected_phase';
  if p_action='STEP' and (p_job_kind in ('POSITION_APPLY','PREPARE')
      or (p_job_kind='PAYE_NET_ENTRY' and p_args ? 'expected_phase'))
     and (pg_catalog.jsonb_typeof(p_args->'expected_phase') is distinct from 'string'
       or not(v_phase=any(case p_job_kind when 'POSITION_APPLY' then array['NEW','REMOVED','SNOOZE']
         when 'PAYE_NET_ENTRY' then array['FINANCE_ALLOCATE']
         else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  v_paged:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','POSITION_APPLY','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_integer_cursor:=p_job_kind in ('TRANSFER_BUILD','CSV_SETTLEMENT','INTERNAL_SETTLEMENT','SIMPLE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD')
    or (p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE')
    or (p_job_kind='PREPARE' and v_phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'));
  v_keys:=case when p_action='CLAIM' then array['lease_seconds']
    when p_job_kind='POSITION_APPLY' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PAYE_NET_ENTRY' and v_phase='FINANCE_ALLOCATE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then array['lease_nonce','owner_epoch','expected_phase','expected_cursor']
    when p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then array['lease_nonce','owner_epoch','expected_phase','expected_work_cursor','expected_component_cursor','limit']
    when p_job_kind='PREPARE' then array['lease_nonce','owner_epoch','expected_phase','expected_cursor','limit']
    when v_paged then array['lease_nonce','owner_epoch','expected_cursor','limit']
    else array['lease_nonce','owner_epoch'] end;
  if (select count(*) from pg_catalog.jsonb_object_keys(p_args))<>pg_catalog.array_length(v_keys,1)
     or exists(select 1 from pg_catalog.jsonb_object_keys(p_args) k where not(k=any(v_keys))) then
    raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
  end if;
  if p_action='CLAIM' then
    if pg_catalog.jsonb_typeof(p_args->'lease_seconds') is distinct from 'number'
       or (p_args->>'lease_seconds')!~'^[1-9][0-9]{0,2}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    v_lease_seconds:=(p_args->>'lease_seconds')::integer;
    if v_lease_seconds not between 1 and 120 then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  else
    if pg_catalog.jsonb_typeof(p_args->'lease_nonce') is distinct from 'string'
       or (p_args->>'lease_nonce')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
    foreach v_key in array array['owner_epoch','expected_cursor'] loop
      if v_key='expected_cursor' and (not coalesce(v_integer_cursor,false) or p_args->v_key='null'::jsonb) then
        continue;
      end if;
      if pg_catalog.jsonb_typeof(p_args->v_key) is distinct from 'string'
         or (p_args->>v_key)!~'^[1-9][0-9]{0,18}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      if (p_args->>v_key)::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end loop;
    v_nonce:=(p_args->>'lease_nonce')::uuid;
    v_owner_epoch:=(p_args->>'owner_epoch')::bigint;
    if v_integer_cursor then v_cursor:=(p_args->>'expected_cursor')::bigint; end if;
    if p_job_kind='POSITION_APPLY' then
      if p_args->'expected_cursor'<>'null'::jsonb
         and (pg_catalog.jsonb_typeof(p_args->'expected_cursor') is distinct from 'string'
              or pg_catalog.char_length(p_args->>'expected_cursor') not between 1 and 256
              or (v_phase='SNOOZE' and (p_args->>'expected_cursor')!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_text_cursor:=p_args->>'expected_cursor';
    elsif p_job_kind='PREPARE' and v_phase in ('RESERVE','ALLOCATE') then
      -- A cursor is the complete (UUID work, opaque retained component TEXT)
      -- pair, or both NULL. Component bounds are CHARACTERS, not bytes.
      if (p_args->'expected_work_cursor'='null'::jsonb) is distinct from
          (p_args->'expected_component_cursor'='null'::jsonb)
         or (p_args->'expected_work_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_work_cursor') is distinct from 'string'
                  or (p_args->>'expected_work_cursor')!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'))
         or (p_args->'expected_component_cursor'<>'null'::jsonb
             and (pg_catalog.jsonb_typeof(p_args->'expected_component_cursor') is distinct from 'string'
                  or pg_catalog.char_length(p_args->>'expected_component_cursor') not between 1 and 256)) then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_work_cursor:=(p_args->>'expected_work_cursor')::uuid;
      v_component_cursor:=p_args->>'expected_component_cursor';
    end if;
    if v_paged then
      if pg_catalog.jsonb_typeof(p_args->'limit') is distinct from 'number'
         or (p_args->>'limit')!~'^[1-9][0-9]{0,2}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
      v_limit:=(p_args->>'limit')::integer;
      if v_limit not between 1 and 100 then
        raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
      end if;
    end if;
  end if;

  -- Immutable dispatch identity, without an outer job lock. Owners acquire
  -- Candidate -> job for CLAIM, parent run -> Candidate -> job for STEP.
  -- Taking job FOR UPDATE here would invert that established order.
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>p_job_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_JOB_KIND_MISMATCH';
  end if;
  if p_job_kind='PREPARE' then
    v_output:=private.bpay_next_prepare_retry_admit_v1(p_job_id,p_action,p_args);
    if v_output is not null then return v_output;end if;
  end if;
  -- Query admission is outside even the public module SHARE. Plain dispatch
  -- discovery is not lease/module authority; every owner rechecks under locks.
  -- Definite terminal/advanced/cursor/stale replies must not newly contend on
  -- the query key. Their independent owner locks/validates the module before
  -- any write, and this boundary reasserts it immediately after that call.
  if p_action='STEP' and p_job_kind='PREPARE' and v_phase in ('NEW','PINNING') then
    if v_job.status='LEASED' and v_job.phase in ('NEW','PINNING')
       and v_job.lease_nonce is not distinct from v_nonce
       and v_job.owner_epoch is not distinct from v_owner_epoch
       and v_job.lease_until_utc>pg_catalog.clock_timestamp()
       and v_job.cursor_key::bigint is not distinct from v_cursor then
      perform private.weekly_source_pay_query_admit_v2();
    else
      v_defer_pin_module:=true;
    end if;
  end if;
  if not v_defer_pin_module and not exists(
      select 1 from private.bpay_next_module_control m
      where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  if p_job_kind='PAYE_NET_ENTRY' then
    select r.case_draft_state_id is not null into strict v_case_net
      from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id;
    if p_action='STEP' and v_case_net is distinct from (p_args ? 'expected_phase') then
      raise exception using errcode='22023',message='BPAY_NEXT_JOB_REQUEST_INVALID';
    end if;
  end if;
  if p_action='CLAIM' then
    if p_job_kind='POSITION_APPLY' and v_job.phase='SNOOZE' and v_job.cursor_key is not null
      and v_job.cursor_key!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';end if;
    if (p_job_kind='POSITION_APPLY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['DONE'] else array['NEW','REMOVED','SNOOZE'] end)))
       or (p_job_kind='PREPARE' and not(v_job.phase=any(case when v_job.status='DONE' then array['READY','DONE','EXPIRED','DISCARDED','CANCELLED']
          else array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL'] end)))
       or (p_job_kind='PREPARATION_EXPIRY' and not(v_job.phase=any(case
          when v_job.status='DONE' then array['EXPIRED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind='PREPARATION_DISCARD' and not(v_job.phase=any(case when v_job.status='DONE' then array['DISCARDED','BLOCKED'] else array['NEW','RELEASE'] end)))
       or (p_job_kind in ('CASE_CREATE','CASE_EDIT','CASE_PAUSE','CASE_RESUME','CASE_WRITE_OFF') and (v_job.phase is distinct from
          case when v_job.status='DONE' then 'DONE' else 'NEW' end or v_job.cursor_key is not null)) then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
    end if;
    -- Distinguish terminal and busy claims; never return the stored nonce.
    -- These read-only early checks are not ownership: owners recheck/lock.
    if v_job.status='DONE' then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
    end if;
    if v_job.status='LEASED' and v_job.lease_until_utc>pg_catalog.clock_timestamp() then
      raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
    end if;
    begin
      case p_job_kind
        when 'POSITION_APPLY' then
          select c.* into strict v_claim from private.bpay_next_claim_position_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARE' then
          select c.* into strict v_claim from private.bpay_next_claim_prepare_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_EXPIRY' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_expiry_job_v1(p_job_id,v_lease_seconds) c;
        when 'PREPARATION_DISCARD' then
          select c.* into strict v_claim from private.bpay_next_claim_preparation_discard_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_CREATE' then
          select c.* into strict v_claim from private.bpay_next_claim_case_create_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_EDIT' then
          select c.* into strict v_claim from private.bpay_next_claim_case_edit_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_PAUSE','CASE_RESUME' then
          select c.* into strict v_claim from private.bpay_next_claim_case_pause_job_v1(p_job_id,v_lease_seconds) c;
        when 'CASE_WRITE_OFF' then
          select c.* into strict v_claim from private.bpay_next_claim_write_off_job_v1(p_job_id,v_lease_seconds) c;
        when 'PAYE_NET_ENTRY' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_paye_net_job_v1(p_job_id,v_lease_seconds) c;
        when 'TRANSFER_BUILD' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_transfer_job_v1(p_job_id,v_lease_seconds) c;
        when 'SIMPLE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_simple_cancel_job_v1(p_job_id,v_lease_seconds) c;
        when 'REISSUE_CANCEL' then
          select c.* into strict v_claim from private.bpay_next_claim_reissue_cancel_job_v1(p_job_id,v_lease_seconds) c;
        else
          select c.* into strict v_claim from private.bpay_next_claim_simple_outcome_job_v1(p_job_id,v_lease_seconds) c;
      end case;
    exception when sqlstate '55000' then
      get stacked diagnostics v_claim_failure=message_text;
      if v_claim_failure not in
          ('BPAY_NEXT_PAYE_NET_JOB_NOT_CLAIMABLE','BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_OUTCOME_JOB_NOT_CLAIMABLE','BPAY_NEXT_CANCEL_JOB_NOT_CLAIMABLE',
           'BPAY_NEXT_JOB_NOT_CLAIMABLE','BPAY_NEXT_PREPARE_JOB_NOT_CLAIMABLE','BPAY_NEXT_REISSUE_CANCEL_JOB_NOT_CLAIMABLE','BPAY_NEXT_EXPIRY_JOB_NOT_CLAIMABLE','BPAY_NEXT_DISCARD_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_CREATE_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_EDIT_JOB_NOT_CLAIMABLE','BPAY_NEXT_CASE_PAUSE_JOB_NOT_CLAIMABLE','BPAY_NEXT_WRITE_OFF_JOB_NOT_CLAIMABLE','BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING') then
        raise;
      end if;
      if v_claim_failure='BPAY_NEXT_EARLIER_WORKER_COMMAND_PENDING'
         and p_job_kind not in ('POSITION_APPLY','PREPARE','REISSUE_CANCEL','PREPARATION_EXPIRY','PREPARATION_DISCARD') then raise; end if;
      -- A loser may have read READY, then waited behind the winning owner.
      -- The failed subtransaction released its owner locks. Classify its
      -- now-visible exact job without acquiring job ahead of Candidate/run,
      -- retrying any owner or adopting the winner's stored lease nonce.
      -- Job and admission predicates share one fresh read snapshot. A
      -- predecessor finishing during separate reads must not manufacture a
      -- permanent eligibility failure for an already accepted financial job.
      select j.*,
        exists(select 1 from private.bpay_next_pay_run r
          where r.id=private.bpay_next_prepare_job_run_v1(j.id) and r.status='PREPARING' and r.selection_state='SEALED'
            and private.bpay_next_prepare_job_active_v1(j.id)
            and exists(select 1 from private.bpay_next_run_worker w where w.run_id=r.id and w.candidate_id=j.candidate_id
              and (w.status='PREPARING' or (j.phase in ('WORKBENCH_WORK','WORKBENCH_CASE') and w.status in ('READY','REVIEW','CANCELLED'))
                or (j.phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') and w.status='CANCELLING')))) as prepare_admissible,
        exists(select 1 from private.bpay_next_cancel_request r
               where r.command_id=j.command_id and r.candidate_id=j.candidate_id
                 and r.status in ('REQUESTED','CANCELLING')) as cancel_request_pending,
        exists(select 1 from private.bpay_next_preparation_expiry_request r
          join private.bpay_next_pay_run run on run.id=r.run_id
          where r.command_id=j.command_id and r.status='CANCELLING'
            and run.status='CANCELLING' and run.confirmed_at_utc is null) as expiry_request_pending,
        exists(select 1 from private.bpay_next_preparation_discard_request q join private.bpay_next_pay_run r on r.id=q.run_id
          where q.command_id=j.command_id and q.status='CANCELLING' and r.status='CANCELLING' and r.confirmed_at_utc is null) as discard_request_pending,
        exists(select 1 from private.bpay_next_case_create_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_create_request_present,
        exists(select 1 from private.bpay_next_case_edit_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id) as case_edit_request_present,
        exists(select 1 from private.bpay_next_case_pause_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.action=j.job_kind
            and r.module_epoch=j.module_epoch and r.command_sequence=j.command_sequence) as case_pause_request_present,
        exists(select 1 from private.bpay_next_write_off_request r
          where r.command_id=j.command_id and r.candidate_id=j.candidate_id and r.status='REQUESTED') as write_off_request_pending,
        exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=j.candidate_id
                 and earlier.command_sequence<j.command_sequence
                 and earlier.status<>'DONE') as predecessor_pending
      into strict v_claim_state from private.bpay_next_job j where j.id=p_job_id;
      if v_claim_state.job_kind<>p_job_kind or v_claim_state.module_epoch is distinct from
          (select m.owner_epoch from private.bpay_next_module_control m
           where m.id=1 and m.active_owner='NEXT') then
        raise;
      end if;
      if v_claim_state.status='DONE' then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_TERMINAL';
      end if;
      if not(v_claim_state.phase=any(case p_job_kind
          when 'PAYE_NET_ENTRY' then array['NEW','FINANCE_ALLOCATE']
          when 'TRANSFER_BUILD' then array['WORK']
          when 'SIMPLE_CANCEL' then array['NEW','RELEASE']
          when 'REISSUE_CANCEL' then array['NEW']
          when 'POSITION_APPLY' then array['NEW','REMOVED','SNOOZE']
          when 'PREPARE' then array['NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL']
          when 'PREPARATION_EXPIRY' then array['NEW','RELEASE']
          when 'PREPARATION_DISCARD' then array['NEW','RELEASE']
          when 'CASE_CREATE' then array['NEW']
          when 'CASE_EDIT' then array['NEW']
          when 'CASE_PAUSE' then array['NEW']
          when 'CASE_RESUME' then array['NEW']
          when 'CASE_WRITE_OFF' then array['NEW']
          else array['MEMBERS'] end))
         or (p_job_kind='PREPARE' and not v_claim_state.prepare_admissible)
         or (p_job_kind='PREPARATION_EXPIRY' and not v_claim_state.expiry_request_pending)
         or (p_job_kind='PREPARATION_DISCARD' and not v_claim_state.discard_request_pending)
         or (p_job_kind='CASE_CREATE' and (not v_claim_state.case_create_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_EDIT' and (not v_claim_state.case_edit_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (not v_claim_state.case_pause_request_present
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='CASE_WRITE_OFF' and (not v_claim_state.write_off_request_pending
              or v_claim_state.cursor_key is not null))
         or (p_job_kind='SIMPLE_CANCEL'
             and (v_claim_state.available_at_utc>pg_catalog.clock_timestamp()
                  or not v_claim_state.cancel_request_pending)) then
        raise;
      end if;
      if v_claim_state.status='LEASED' and v_claim_state.lease_until_utc>pg_catalog.clock_timestamp() then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_LEASE_BUSY';
      end if;
      if v_claim_state.status in ('READY','LEASED') and v_claim_state.predecessor_pending then
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_WAIT_FOR_PREDECESSOR';
      end if;
      if v_claim_state.status in ('READY','LEASED') and not v_claim_state.predecessor_pending
         and (v_claim_state.status='READY'
              or v_claim_state.lease_until_utc<=pg_catalog.clock_timestamp()) then
        -- The predecessor just completed or the short lease just expired.
        -- Only signal a fresh CLAIM; never invoke the owner a second time.
        raise exception using errcode='55000',message='BPAY_NEXT_JOB_CLAIM_RETRY';
      end if;
      -- Other permanent scope/phase/request admission failures retain the
      -- exact original owner rejection, not a general automatic retry.
      raise;
    end;
    -- Only the owner-returned newly acquired token crosses this boundary.
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status<>'LEASED' or v_job.lease_nonce is distinct from v_claim.lease_nonce
       or v_job.owner_epoch is distinct from v_claim.owner_epoch then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_CLAIM_RESULT_INVALID';
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'phase',v_job.phase,
      'lease_nonce',v_claim.lease_nonce,'owner_epoch',v_claim.owner_epoch::text,
      'lease_until_utc',v_lease_until,'cursor',v_job.cursor_key);
    if p_job_kind='PREPARE' then
      v_output:=v_output||pg_catalog.jsonb_build_object(
        'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
        'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
        'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end);
    end if;
  else
    -- Do not reject cleared DONE leases here. The owners' scoped DONE replay
    -- branches are authoritative and return the immutable retained result.
    case p_job_kind
      when 'POSITION_APPLY' then
        select p.* into strict v_page from private.bpay_next_apply_position_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_phase,v_text_cursor,v_limit) p;
        v_result:=pg_catalog.jsonb_build_object('phase',v_page.next_phase,
          'cursor',v_page.next_cursor,'rows_visited',v_page.rows_visited::text,'done',v_page.done);
      when 'PREPARE' then
        -- Terminal authority comes from the retained owner, never a fabricated
        -- READY row or an adopted active cursor/nonce. One owner per STEP.
        select private.bpay_next_preparation_expiry_fenced_v1(
          rc.run_id,rc.command_id) into v_expired_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        select private.bpay_next_preparation_discard_fenced_v1(rc.run_id,rc.command_id) into v_discarded_prepare
          from private.bpay_next_run_command rc where rc.run_id=private.bpay_next_prepare_job_run_v1(p_job_id);
        v_discarded_prepare:=coalesce(v_discarded_prepare,false) or(v_job.status='DONE' and v_job.phase='DISCARDED'
          and exists(select 1 from private.bpay_next_worker_reprepare_request x where x.previous_job_id=p_job_id
            and x.candidate_id=v_job.candidate_id and x.module_epoch=v_job.module_epoch and x.initiator_kind='OFFICE' and x.action='RELEASE'));
        if coalesce(v_discarded_prepare,false) then
          if v_job.status<>'DONE' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_DISCARDING';end if;
          v_result:=private.bpay_next_discarded_prepare_receipt_v1(p_job_id);
        elsif coalesce(v_expired_prepare,false) then
          if v_job.status<>'DONE' then
            raise exception using errcode='55000',message='BPAY_NEXT_PREPARATION_EXPIRING';
          end if;
          -- May have been retired before its first CLAIM: no worker is invented.
          -- Only the exact expiry owner validates and returns this old receipt.
          v_result:=private.bpay_next_expired_prepare_receipt_v1(p_job_id);
        elsif v_job.status='DONE' and v_job.phase='CANCELLED' then
          if not exists(select 1 from private.bpay_next_worker_reprepare_request x
            join private.bpay_next_run_worker w on w.id=x.run_worker_id
            where x.job_id=p_job_id and x.action='RELEASE' and x.status='COMPLETE'
              and x.target_preparation_revision=w.preparation_revision and w.status='CANCELLED'
              and x.module_epoch=v_job.module_epoch) then
            raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
          v_result:=pg_catalog.jsonb_build_object('phase','CANCELLED','cursor',v_job.cursor_key,'rows_visited','0','replay',true);
        elsif v_job.status='DONE' and (exists(select 1 from private.bpay_next_case_allocation_state s
          join private.bpay_next_run_worker w on w.id=s.run_worker_id
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id
            and s.preparation_revision=w.captured_preparation_revision and s.pass_kind='DRAFT')
          or exists(select 1 from private.bpay_next_run_worker w
            where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id and w.status='REVIEW'
              and w.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
                'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT'))) then
          -- An original NEW/PINNING request has no page limit. Terminal
          -- read-back does no work, but the owner still requires a valid bound.
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,coalesce(v_limit,1));
        elsif v_job.status='DONE' and v_job.phase='READY' then
          v_result:=private.bpay_next_complete_no_case_worker_v1(p_job_id,v_nonce,v_owner_epoch);
        elsif v_job.status='DONE' and v_job.phase='DONE' then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,null);
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited','0');
        elsif v_phase in ('WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then
          v_result:=private.bpay_next_worker_release_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('WORKBENCH_WORK','WORKBENCH_CASE') then
          v_result:=private.bpay_next_prepare_projection_page_v1(p_job_id,v_nonce,v_owner_epoch,v_phase,v_cursor,v_limit);
        elsif v_phase in ('NEW','PINNING') then
          v_result:=private.bpay_next_pin_selected_work_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor);
          -- This owner examines at most one selected work. It exposes no page
          -- count, but its exact work/replay receipt identifies zero vs one.
          v_result:=v_result||pg_catalog.jsonb_build_object('rows_visited',case
            when v_result->>'rows_visited' is not null then v_result->>'rows_visited'
            when v_result->>'replay'='true' then '0'
            when v_result->>'work_id' is not null then '1' else '0' end);
        elsif v_phase='RESERVE' then
          v_result:=private.bpay_next_capture_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='ALLOCATE' then
          v_result:=private.bpay_next_hold_fresh_position_page_v1(p_job_id,v_nonce,v_owner_epoch,v_work_cursor,v_component_cursor,v_limit);
        elsif v_phase='FINANCE_ALLOCATE' then
          v_result:=private.bpay_next_prepare_case_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          raise exception using errcode='55000',message='BPAY_NEXT_JOB_PHASE_UNSUPPORTED';
        end if;
      when 'PAYE_NET_ENTRY' then
        if v_case_net then
          v_result:=private.bpay_next_apply_case_paye_net_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
        else
          v_result:=private.bpay_next_apply_simple_paye_net_v1(p_job_id,v_nonce,v_owner_epoch);
        end if;
      when 'TRANSFER_BUILD' then
        v_result:=private.bpay_next_build_simple_transfer_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'INTERNAL_SETTLEMENT' then
        v_result:=private.bpay_next_post_simple_outcome_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CSV_RETURN' then
        v_result:=private.bpay_next_post_simple_return_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASH_REISSUE' then
        v_result:=private.bpay_next_build_simple_reissue_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'SIMPLE_CANCEL' then
        v_result:=private.bpay_next_cancel_simple_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'REISSUE_CANCEL' then
        v_result:=private.bpay_next_apply_reissue_cancel_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'PREPARATION_EXPIRY' then
        v_result:=private.bpay_next_expire_preparation_worker_page_v1(
          p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'PREPARATION_DISCARD' then
        v_result:=private.bpay_next_discard_preparation_worker_page_v1(p_job_id,v_nonce,v_owner_epoch,v_cursor,v_limit);
      when 'CASE_CREATE' then
        -- One exact original approval application. The retained 0330 owner
        -- alone governs lease validation and DONE/DONE immutable replay.
        v_result:=private.bpay_next_apply_case_create_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_EDIT' then
        v_result:=private.bpay_next_apply_case_edit_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_PAUSE','CASE_RESUME' then
        v_result:=private.bpay_next_apply_case_pause_v1(p_job_id,v_nonce,v_owner_epoch);
      when 'CASE_WRITE_OFF' then
        v_result:=private.bpay_next_apply_write_off_v1(p_job_id,v_nonce,v_owner_epoch);
    end case;
    if v_defer_pin_module and not exists(
        select 1 from private.bpay_next_module_control m
        where m.id=1 and m.active_owner='NEXT' for share) then
      raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
    end if;
    select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
    if v_job.status not in ('LEASED','DONE')
       or (p_job_kind<>'POSITION_APPLY' and pg_catalog.jsonb_typeof(v_result->'replay') is distinct from 'boolean')
       or (p_job_kind='POSITION_APPLY' and ((v_result->>'done')::boolean is distinct from (v_job.status='DONE')
            or v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
      or (p_job_kind='PREPARE' and v_result->>'phase' is distinct from
            case when v_discarded_prepare then 'DISCARDED' when v_expired_prepare then 'EXPIRED'
              when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end)
       or (p_job_kind in ('PREPARATION_EXPIRY','PREPARATION_DISCARD') and (v_result->>'phase' is distinct from v_job.phase
            or v_result->>'cursor' is distinct from v_job.cursor_key))
       or (p_job_kind='CASE_CREATE' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or v_result->>'phase' is distinct from 'CASE_CREATED'
            or not exists(select 1 from private.bpay_next_case_create_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.case_id::text=v_result->>'case_id')))
       or (p_job_kind='CASE_EDIT' and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or v_result->>'phase' is distinct from case when v_job.last_error_code is null then 'CASE_EDITED' else 'BLOCKED' end
          or v_result->>'issue_code' is distinct from v_job.last_error_code
          or not exists(select 1 from private.bpay_next_case_edit_request q where q.command_id=v_job.command_id
            and q.candidate_id=v_job.candidate_id and q.case_id::text=v_result->>'case_id'
            and q.new_rule_id::text=v_result->>'rule_id' and (q.expected_case_revision+1)::text=v_result->>'case_revision')))
       or (p_job_kind in ('CASE_PAUSE','CASE_RESUME') and (v_job.status<>'DONE' or v_job.phase<>'DONE' or v_job.cursor_key is not null
          or not exists(select 1 from private.bpay_next_case_pause_request q where q.command_id=v_job.command_id
             and q.candidate_id=v_job.candidate_id and q.action=p_job_kind
             and private.bpay_next_case_pause_receipt_v1(q.command_id,q.case_id,q.candidate_id) is not distinct from(v_result-'replay'))))
       or (p_job_kind='CASE_WRITE_OFF' and (v_job.status<>'DONE' or v_job.phase<>'DONE'
            or v_job.cursor_key is not null or not exists(select 1 from private.bpay_next_write_off_request r
              where r.command_id=v_job.command_id and r.candidate_id=v_job.candidate_id
                and r.status in ('WRITTEN_OFF','BLOCKED','REVIEW') and r.status=v_result->>'phase'
                and r.case_id::text=v_result->>'case_id' and r.case_component_id::text=v_result->>'case_component_id'
                and r.applied_amount is not distinct from (v_result->>'applied_amount')::numeric
                and r.remaining_amount is not distinct from (v_result->>'remaining_amount')::numeric
                and r.protected_amount is not distinct from (v_result->>'protected_amount')::numeric
                and r.event_id::text is not distinct from v_result->>'event_id'
                and r.issue_code is not distinct from v_result->>'issue_code')))
       or (v_case_net and (v_result->>'phase' is distinct from
            case when v_job.status='DONE' and v_job.phase='DONE' then 'REVIEW' else v_job.phase end
          or v_result->>'cursor' is distinct from v_job.cursor_key
          or (v_result->>'run_worker_id')::uuid is distinct from
            (select r.run_worker_id from private.bpay_next_paye_net_request r where r.command_id=v_job.command_id))) then
      raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
    end if;
    if p_job_kind='PREPARE' then
      update private.bpay_next_job set retry_count=0,next_retry_at_utc=null,needs_attention=false,last_retry_owner_epoch=null,
        retry_prior_issue_code=null,retry_prior_issue_work_id=null where id=p_job_id;
    end if;
    -- Yield only this successfully validated page's own lease. The next ID
    -- notification takes a fresh CLAIM from the committed native checkpoint.
    if v_job.status='LEASED' then
      if v_job.lease_nonce is distinct from v_nonce or v_job.owner_epoch is distinct from v_owner_epoch then
        raise exception using errcode='23514',message='BPAY_NEXT_JOB_STEP_RESULT_INVALID';
      end if;
      update private.bpay_next_job set status='READY',lease_nonce=null,lease_until_utc=null
        where id=p_job_id and status='LEASED' and lease_nonce=v_nonce and owner_epoch=v_owner_epoch
        returning * into strict v_job;
    end if;
    v_lease_until:=pg_catalog.to_char(v_job.lease_until_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
    v_output:=pg_catalog.jsonb_build_object(
      'job_id',p_job_id,'job_kind',p_job_kind,'owner_epoch',v_job.owner_epoch::text,
      'lease_until_utc',v_lease_until,'done',v_job.status='DONE');
    if p_job_kind<>'POSITION_APPLY' then
      v_output:=v_output||pg_catalog.jsonb_build_object('replay',v_result->'replay');
    end if;
    case p_job_kind
      when 'POSITION_APPLY' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited');
      when 'PREPARE' then
        if coalesce(v_expired_prepare,false) or coalesce(v_discarded_prepare,false) then
          v_output:=v_output||pg_catalog.jsonb_build_object('phase',case when v_discarded_prepare then 'DISCARDED' else 'EXPIRED' end,
            'cursor',v_result->>'cursor','work_cursor',v_result->>'work_cursor',
            'component_cursor',v_result->>'component_cursor',
            'run_worker_id',v_result->>'run_worker_id','worker_status',v_result->>'worker_status',
            'issue_code',v_result->>'issue_code','issue_work_id',v_result->>'issue_work_id',
            'rows_visited',v_result->>'rows_visited');
        else
        select w.* into strict v_worker from private.bpay_next_run_worker w
          where w.run_id=private.bpay_next_prepare_job_run_v1(p_job_id) and w.candidate_id=v_job.candidate_id;
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',case when v_job.phase in ('NEW','PINNING','FINANCE_ALLOCATE','WORKBENCH_WORK','WORKBENCH_CASE','WORKER_RELEASE_WORK','WORKER_RELEASE_CASE','WORKER_RELEASE_FINAL') then v_job.cursor_key else null end,
          'work_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_work_cursor else null end,
          'component_cursor',case when v_job.phase in ('RESERVE','ALLOCATE') then v_job.position_component_cursor else null end,
          'run_worker_id',v_worker.id,'worker_status',v_worker.status,
          'issue_code',v_worker.review_issue_code,'issue_work_id',v_worker.review_issue_work_id,
          -- NULL means the exact owner did not report a count; do not fabricate
          -- zero for a REVIEW reached after earlier rows in the same page.
          'rows_visited',v_result->>'rows_visited');
        end if;
      when 'PAYE_NET_ENTRY' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','projection_id',v_result->>'projection_id');
        if v_case_net then
          v_output:=v_output||pg_catalog.jsonb_build_object(
            'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
            'run_worker_id',v_result->>'run_worker_id','issue_code',v_result->>'issue_code');
        end if;
      when 'TRANSFER_BUILD' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','transfer_id',v_result->>'transfer_id',
          'transfer_status',coalesce(v_result->>'transfer_status',v_result->>'phase'),
          'cursor',v_job.cursor_key,'member_count',v_result->>'member_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'INTERNAL_SETTLEMENT' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',case when v_job.status='DONE' then 'COMPLETE' else 'MEMBERS' end,
          'cursor',v_job.cursor_key,'posted_members',v_result->>'posted_members',
          'rows_visited',coalesce(v_result->>'rows_visited','0'));
      when 'CSV_RETURN' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','return_cash_id',v_result->>'return_cash_id');
      when 'CASH_REISSUE' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase','COMPLETE','transfer_id',v_result->>'transfer_id');
      when 'REISSUE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'transfer_id',v_result->>'transfer_id','blocked_code',v_result->>'blocked_code');
      when 'SIMPLE_CANCEL' then
        v_output:=v_output||pg_catalog.jsonb_build_object(
          'phase',v_result->>'phase','run_worker_id',v_result->>'run_worker_id',
          'cursor',v_job.cursor_key,'released_line_count',v_result->>'released_line_count',
          'rows_visited',coalesce(v_result->>'rows_visited','0'),
          'holds_released',coalesce(v_result->>'holds_released','0'),
          'blocked_code',v_result->>'blocked_code');
      when 'PREPARATION_EXPIRY','PREPARATION_DISCARD' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'cursor',v_result->>'cursor','rows_visited',v_result->>'rows_visited',
          'work_released',v_result->>'work_released','case_holds_released',v_result->>'case_holds_released',
          'run_worker_id',v_result->>'run_worker_id');
      when 'CASE_CREATE' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id');
      when 'CASE_EDIT' then
        v_output:=v_output||jsonb_build_object('phase',v_result->>'phase','case_id',v_result->>'case_id',
          'case_revision',v_result->>'case_revision','rule_id',v_result->>'rule_id','issue_code',v_result->>'issue_code');
      when 'CASE_PAUSE','CASE_RESUME' then
        v_output:=v_output||(v_result-'replay');
      when 'CASE_WRITE_OFF' then
        v_output:=v_output||pg_catalog.jsonb_build_object('phase',v_result->>'phase',
          'case_id',v_result->>'case_id','case_component_id',v_result->>'case_component_id',
          'applied_amount',v_result->>'applied_amount','remaining_amount',v_result->>'remaining_amount',
          'protected_amount',v_result->>'protected_amount','event_id',v_result->>'event_id','issue_code',v_result->>'issue_code');
    end case;
  end if;
  if pg_catalog.octet_length(v_output::text)>120000 then
    raise exception using errcode='54000',message='BPAY_NEXT_JOB_RESPONSE_INVALID';
  end if;
  return v_output;
end
$function$;
alter function private.bpay_next_snooze_revision_basis_v1(uuid,text,text) owner to postgres;
revoke all on function private.bpay_next_snooze_revision_basis_v1(uuid,text,text) from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_publication_cancel_valid_v1(jsonb,jsonb,boolean,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_snooze_publication_cancel_valid_v1(jsonb,jsonb,boolean,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer) owner to postgres;
revoke all on function private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_metadata_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_metadata_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_snooze_receipt_guard_v1() owner to postgres;
revoke all on function private.bpay_next_snooze_receipt_guard_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_retry_work_job_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;
alter function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_financial_job_v1(text,uuid,text,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
