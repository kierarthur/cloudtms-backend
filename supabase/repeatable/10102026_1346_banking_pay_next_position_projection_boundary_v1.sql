-- WB06 narrow1430 successor and required exact Processing/once-only projection support.
\set ON_ERROR_STOP on
begin;

-- One exact current projection invalidation; no native earnings calculation.
create or replace function private.bpay_next_workbench_mark_processing_v1(p_work_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare
  v_work private.bpay_next_work%rowtype;v_revision private.bpay_next_work_revision%rowtype;
  v_metadata private.bpay_next_workbench_candidate_metadata%rowtype;v_item private.bpay_next_workbench_item%rowtype;
  v_before private.bpay_next_workbench_delta[];v_after private.bpay_next_workbench_delta[];
  v_candidate uuid;v_client uuid;v_target text;v_financial bigint;v_projection bigint;v_was_dirty boolean;v_wait boolean;
  v_group text:='WORK:'||p_work_id::text;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  select c.financial_view_revision into strict v_financial from private.bpay_next_worker_control c
    where c.candidate_id=v_candidate for update;
  select w.* into strict v_work from private.bpay_next_work w where w.id=p_work_id for update;
  v_metadata:=private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  select c.pay_method::text into v_target from public.candidates c where c.id=v_candidate;
  if v_target not in ('PAYE','UMBRELLA') then v_target:=null;end if;
  select c.client_id into v_client from public.contracts c where c.id=v_work.contract_id;
  select r.* into v_revision from private.bpay_next_work_revision r where r.work_id=v_work.id
    and r.id=coalesce(v_work.current_revision_id,v_work.applied_revision_id);
  select exists(select 1 from private.bpay_next_workbench_group g where g.group_key=v_group and g.dirty) into v_was_dirty;
  -- Preserve the real stored-before WAIT marker across reauthorisation pages.
  select exists(select 1 from private.bpay_next_workbench_item i where i.group_key=v_group
    and i.reason_code='AWAITING_REAUTHORISATION') into v_wait;
  if v_work.approval_state='WITHDRAWN' then v_wait:=private.bpay_next_work_wait_state_v1(v_work.id);end if;
  select array_agg(d) into v_before from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  update private.bpay_next_workbench_revision set workbench_revision=workbench_revision+1,
    dirty_group_count=dirty_group_count+case when v_was_dirty then 0 else 1 end,
    updated_at_utc=transaction_timestamp() where id=1 returning workbench_revision into v_projection;
  delete from private.bpay_next_workbench_item where group_key=v_group;
  insert into private.bpay_next_workbench_group(group_key,group_kind,work_id,candidate_id,client_id,contract_id,
    physical_timesheet_id,current_revision_id,source_pay_channel,target_pay_channel,week_ending_date,
    complete,dirty,projection_revision,financial_view_revision,metadata_version,progress_state,certified_zero,
    ready_count,action_count,blocked_count,waiting_count)
  values(v_group,'WORK',v_work.id,v_candidate,v_client,v_work.contract_id,v_revision.physical_timesheet_id,
    v_work.current_revision_id,v_revision.source_pay_channel,v_target,v_work.week_ending_date,false,true,
    v_projection,v_financial,v_metadata.metadata_version,'PROCESSING',false,0,case when v_wait then 1 else 0 end,
    case when v_wait then 0 else 1 end,0)
  on conflict(group_key) do update set client_id=excluded.client_id,contract_id=excluded.contract_id,
    physical_timesheet_id=excluded.physical_timesheet_id,current_revision_id=excluded.current_revision_id,
    source_pay_channel=excluded.source_pay_channel,target_pay_channel=excluded.target_pay_channel,
    week_ending_date=excluded.week_ending_date,complete=false,dirty=true,certified_zero=false,
    projection_revision=excluded.projection_revision,financial_view_revision=excluded.financial_view_revision,
    metadata_version=excluded.metadata_version,progress_state='PROCESSING',error_code=null,
    ready_count=excluded.ready_count,action_count=excluded.action_count,blocked_count=excluded.blocked_count,
    waiting_count=0,updated_at_utc=transaction_timestamp();
  v_item:=null;v_item.id:=gen_random_uuid();v_item.group_key:=v_group;v_item.candidate_id:=v_candidate;
  v_item.work_id:=v_work.id;v_item.revision_id:=v_revision.id;v_item.member_kind:='GROUP_EVIDENCE';
  v_item.member_key:=v_work.id::text;v_item.component_key:='';v_item.portion_kind:='EVIDENCE';
  v_item.client_id:=v_client;v_item.contract_id:=v_work.contract_id;v_item.physical_timesheet_id:=v_revision.physical_timesheet_id;
  v_item.week_ending_date:=v_work.week_ending_date;v_item.candidate_display_name:=v_metadata.candidate_sort_name;
  v_item.candidate_reference:=v_metadata.tms_ref;v_item.client_display_name:=v_revision.client_display_name;
  v_item.timesheet_reference:=v_revision.timesheet_reference;v_item.source_pay_channel:=v_revision.source_pay_channel;
  v_item.target_pay_channel:=v_target;v_item.direction:='INFORMATIONAL';v_item.amount_known:=false;
  v_item.is_selected:=false;v_item.selected_amount_known:=false;v_item.selectable:=false;
  v_item.section:=case when v_wait then 'ACTION' else 'BLOCKED' end;
  v_item.reason_code:=case when v_wait then 'AWAITING_REAUTHORISATION'
    when v_work.approval_state='WITHDRAWN' then 'APPROVAL_WITHDRAWN' else 'APPLY_PENDING' end;
  v_item.reason_priority:=case when v_wait then 70 when v_work.approval_state='WITHDRAWN' then 20 else 30 end;
  v_item.query_state:='NOT_APPLICABLE';v_item.query_qualified:=false;v_item.progress_state:='PROCESSING';
  v_item.financial_view_revision:=v_financial;v_item.projection_revision:=v_projection;v_item.metadata_version:=v_metadata.metadata_version;
  v_item.updated_at_utc:=transaction_timestamp();insert into private.bpay_next_workbench_item select (v_item).*;
  select array_agg(d) into v_after from private.bpay_next_workbench_group_contribution_v1(v_group) d;
  perform private.bpay_next_workbench_apply_delta_v1(v_before,v_after);
end
$function$;

create or replace function private.bpay_next_workbench_refresh_many_v1(p_work_ids uuid[],p_case_ids uuid[])
returns void language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_candidate uuid;v_count bigint;v_case_before bigint[];v_case_index integer:=0;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if coalesce(cardinality(p_work_ids),0)+coalesce(cardinality(p_case_ids),0)>100
    or coalesce(pg_column_size(p_work_ids),0)+coalesce(pg_column_size(p_case_ids),0)>16384
    or array_position(p_work_ids,null::uuid) is not null or array_position(p_case_ids,null::uuid) is not null then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REFRESH_ENVELOPE_EXCEEDED';end if;
  if exists(select 1 from unnest(p_work_ids) x where not exists(select 1 from private.bpay_next_work w where w.id=x))
    or exists(select 1 from unnest(p_case_ids) x where not exists(select 1 from private.bpay_next_finance_case c where c.id=x)) then
    raise exception using errcode='23503',message='BPAY_NEXT_WORKBENCH_REFRESH_OWNER_MISSING';end if;
  -- Linked collection Cases are genuine related origins, never unrelated debt.
  select array_agg(x.id order by x.id) into p_case_ids from(select distinct id from unnest(p_case_ids) id
    union select wc.case_id from private.bpay_next_work_collection wc where wc.work_id=any(p_work_ids) and wc.case_id is not null
    order by id limit 101) x;
  select count(distinct id) into v_count from unnest(p_work_ids) id;
  if v_count+coalesce(cardinality(p_case_ids),0)>100 then
    raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REFRESH_LINKED_ENVELOPE_EXCEEDED';end if;
  for v_candidate in select x.candidate_id from (
    select w.candidate_id from private.bpay_next_work w where w.id=any(p_work_ids)
    union select c.candidate_id from private.bpay_next_finance_case c where c.id=any(p_case_ids)) x order by x.candidate_id loop
    perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CANDIDATE_CONTROL_MISSING';end if;
  end loop;
  perform 1 from private.bpay_next_work w where w.id=any(p_work_ids) order by w.id for update;
  perform 1 from private.bpay_next_finance_case c where c.id=any(p_case_ids) order by c.id for update;
  for v_candidate in select x.candidate_id from (
    select w.candidate_id from private.bpay_next_work w where w.id=any(p_work_ids)
    union select c.candidate_id from private.bpay_next_finance_case c where c.id=any(p_case_ids)) x order by x.candidate_id loop
    perform private.bpay_next_workbench_ensure_metadata_v1(v_candidate);
  end loop;
  -- A WORK WAIT transition already refreshes its exact origin Cases.
  -- Snapshot their projection revisions so the wrapper never replaces twice.
  select array_agg(coalesce(g.projection_revision,0) order by x.id) into v_case_before
    from unnest(p_case_ids) x(id) left join private.bpay_next_workbench_group g on g.group_key='CASE:'||x.id::text;
  for v_id in select distinct x from unnest(p_work_ids) x order by x loop perform private.bpay_next_workbench_refresh_work_v1(v_id);end loop;
  for v_id in select distinct x from unnest(p_case_ids) x order by x loop
    v_case_index:=v_case_index+1;
    if coalesce((select g.projection_revision from private.bpay_next_workbench_group g
      where g.group_key='CASE:'||v_id::text),0)=v_case_before[v_case_index] then
      perform private.bpay_next_workbench_refresh_case_v1(v_id);
    end if;
  end loop;
end
$function$;


create or replace function private.bpay_next_publish_staged_revision_v1(
  p_work_id uuid, p_revision_id uuid, p_command_id uuid
) returns bigint
language plpgsql
set search_path = pg_catalog, private
as $function$
declare
  v_work private.bpay_next_work%rowtype;
  v_candidate uuid;
  v_revision private.bpay_next_work_revision%rowtype;
  v_predecessor uuid;
  v_sequence bigint;
  v_existing_work uuid;
  v_existing_revision uuid;
  v_physical_booking_id text;
  v_physical_version integer;
  v_physical_current boolean;
  v_physical_archived_at timestamptz;
  v_physical_revoked_at timestamptz;
begin
  if p_work_id is null or p_revision_id is null or p_command_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_PUBLICATION_INPUT_INVALID';
  end if;
  if (select active_owner from private.bpay_next_module_control
        where id=1 for share) <> 'NEXT' then
    raise exception using errcode='55000', message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- Immutable command replay stays a read before new Candidate metadata.
  select p.work_id,p.revision_id,c.agency_sequence into v_existing_work,v_existing_revision,v_sequence
    from private.bpay_next_publication p join private.bpay_next_command c on c.id=p.command_id
    where p.command_id=p_command_id;
  if found then
    if (v_existing_work,v_existing_revision) is distinct from (p_work_id,p_revision_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_PUBLICATION_REPLAY_CONFLICT';
    end if;
    return v_sequence;
  end if;
  select w.candidate_id into strict v_candidate from private.bpay_next_work w where w.id=p_work_id;
  insert into private.bpay_next_worker_control(candidate_id) values(v_candidate) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select * into strict v_work from private.bpay_next_work
    where id=p_work_id for update;
  select p.work_id,p.revision_id,c.agency_sequence
    into v_existing_work,v_existing_revision,v_sequence
    from private.bpay_next_publication p
    join private.bpay_next_command c on c.id=p.command_id
    where p.command_id=p_command_id;
  if found then
    if (v_existing_work,v_existing_revision) is distinct from
       (p_work_id,p_revision_id) then
      raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_REPLAY_CONFLICT';
    end if;
    return v_sequence;
  end if;
  select * into strict v_revision from private.bpay_next_work_revision
    where id=p_revision_id and work_id=p_work_id for update;
  select t.booking_id,t.version,t.is_current,t.archived_at_utc,t.revoked_at
    into v_physical_booking_id,v_physical_version,v_physical_current,
         v_physical_archived_at,v_physical_revoked_at
    from public.timesheets t where t.timesheet_id=v_revision.physical_timesheet_id;
  if v_revision.sealed_at_utc is not null
     or v_revision.revision_no <> v_work.current_revision_no+1
     or v_physical_booking_id is distinct from v_work.booking_id
     or v_physical_version is distinct from v_revision.physical_timesheet_version
     or v_physical_current is distinct from true
     or v_physical_archived_at is not null
     or v_physical_revoked_at is not null
     or v_revision.week_ending_date <> v_work.week_ending_date then
    raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_REVISION_MISMATCH';
  end if;
  -- An approved immutable work revision retains this physical Timesheet.
  -- Tell the installed bounded removal preview that Archive, not permanent
  -- Delete, is the available lifecycle route. One exact identity is marked;
  -- there is no version-family or Candidate-history scan.
  perform public.timesheet_financial_retention_mark_v1(
    array[v_revision.physical_timesheet_id]);
  select revision_id into v_predecessor
    from private.bpay_next_publication
    where work_id=p_work_id order by revision_no desc limit 1;
  if v_work.current_revision_no>0 and v_predecessor is null then
    raise exception using errcode='23514', message='BPAY_NEXT_PUBLICATION_PREDECESSOR_MISSING';
  end if;
  update private.bpay_next_work_revision
    set approved_at_utc=pg_catalog.transaction_timestamp(),
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_revision_id;
  update private.bpay_next_work
    set current_revision_id=p_revision_id,
        current_revision_no=v_revision.revision_no,
        approval_state='APPROVED',
        updated_at_utc=pg_catalog.transaction_timestamp()
    where id=p_work_id;

  -- Same-transaction finite projection barrier precedes the command clock.
  perform private.bpay_next_workbench_mark_processing_v1(p_work_id);

  -- Clock receipt is deliberately last in the upstream lock chain. Failure
  -- rolls back the pointer, sealed revision, member and command together.
  v_sequence := private.bpay_next_receive_command_v1(p_command_id,'POSITION_APPLY');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values (p_command_id,v_work.candidate_id,1);
  insert into private.bpay_next_publication
    (command_id,work_id,candidate_id,revision_id,predecessor_revision_id,revision_no)
    values (p_command_id,p_work_id,v_work.candidate_id,p_revision_id,
            v_predecessor,v_revision.revision_no);
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return v_sequence;
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
      update private.bpay_next_publication
        set status='APPLYING',cursor_key=v_cursor
        where command_id=v_publication.command_id;
      update private.bpay_next_job set cursor_key=v_cursor where id=p_job_id;
      next_phase:='REMOVED'; next_cursor:=v_cursor;
    end if;
  else
    raise exception using errcode='23514', message='BPAY_NEXT_POSITION_PHASE_INVALID';
  end if;
  if v_seen>0 or next_phase='DONE' then
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

alter function private.bpay_next_workbench_mark_processing_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_mark_processing_v1(uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[]) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_many_v1(uuid[],uuid[]) from public,anon,authenticated,service_role;

alter function private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;

alter function private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer) owner to postgres;
revoke all on function private.bpay_next_apply_position_page_v1(uuid,uuid,bigint,text,text,integer) from public,anon,authenticated,service_role;

notify pgrst, 'reload schema';
commit;
