-- WB-29 / BD155: complete native treatment publication; original earnings retained.
\set ON_ERROR_STOP on
begin;

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
  if v_revision.payment_treatment_approval_revision_id is not null then
    if v_predecessor is distinct from v_work.current_revision_id
      or private.bpay_next_treatment_approval_revision_v1(v_predecessor)
        is distinct from v_revision.payment_treatment_approval_revision_id then
      raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_PREDECESSOR_CHANGED';end if;
    perform private.bpay_next_treatment_payload_assert_v1(p_revision_id);
  end if;
  update private.bpay_next_work_revision
    set approved_at_utc=pg_catalog.transaction_timestamp(),
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_revision_id;
  -- Existing segment seal derives the segments; compare only after that UPDATE.
  if v_revision.payment_treatment_approval_revision_id is not null then
    perform private.bpay_next_treatment_payload_assert_v1(p_revision_id);
  end if;
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


alter function private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_publish_staged_revision_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
commit;
