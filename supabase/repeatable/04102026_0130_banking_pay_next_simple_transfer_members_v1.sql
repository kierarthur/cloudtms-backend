-- Owner-only, cursor-bounded Candidate transfer membership. A completed
-- member list still has no approved bank instruction and cannot be issued.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_claim_simple_transfer_job_v1(
  p_job_id uuid,p_lease_seconds integer default 120
) returns table(lease_nonce uuid,owner_epoch bigint)
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;
  v_candidate_id uuid;
  v_job private.bpay_next_job%rowtype;
  v_owner_epoch bigint;
  v_nonce uuid;
begin
  if p_job_id is null or p_lease_seconds is null
     or p_lease_seconds not between 1 and 120 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_LEASE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select candidate_id into strict v_candidate_id from private.bpay_next_job
    where id=p_job_id;
  insert into private.bpay_next_worker_control(candidate_id)
    values(v_candidate_id) on conflict(candidate_id) do nothing;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  if v_job.module_epoch<>v_epoch or v_job.job_kind<>'TRANSFER_BUILD'
     or v_job.status not in ('READY','LEASED')
     or (v_job.status='LEASED'
         and v_job.lease_until_utc>pg_catalog.clock_timestamp())
     or exists(select 1 from private.bpay_next_job earlier
               where earlier.candidate_id=v_candidate_id
                 and earlier.command_sequence<v_job.command_sequence
                 and earlier.status<>'DONE') then
    raise exception using errcode='55000',
      message='BPAY_NEXT_TRANSFER_JOB_NOT_CLAIMABLE';
  end if;
  update private.bpay_next_worker_control
    set active_owner_epoch=active_owner_epoch+1,
        updated_at_utc=pg_catalog.transaction_timestamp()
    where candidate_id=v_candidate_id
    returning active_owner_epoch into v_owner_epoch;
  v_nonce:=pg_catalog.gen_random_uuid();
  update private.bpay_next_job
    set status='LEASED',owner_epoch=v_owner_epoch,
        lease_nonce=v_nonce,
        lease_until_utc=pg_catalog.clock_timestamp()+
          pg_catalog.make_interval(secs=>p_lease_seconds),
        attempt_count=attempt_count+1
    where id=p_job_id;
  lease_nonce:=v_nonce; owner_epoch:=v_owner_epoch;
  return next;
end
$function$;

create or replace function private.bpay_next_build_simple_transfer_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint,p_limit integer
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_candidate_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_line record;
  v_seen integer:=0;
  v_member_count bigint;
  v_cash_sum numeric(18,2);
  v_cursor bigint;
  v_adjustment numeric(18,2);
  v_epoch bigint;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null
     or p_limit is null or p_limit not between 1 and 100
     or p_expected_cursor is not null and p_expected_cursor<1 then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_PAGE_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate_id
    from private.bpay_next_job j
    join private.bpay_next_transfer t on t.build_command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  if exists(select 1 from private.bpay_next_destination_group b
     join private.bpay_next_transfer t on t.id=b.anchor_transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id where j.id=p_job_id) then
    return private.bpay_next_build_destination_group_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  if exists(select 1 from private.bpay_next_case_transfer_build b
     join private.bpay_next_transfer t on t.id=b.transfer_id
     join private.bpay_next_job j on j.command_id=t.build_command_id
     where j.id=p_job_id) then
    return private.bpay_next_build_case_transfer_page_v1(
      p_job_id,p_lease_nonce,p_owner_epoch,p_expected_cursor,p_limit);
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control
    where candidate_id=v_candidate_id for update;
  select * into strict v_job from private.bpay_next_job
    where id=p_job_id for update;
  select * into strict v_transfer from private.bpay_next_transfer
    where build_command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=v_transfer.run_worker_id for update;
  select * into strict v_projection from private.bpay_next_net_projection
    where id=v_transfer.projection_id;
  if v_job.job_kind<>'TRANSFER_BUILD'
     or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id
     or v_transfer.candidate_id<>v_worker.candidate_id
     or v_transfer.beneficiary_kind<>'CANDIDATE'
     or v_transfer.beneficiary_id<>v_worker.candidate_id
     or v_run.id<>v_worker.run_id
     or v_projection.run_worker_id<>v_worker.id then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_JOB_SCOPE_INVALID';
  end if;
  if v_job.status='DONE' and v_job.phase='MEMBERS_READY'
     and v_transfer.status<>'BUILDING' then
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_status',v_transfer.status,
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'WORK'
     or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or v_worker.status<>'READY'
     or v_worker.net_projection_revision<>1
     or v_projection.projection_no<>1
     or v_projection.cash_amount<>v_transfer.cash_amount
     or v_transfer.status<>'BUILDING'
     or v_transfer.account_approval_ref is not null
     or v_transfer.original_transfer_id is not null
     or v_transfer.member_count>v_worker.captured_line_count then
    raise exception using errcode='55000',
      message='BPAY_NEXT_TRANSFER_BUILD_NOT_ELIGIBLE';
  end if;
  if v_job.cursor_key is distinct from p_expected_cursor::text then
    return pg_catalog.jsonb_build_object('phase','BUILDING',
      'transfer_id',v_transfer.id,'cursor',v_job.cursor_key,
      'member_count',v_transfer.member_count,'replay',true);
  end if;
  v_member_count:=v_transfer.member_count;
  v_cash_sum:=v_transfer.member_cash_sum;
  v_cursor:=p_expected_cursor;
  for v_line in
    select l.id,l.line_no,l.frozen_inc_vat
    from private.bpay_next_run_line l
    where l.run_worker_id=v_worker.id
      and (v_cursor is null or l.line_no>v_cursor)
    order by l.line_no,l.id limit p_limit
  loop
    v_seen:=v_seen+1;
    v_member_count:=v_member_count+1;
    v_cash_sum:=v_cash_sum+v_line.frozen_inc_vat;
    insert into private.bpay_next_transfer_member
      (transfer_id,run_worker_id,member_no,subject_kind,
       run_line_id,signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_member_count,'WORK',
             v_line.id,v_line.frozen_inc_vat);
    v_cursor:=v_line.line_no;
  end loop;
  if v_member_count>v_worker.captured_line_count then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_LINE_COUNT_EXCEEDED';
  end if;
  if v_member_count=v_worker.captured_line_count then
    if exists(select 1 from private.bpay_next_run_line l
              where l.run_worker_id=v_worker.id
                and (v_cursor is null or l.line_no>v_cursor))
       or v_cash_sum<>v_worker.gross_inc_vat then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_FROZEN_GROSS_MISMATCH';
    end if;
    v_adjustment:=v_projection.cash_amount-v_worker.gross_inc_vat;
    insert into private.bpay_next_transfer_member
      (transfer_id,run_worker_id,member_no,subject_kind,
       signed_cash_contribution)
      values(v_transfer.id,v_worker.id,v_member_count+1,
             'NET_ADJUSTMENT',v_adjustment);
    v_member_count:=v_member_count+1;
    v_cash_sum:=v_cash_sum+v_adjustment;
    if v_cash_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_CASH_MISMATCH';
    end if;
    update private.bpay_next_transfer
      set member_count=v_member_count,member_cash_sum=v_cash_sum,
          status='MEMBERS_READY'
      where id=v_transfer.id;
    update private.bpay_next_job
      set status='DONE',phase='MEMBERS_READY',
          cursor_key=v_cursor::text,
          lease_nonce=null,lease_until_utc=null
      where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE'
      where id=v_job.command_id;
    return pg_catalog.jsonb_build_object('phase','MEMBERS_READY',
      'transfer_id',v_transfer.id,'cursor',v_cursor,
      'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
  end if;
  if v_seen=0 then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_FROZEN_LINE_MISSING';
  end if;
  update private.bpay_next_transfer
    set member_count=v_member_count,member_cash_sum=v_cash_sum
    where id=v_transfer.id;
  update private.bpay_next_job set cursor_key=v_cursor::text
    where id=p_job_id;
  return pg_catalog.jsonb_build_object('phase','BUILDING',
    'transfer_id',v_transfer.id,'cursor',v_cursor,
    'member_count',v_member_count,'rows_visited',v_seen,'replay',false);
end
$function$;

alter function private.bpay_next_claim_simple_transfer_job_v1(uuid,integer)
  owner to postgres;
alter function private.bpay_next_build_simple_transfer_page_v1(
  uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_claim_simple_transfer_job_v1(uuid,integer),
  private.bpay_next_build_simple_transfer_page_v1(
    uuid,uuid,bigint,bigint,integer)
  from public,anon,authenticated,service_role;

commit;
