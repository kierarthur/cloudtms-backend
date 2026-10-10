-- PS-C01 successor of final2018 BUILD (historical0130). CLAIM reused unchanged.
-- Only ordinary Umbrella WORK bridge capture; Case/destination owners unchanged.
\set ON_ERROR_STOP on
begin;
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
     or not ((v_worker.target_pay_channel='PAYE' and
          v_transfer.beneficiary_kind='CANDIDATE' and v_transfer.beneficiary_id=v_worker.candidate_id)
       or (v_worker.target_pay_channel='UMBRELLA' and v_worker.target_umbrella_id is not null and
          v_transfer.beneficiary_kind='UMBRELLA' and v_transfer.beneficiary_id=v_worker.target_umbrella_id))
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
    select l.id,l.line_no,l.frozen_inc_vat,l.frozen_ex_vat,l.source_consumed_ex_vat,
      l.source_pay_channel,l.target_pay_channel,l.component_key,l.work_id,
      a.id as qualified_line_id,a.component_kind,a.tax_treatment,
      r.id as qualified_revision_id
    from private.bpay_next_run_line l
    left join private.bpay_next_approved_line a on a.id=l.approved_line_id
      and a.revision_id=l.captured_revision_id and a.component_key=l.component_key
    left join private.bpay_next_work_revision r on r.id=l.captured_revision_id and r.work_id=l.work_id
    where l.run_worker_id=v_worker.id
      and (v_cursor is null or l.line_no>v_cursor)
    order by l.line_no,l.id limit p_limit
  loop
    if v_worker.target_pay_channel='UMBRELLA' and
       (v_projection.input_kind is distinct from 'UMBRELLA'
        or v_worker.case_selection_revision<>0 or v_worker.net_request_revision<>0
        or v_projection.entered_paye_net is not null or v_projection.request_command_id is not null
        or v_projection.cash_amount is distinct from v_worker.gross_inc_vat
        or v_line.qualified_line_id is null or v_line.qualified_revision_id is null
        or v_line.component_kind is distinct from 'WORK' or v_line.tax_treatment is distinct from 'TAXABLE'
        or v_line.source_pay_channel is distinct from 'UMBRELLA'
        or v_line.target_pay_channel is distinct from 'UMBRELLA'
        or v_line.source_consumed_ex_vat<0
        or v_line.frozen_ex_vat is distinct from v_line.source_consumed_ex_vat) then
      raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
    end if;
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
    if v_worker.target_pay_channel='PAYE' then
      v_adjustment:=v_projection.cash_amount-v_worker.gross_inc_vat;
      insert into private.bpay_next_transfer_member
        (transfer_id,run_worker_id,member_no,subject_kind,
         signed_cash_contribution)
        values(v_transfer.id,v_worker.id,v_member_count+1,
               'NET_ADJUSTMENT',v_adjustment);
      v_member_count:=v_member_count+1;
      v_cash_sum:=v_cash_sum+v_adjustment;
    end if;
    if v_cash_sum<>v_projection.cash_amount then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_CASH_MISMATCH';
    end if;
    if v_worker.target_pay_channel='UMBRELLA' then
      if v_projection.payment_case_target_inc_vat<>0
         or v_projection.deduction_case_target_inc_vat<>0 then
        raise exception using errcode='55000',message='BPAY_NEXT_UMBRELLA_WORK_EXECUTION_UNSUPPORTED';
      end if;
      update private.bpay_next_transfer
        set umbrella_bridge_kind=v_projection.input_kind,
            umbrella_gross_ex_vat=v_projection.gross_ex_vat,
            umbrella_gross_vat=v_projection.gross_vat,
            umbrella_gross_inc_vat=v_projection.gross_inc_vat,
            umbrella_payment_case_target_inc_vat=v_projection.payment_case_target_inc_vat,
            umbrella_deduction_case_target_inc_vat=v_projection.deduction_case_target_inc_vat
        where id=v_transfer.id;
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
ALTER FUNCTION private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer) FROM PUBLIC,anon,authenticated,service_role;
-- Minimal0145 support: captured bridge cannot be cleared on cancellation.
create or replace function private.bpay_next_transfer_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_DELETE_FORBIDDEN';
  end if;
  if old.umbrella_bridge_kind is not null
     and (new.umbrella_bridge_kind,new.umbrella_gross_ex_vat,new.umbrella_gross_vat,
          new.umbrella_gross_inc_vat,new.umbrella_payment_case_target_inc_vat,
          new.umbrella_deduction_case_target_inc_vat)
       is distinct from
         (old.umbrella_bridge_kind,old.umbrella_gross_ex_vat,old.umbrella_gross_vat,
          old.umbrella_gross_inc_vat,old.umbrella_payment_case_target_inc_vat,
          old.umbrella_deduction_case_target_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_UMBRELLA_BRIDGE_IMMUTABLE';
  end if;
  if new.execution_kind is distinct from old.execution_kind then
    raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_EXECUTION_KIND_IMMUTABLE';
  end if;
  if old.status='BUILDING' then
    if new.id is distinct from old.id
       or new.run_worker_id is distinct from old.run_worker_id
       or new.candidate_id is distinct from old.candidate_id
       or new.projection_id is distinct from old.projection_id
       or new.build_command_id is distinct from old.build_command_id
       or new.transfer_no is distinct from old.transfer_no
       or new.beneficiary_kind is distinct from old.beneficiary_kind
       or new.beneficiary_id is distinct from old.beneficiary_id
       or new.cash_amount is distinct from old.cash_amount
       or new.currency is distinct from old.currency
       or new.original_transfer_id is distinct from old.original_transfer_id
       or new.return_cash_id is distinct from old.return_cash_id
       or new.created_at_utc is distinct from old.created_at_utc
       or new.member_count<old.member_count
       or new.status not in ('BUILDING','MEMBERS_READY') then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BUILD_IMMUTABLE';
    end if;
    return new;
  end if;
  if old.status='MEMBERS_READY' then
    if new.id is distinct from old.id
       or new.run_worker_id is distinct from old.run_worker_id
       or new.candidate_id is distinct from old.candidate_id
       or new.projection_id is distinct from old.projection_id
       or new.build_command_id is distinct from old.build_command_id
       or new.transfer_no is distinct from old.transfer_no
       or new.beneficiary_kind is distinct from old.beneficiary_kind
       or new.beneficiary_id is distinct from old.beneficiary_id
       or new.cash_amount is distinct from old.cash_amount
       or new.currency is distinct from old.currency
       or new.member_count is distinct from old.member_count
       or new.member_cash_sum is distinct from old.member_cash_sum
       or new.original_transfer_id is distinct from old.original_transfer_id
       or new.return_cash_id is distinct from old.return_cash_id
       or new.created_at_utc is distinct from old.created_at_utc
       or (old.execution_kind='BANK' and new.status not in
           ('MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV','CANCELLED'))
       or (old.execution_kind='INTERNAL_ZERO' and new.status not in
           ('MEMBERS_READY','INTERNAL_PROCESSING','CANCELLED')) then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_READY_MONEY_IMMUTABLE';
    end if;
    if old.execution_kind='INTERNAL_ZERO' and new.status='INTERNAL_PROCESSING'
       and not exists(select 1 from private.bpay_next_internal_receipt i
         join private.bpay_next_outcome_request r on r.internal_receipt_id=i.id
         where i.transfer_id=old.id and r.command_id=i.command_id
           and r.transfer_id=old.id and r.posted_member_count=0 and not r.posting_complete) then
      raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_INTERNAL_RECEIPT_REQUIRED';
    end if;
    return new;
  end if;
  if (pg_catalog.to_jsonb(new)-'status')
     is distinct from (pg_catalog.to_jsonb(old)-'status') then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_ISSUED_IMMUTABLE';
  end if;
  if old.execution_kind='INTERNAL_ZERO' then
    if (old.status='INTERNAL_PROCESSING' and new.status not in ('INTERNAL_PROCESSING','INTERNAL_SETTLED'))
       or (old.status in ('INTERNAL_SETTLED','CANCELLED') and new.status<>old.status)
       or (new.status='INTERNAL_SETTLED' and not exists(
         select 1 from private.bpay_next_outcome_request r
           join private.bpay_next_internal_receipt i on i.id=r.internal_receipt_id
         where r.transfer_id=old.id and r.command_id=i.command_id and r.outcome_id is null
           and r.posting_complete and r.posted_member_count=old.member_count)) then
      raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_INTERNAL_STATE_PROTECTED';
    end if;
    return new;
  end if;
  if (old.status='ISSUED_CSV'
        and new.status not in ('ISSUED_CSV','UNKNOWN','SETTLED','REFUSED'))
     or (old.status='UNKNOWN'
        and new.status not in ('UNKNOWN','SETTLED','REFUSED'))
     or (old.status='SETTLED'
        and new.status not in ('SETTLED','RETURNED'))
     or (old.status in ('RETURNED','REFUSED','CANCELLED')
        and new.status<>old.status) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_TERMINAL_STATE_PROTECTED';
  end if;
  return new;
end
$function$;
ALTER FUNCTION private.bpay_next_transfer_guard_v1() OWNER TO postgres;
REVOKE ALL ON FUNCTION private.bpay_next_transfer_guard_v1() FROM PUBLIC,anon,authenticated,service_role;
commit;
