-- Owner-only execution preparation for a no-case PAYE Candidate transfer.
-- This copies current bank details, but deliberately does not issue a file,
-- approve execution or change any frozen financial figure. CSV issue must
-- check the current Candidate account again against this snapshot.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_bind_simple_csv_destination_v1(
  p_transfer_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_candidate public.candidates%rowtype;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_name text;
  v_sort text;
  v_account text;
  v_hash text;
  v_replay boolean;
begin
  if p_transfer_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_CSV_DESTINATION_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select w.run_id into strict v_run_id
    from private.bpay_next_transfer t
    join private.bpay_next_run_worker w on w.id=t.run_worker_id
    where t.id=p_transfer_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    join private.bpay_next_transfer t on t.run_worker_id=w.id
    where t.id=p_transfer_id for update of w;
  select * into strict v_transfer from private.bpay_next_transfer
    where id=p_transfer_id for update;
  v_leg:=private.bpay_next_destination_execution_leg_v1(p_transfer_id);
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or (v_worker.status<>'READY' and not (v_worker.status='COMPLETE'
       and v_leg.transfer_id is not null and v_transfer.original_transfer_id is not null))
     or v_worker.target_pay_channel<>'PAYE'
     or v_transfer.status<>'MEMBERS_READY'
     or v_transfer.candidate_id<>v_worker.candidate_id
     or v_transfer.beneficiary_kind<>'CANDIDATE'
     or v_transfer.beneficiary_id<>v_worker.candidate_id
     or (v_transfer.projection_id is null and v_transfer.return_cash_id is null)
     or v_transfer.cash_amount<=0
     or v_transfer.member_count<=0
     or v_transfer.member_cash_sum<>v_transfer.cash_amount
     or (not exists(select 1 from private.bpay_next_command c
                   where c.id=v_transfer.build_command_id
                     and c.command_kind in ('TRANSFER_BUILD','CASH_REISSUE')
                     and c.status='COMPLETE')
       and not (v_leg.transfer_id is not null and v_transfer.original_transfer_id is null)) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_DESTINATION_NOT_ELIGIBLE';
  end if;
  select * into strict v_candidate from public.candidates
    where id=v_worker.candidate_id;
  v_name:=nullif(pg_catalog.btrim(v_candidate.account_holder),'');
  v_sort:=pg_catalog.regexp_replace(
    coalesce(v_candidate.sort_code,''),'[^0-9]','','g');
  v_account:=pg_catalog.regexp_replace(
    coalesce(v_candidate.account_number,''),'[^0-9]','','g');
  v_hash:=public._bank_hash(v_candidate.sort_code,
    v_candidate.account_number,v_candidate.account_holder);
  if v_leg.leg_kind='ONEOFF' then
    select o.* into strict v_origin from private.bpay_next_net_destination_amount d
      join private.bpay_next_stored_credit_origin o on o.command_id=d.first_origin_command_id
      where d.destination_id=v_leg.destination_id;
    v_name:=nullif(pg_catalog.btrim(v_origin.beneficiary_name),'');
    v_sort:=v_origin.sort_code;v_account:=v_origin.account_number;v_hash:=v_origin.bank_details_hash;
  end if;
  if v_candidate.active is not true
     or pg_catalog.upper(coalesce(v_candidate.pay_method,''))<>'PAYE'
     or v_name is null
     or v_sort !~ '^[0-9]{6}$'
     or v_account !~ '^[0-9]{8}$'
     or v_hash is null
     or (v_leg.leg_kind is distinct from 'ONEOFF' and v_hash is distinct from v_candidate.bank_details_hash) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_CSV_BANK_DETAILS_NOT_READY';
  end if;
  v_replay:=coalesce(v_transfer.destination_rail='CSV'
    and v_transfer.beneficiary_name_snapshot=v_name
    and v_transfer.sort_code_snapshot=v_sort
    and v_transfer.account_number_snapshot=v_account
    and v_transfer.bank_details_hash_snapshot=v_hash,false);
  if not v_replay then
    update private.bpay_next_transfer
      set destination_rail='CSV',beneficiary_name_snapshot=v_name,
          sort_code_snapshot=v_sort,account_number_snapshot=v_account,
          bank_details_hash_snapshot=v_hash
      where id=p_transfer_id;
  end if;
  return pg_catalog.jsonb_build_object('transfer_id',p_transfer_id,
    'phase','MEMBERS_READY','destination_rail','CSV',
    'bank_hash',v_hash,'replay',v_replay,
    'instruction_issued',false);
end
$function$;

alter function private.bpay_next_bind_simple_csv_destination_v1(uuid)
  owner to postgres;
revoke all on function private.bpay_next_bind_simple_csv_destination_v1(uuid)
  from public,anon,authenticated,service_role;

commit;
