-- One-transfer guards: member rows can be appended only while their exact
-- transfer is BUILDING. A completed member can never be repriced or removed.
-- Once a CSV/external instruction exists, frozen transfer identity, money and
-- destination cannot be changed by a later status update.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_transfer_member_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_status text;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_MEMBER_IMMUTABLE';
  end if;
  select t.status into strict v_status
    from private.bpay_next_transfer t
    where t.id=new.transfer_id for share;
  if v_status<>'BUILDING' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_MEMBER_AFTER_BUILD';
  end if;
  return new;
end
$function$;

create or replace function private.bpay_next_transfer_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  if tg_op='DELETE' then
    raise exception using errcode='23514',
      message='BPAY_NEXT_TRANSFER_DELETE_FORBIDDEN';
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

create or replace function private.bpay_next_csv_instruction_immutable_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
begin
  raise exception using errcode='23514',
    message='BPAY_NEXT_CSV_INSTRUCTION_IMMUTABLE';
end
$function$;

-- External evidence cannot be manufactured for a non-bank execution, even
-- through an owner-only INSERT that bypasses the public intake.
create or replace function private.bpay_next_bank_evidence_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_kind text;
begin
  select execution_kind into strict v_kind from private.bpay_next_transfer where id=new.transfer_id for share;
  if v_kind<>'BANK' then raise exception using errcode='23514',message='BPAY_NEXT_INTERNAL_EXTERNAL_EVIDENCE_FORBIDDEN';end if;
  return new;
end
$function$;

drop trigger if exists bpay_next_transfer_member_guard_v1
  on private.bpay_next_transfer_member;
create trigger bpay_next_transfer_member_guard_v1
before insert or update or delete on private.bpay_next_transfer_member
for each row execute function private.bpay_next_transfer_member_guard_v1();
drop trigger if exists bpay_next_transfer_guard_v1
  on private.bpay_next_transfer;
create trigger bpay_next_transfer_guard_v1
before update or delete on private.bpay_next_transfer
for each row execute function private.bpay_next_transfer_guard_v1();
drop trigger if exists bpay_next_csv_instruction_immutable_v1
  on private.bpay_next_csv_instruction;
create trigger bpay_next_csv_instruction_immutable_v1
before update or delete on private.bpay_next_csv_instruction
for each row execute function private.bpay_next_csv_instruction_immutable_v1();

drop trigger if exists bpay_next_csv_bank_evidence_guard_v1 on private.bpay_next_csv_instruction;
create trigger bpay_next_csv_bank_evidence_guard_v1 before insert on private.bpay_next_csv_instruction
  for each row execute function private.bpay_next_bank_evidence_guard_v1();
drop trigger if exists bpay_next_outcome_bank_evidence_guard_v1 on private.bpay_next_transfer_outcome;
create trigger bpay_next_outcome_bank_evidence_guard_v1 before insert on private.bpay_next_transfer_outcome
  for each row execute function private.bpay_next_bank_evidence_guard_v1();

alter function private.bpay_next_transfer_member_guard_v1()
  owner to postgres;
alter function private.bpay_next_transfer_guard_v1()
  owner to postgres;
alter function private.bpay_next_csv_instruction_immutable_v1()
  owner to postgres;
alter function private.bpay_next_bank_evidence_guard_v1() owner to postgres;
revoke all on function private.bpay_next_transfer_member_guard_v1(),
  private.bpay_next_transfer_guard_v1(),
  private.bpay_next_bank_evidence_guard_v1(),
  private.bpay_next_csv_instruction_immutable_v1()
  from public,anon,authenticated,service_role;

commit;
