-- PS-03: retain append-only/BUILDING guard and qualify ordinary stored origins.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_transfer_member_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_transfer record;
  v_line record;
  v_projection record;
  v_request record;
  v_reissue record;
  v_cash record;
  v_original record;
  v_destination private.bpay_next_case_transfer_build%rowtype;
begin
  if tg_op<>'INSERT' then
    raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_MEMBER_IMMUTABLE';
  end if;
  select id,status,run_worker_id,candidate_id,original_transfer_id,projection_id,return_cash_id,build_command_id,cash_amount into strict v_transfer from private.bpay_next_transfer
    where id=new.transfer_id for share;
  if v_transfer.status<>'BUILDING' then
    raise exception using errcode='23514',message='BPAY_NEXT_TRANSFER_MEMBER_AFTER_BUILD';
  end if;
  if new.run_worker_id is distinct from v_transfer.run_worker_id then
    raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_ORIGIN_WORKER_MISMATCH';
  end if;
  -- Those original owners validate their own WORK/CASE/NET amounts and stages.
  if exists(select 1 from private.bpay_next_case_transfer_build where transfer_id=v_transfer.id) then
    return new;
  end if;
  v_destination:=private.bpay_next_destination_build_binding_v1(v_transfer.id);
  if v_destination.transfer_id is not null then return new; end if;
  if new.subject_kind='WORK' then
    select run_worker_id,frozen_inc_vat into strict v_line from private.bpay_next_run_line where id=new.run_line_id;
    if v_transfer.original_transfer_id is not null or v_transfer.projection_id is null
       or v_line.run_worker_id is distinct from v_transfer.run_worker_id
       or new.signed_cash_contribution is distinct from v_line.frozen_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_WORK_ORIGIN_MISMATCH';
    end if;
  elsif new.subject_kind='NET_ADJUSTMENT' then
    select request_command_id,input_kind,run_worker_id,entered_paye_net,gross_inc_vat,cash_amount into v_projection from private.bpay_next_net_projection where id=v_transfer.projection_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_NET_ORIGIN_MISSING';end if;
    select run_worker_id,candidate_id,entered_paye_net,frozen_gross_inc_vat,case_draft_state_id into v_request from private.bpay_next_paye_net_request where command_id=v_projection.request_command_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_NET_REQUEST_MISSING';end if;
    if v_transfer.original_transfer_id is not null or v_projection.input_kind<>'PAYE_MANUAL'
       or v_request.case_draft_state_id is not null
       or v_projection.run_worker_id is distinct from v_transfer.run_worker_id
       or (v_request.run_worker_id,v_request.candidate_id,v_request.entered_paye_net,v_request.frozen_gross_inc_vat)
         is distinct from (v_transfer.run_worker_id,v_transfer.candidate_id,v_projection.entered_paye_net,v_projection.gross_inc_vat)
       or new.signed_cash_contribution is distinct from v_projection.cash_amount-v_projection.gross_inc_vat then
      raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_NET_ORIGIN_MISMATCH';
    end if;
  elsif new.subject_kind='CASH_REISSUE' then
    select id,original_transfer_id,candidate_id into v_cash from private.bpay_next_return_cash where id=v_transfer.return_cash_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_REISSUE_CASH_MISSING';end if;
    select id,run_worker_id,candidate_id into v_original from private.bpay_next_transfer where id=v_transfer.original_transfer_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_REISSUE_PARENT_MISSING';end if;
    select return_cash_id,run_worker_id,candidate_id,transfer_id into v_reissue from private.bpay_next_reissue_request where command_id=v_transfer.build_command_id;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_REISSUE_REQUEST_MISSING';end if;
    if v_transfer.projection_id is not null or new.member_no<>1
       or (v_cash.original_transfer_id,v_cash.candidate_id,v_original.run_worker_id,v_original.candidate_id)
         is distinct from (v_original.id,v_transfer.candidate_id,v_transfer.run_worker_id,v_transfer.candidate_id)
       or (v_reissue.return_cash_id,v_reissue.run_worker_id,v_reissue.candidate_id)
         is distinct from (v_cash.id,v_transfer.run_worker_id,v_transfer.candidate_id)
       or (v_reissue.transfer_id is not null and v_reissue.transfer_id<>v_transfer.id)
       or new.signed_cash_contribution is distinct from v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_REISSUE_ORIGIN_MISMATCH';
    end if;
  else
    raise exception using errcode='23514',message='BPAY_NEXT_MEMBER_ORIGIN_UNBOUND';
  end if;
  return new;
end
$function$;
alter function private.bpay_next_transfer_member_guard_v1() owner to postgres;
revoke all on function private.bpay_next_transfer_member_guard_v1() from public,anon,authenticated,service_role;
commit;
