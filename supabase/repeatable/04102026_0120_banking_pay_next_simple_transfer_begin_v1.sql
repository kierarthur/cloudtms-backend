-- Owner-only handoff from frozen PAYE projection to a non-executable
-- Candidate transfer being built in pages. Bank details bind later.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_begin_simple_transfer_v1(
  p_command_id uuid,p_run_worker_id uuid
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_sequence bigint;
  v_candidate_id uuid;
  v_case_selection_revision bigint;
begin
  if p_command_id is null or p_run_worker_id is null then
    raise exception using errcode='22023',
      message='BPAY_NEXT_TRANSFER_BEGIN_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
                where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  select run_id,candidate_id,case_selection_revision
    into strict v_run_id,v_candidate_id,v_case_selection_revision from private.bpay_next_run_worker
    where id=p_run_worker_id;
  if v_case_selection_revision>0 then
    return private.bpay_next_begin_case_transfer_v1(p_command_id,p_run_worker_id);
  end if;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id for update;
  select * into v_transfer from private.bpay_next_transfer
    where build_command_id=p_command_id;
  if found then
    if v_transfer.run_worker_id<>p_run_worker_id
       or v_transfer.candidate_id<>v_worker.candidate_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_TRANSFER_BEGIN_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence
      from private.bpay_next_command
      where id=p_command_id and command_kind='TRANSFER_BUILD';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'transfer_id',v_transfer.id,'phase',v_transfer.status,'replay',true);
  end if;
  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY'
     or v_worker.target_pay_channel<>'PAYE'
     or v_worker.net_projection_revision<>1
     or v_worker.net_request_revision<>1
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_TRANSFER_NOT_ELIGIBLE';
  end if;
  select * into strict v_projection from private.bpay_next_net_projection
    where run_worker_id=v_worker.id and projection_no=1;
  if v_projection.input_kind<>'PAYE_MANUAL'
     or v_projection.gross_ex_vat<>v_worker.gross_ex_vat
     or v_projection.gross_vat<>0
     or v_projection.gross_inc_vat<>v_worker.gross_inc_vat
     or v_projection.entered_paye_net is distinct from
        v_worker.entered_paye_net
     or v_projection.accepted_recoveries<>0
     or v_projection.cash_amount<0
     or v_projection.cash_amount<>v_worker.entered_paye_net
     or v_projection.request_command_id is null then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SIMPLE_TRANSFER_PROJECTION_INVALID';
  end if;
  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'TRANSFER_BUILD');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_transfer
    (run_worker_id,candidate_id,projection_id,build_command_id,
     transfer_no,beneficiary_kind,beneficiary_id,cash_amount,status,execution_kind)
    values(v_worker.id,v_worker.candidate_id,v_projection.id,p_command_id,
      1,'CANDIDATE',v_worker.candidate_id,v_projection.cash_amount,'BUILDING',
      case when v_projection.cash_amount=0 then 'INTERNAL_ZERO' else 'BANK' end)
    returning * into v_transfer;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'transfer_id',v_transfer.id,'phase','BUILDING','replay',false);
end
$function$;

alter function private.bpay_next_begin_simple_transfer_v1(uuid,uuid)
  owner to postgres;
revoke all on function private.bpay_next_begin_simple_transfer_v1(uuid,uuid)
  from public,anon,authenticated,service_role;

commit;
