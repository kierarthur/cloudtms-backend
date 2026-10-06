-- Narrow internal PAYE-net intake for an already frozen, no-case Draft.
-- This records an ordered request only. It makes no projection, transfer,
-- CSV instruction or financial effect and grants no browser/service access.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_accept_simple_paye_net_v1(
  p_command_id uuid,p_run_worker_id uuid,p_entered_paye_net numeric
) returns jsonb
language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_run_id uuid;
  v_run private.bpay_next_pay_run%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_prior private.bpay_next_paye_net_request%rowtype;
  v_sequence bigint;
begin
  if p_command_id is null or p_run_worker_id is null
     or p_entered_paye_net is null
     or p_entered_paye_net<0
     or p_entered_paye_net<>pg_catalog.round(p_entered_paye_net,2) then
    raise exception using errcode='22023',message='BPAY_NEXT_PAYE_NET_INPUT_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control
                where id=1 and active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;

  -- The immutable worker identity locates its parent; lock the shared run
  -- header before this worker, as confirmation/cancellation owners do.
  select run_id into strict v_run_id from private.bpay_next_run_worker
    where id=p_run_worker_id;
  select * into strict v_run from private.bpay_next_pay_run
    where id=v_run_id for update;
  select * into strict v_worker from private.bpay_next_run_worker
    where id=p_run_worker_id for update;

  -- Exact retry wins even if an ensuing owner has changed the run state.
  select * into v_prior from private.bpay_next_paye_net_request
    where command_id=p_command_id;
  if found then
    if v_prior.run_worker_id<>p_run_worker_id
       or v_prior.candidate_id<>v_worker.candidate_id
       or v_prior.case_draft_state_id is not null
       or v_prior.entered_paye_net<>p_entered_paye_net then
      raise exception using errcode='23514',message='BPAY_NEXT_PAYE_NET_REPLAY_CONFLICT';
    end if;
    select agency_sequence into strict v_sequence from private.bpay_next_command
      where id=p_command_id and command_kind='PAYE_NET_ENTRY';
    return pg_catalog.jsonb_build_object('sequence',v_sequence,
      'request_no',v_prior.request_no,'replay',true,
      'phase','ACCEPTED_PENDING_PROJECTION');
  end if;

  if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null
     or exists(select 1 from private.bpay_next_cancel_request r
               where r.run_worker_id=v_worker.id and r.status in ('REQUESTED','CANCELLING'))
     or v_worker.status<>'READY' or v_worker.target_pay_channel<>'PAYE'
     or v_worker.gross_vat<>0
     or v_worker.gross_inc_vat<0
     or p_entered_paye_net>v_worker.gross_inc_vat
     or v_worker.net_request_revision<>0
     or v_worker.net_projection_revision<>0
     or v_worker.case_selection_revision<>0
     or exists(select 1 from private.bpay_next_transfer t
               where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',
      message='BPAY_NEXT_SIMPLE_PAYE_NET_NOT_ELIGIBLE';
  end if;

  v_sequence:=private.bpay_next_receive_command_v1(
    p_command_id,'PAYE_NET_ENTRY');
  insert into private.bpay_next_command_member
    (command_id,candidate_id,member_no)
    values(p_command_id,v_worker.candidate_id,1);
  insert into private.bpay_next_paye_net_request
    (command_id,run_worker_id,candidate_id,request_no,
     entered_paye_net,frozen_gross_inc_vat)
    values(p_command_id,v_worker.id,v_worker.candidate_id,1,
           p_entered_paye_net,v_worker.gross_inc_vat);
  update private.bpay_next_run_worker
    set net_request_revision=1 where id=v_worker.id;
  update private.bpay_next_command
    set expected_member_count=1,status='SEALED',
        sealed_at_utc=pg_catalog.transaction_timestamp()
    where id=p_command_id;
  return pg_catalog.jsonb_build_object('sequence',v_sequence,
    'request_no',1,'replay',false,
    'phase','ACCEPTED_PENDING_PROJECTION');
end
$function$;

alter function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric)
  owner to postgres;
revoke all on function private.bpay_next_accept_simple_paye_net_v1(uuid,uuid,numeric)
  from public,anon,authenticated,service_role;

commit;
