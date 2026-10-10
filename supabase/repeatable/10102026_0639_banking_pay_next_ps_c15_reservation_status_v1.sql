-- PS-14: native original obligations, then disjoint original legs. Write-free.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_destination_execution_leg_v1(p_transfer_id uuid)
returns private.bpay_next_destination_group_leg language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_t private.bpay_next_transfer%rowtype;v_o private.bpay_next_transfer%rowtype;
  v_l private.bpay_next_destination_group_leg%rowtype;v_g private.bpay_next_destination_group%rowtype;
  v_n private.bpay_next_net_destination_state%rowtype;v_p private.bpay_next_net_projection%rowtype;
  v_d private.bpay_next_net_destination_amount%rowtype;v_origin private.bpay_next_stored_credit_origin%rowtype;
  v_bank public.pay_finance_case_oneoff_payout_bank_details%rowtype;
begin
  select * into strict v_t from private.bpay_next_transfer where id=p_transfer_id;
  select * into strict v_o from private.bpay_next_transfer where id=coalesce(v_t.original_transfer_id,v_t.id);
  select * into v_l from private.bpay_next_destination_group_leg where transfer_id=v_o.id;
  if not found then
    if exists(select 1 from private.bpay_next_destination_group where run_worker_id=v_o.run_worker_id) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_UNBOUND';end if;
    return null;
  end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_l.anchor_transfer_id;
  select * into strict v_n from private.bpay_next_net_destination_state where state_id=v_g.net_state_id;
  select * into strict v_p from private.bpay_next_net_projection where id=v_g.projection_id;
  if v_g.stage<>'COMPLETE' or v_g.completed_at_utc is null
     or v_g.sealed_leg_count<>v_g.expected_leg_count or v_g.created_leg_count<>v_g.expected_leg_count
     or v_g.sealed_cash_total<>v_p.cash_amount or v_n.projection_id<>v_p.id
     or v_n.run_worker_id<>v_g.run_worker_id or v_p.run_worker_id<>v_g.run_worker_id
     or v_n.completed_at_utc is null or v_n.own_amount+v_n.external_amount<>v_p.cash_amount
     or v_n.cash_amount<>v_p.cash_amount or v_g.expected_leg_count<>v_n.external_leg_count+1
     or v_o.run_worker_id<>v_g.run_worker_id or v_o.candidate_id<>v_g.candidate_id
     or v_o.projection_id<>v_p.id or v_o.original_transfer_id is not null or v_o.return_cash_id is not null
     or v_o.member_cash_sum<>v_o.cash_amount or v_o.member_count<1
     or v_o.beneficiary_kind<>'CANDIDATE' or v_o.beneficiary_id<>v_g.candidate_id
     or v_t.run_worker_id<>v_o.run_worker_id or v_t.candidate_id<>v_o.candidate_id
     or v_t.beneficiary_kind<>v_o.beneficiary_kind or v_t.beneficiary_id<>v_o.beneficiary_id
     or (v_t.original_transfer_id is not null and (v_t.return_cash_id is null or v_t.projection_id is not null))
     or not exists(select 1 from private.bpay_next_transfer a join private.bpay_next_command c on c.id=a.build_command_id
       join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=v_g.candidate_id
       where a.id=v_g.anchor_transfer_id and a.run_worker_id=v_g.run_worker_id and a.projection_id=v_p.id
         and c.command_kind='TRANSFER_BUILD' and c.status='COMPLETE' and j.job_kind='TRANSFER_BUILD'
         and j.status='DONE' and j.phase='MEMBERS_READY') then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_SCOPE_INVALID';end if;
  if v_l.leg_kind='OWN' then
    if v_o.id<>v_g.anchor_transfer_id or v_l.destination_id is not null or v_o.cash_amount<>v_n.own_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_OWN_INVALID';end if;
  else
    select * into strict v_d from private.bpay_next_net_destination_amount where destination_id=v_l.destination_id;
    select * into strict v_origin from private.bpay_next_stored_credit_origin where command_id=v_d.first_origin_command_id;
    select * into strict v_bank from public.pay_finance_case_oneoff_payout_bank_details
      where finance_case_id=v_origin.legacy_case_id and candidate_id=v_origin.candidate_id for share;
    if v_d.state_id<>v_n.state_id or v_d.amount<>v_o.cash_amount or v_o.execution_kind<>'BANK'
       or v_origin.candidate_id<>v_g.candidate_id or v_d.bank_details_hash<>v_origin.bank_details_hash
       or v_origin.original_source_pay_channel<>'UMBRELLA' or v_origin.original_tax_treatment<>'NON_TAXABLE'
       or v_origin.original_routing_kind<>'ONE_OFF_SPECIFIED_BANK_ACCOUNT'
       or (v_bank.updated_at_utc,v_bank.bank_details_hash,v_bank.beneficiary_name,
           pg_catalog.replace(v_bank.sort_code,'-',''),v_bank.account_number)
         is distinct from (v_origin.bank_version_at_utc,v_origin.bank_details_hash,v_origin.beneficiary_name,
           v_origin.sort_code,v_origin.account_number)
       or public._bank_hash(v_bank.sort_code,v_bank.account_number,v_bank.beneficiary_name)
         is distinct from v_origin.bank_details_hash then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_EXECUTION_ONEOFF_INVALID';end if;
  end if;
  return v_l;
end $function$;

create or replace function private.bpay_next_destination_posting_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_g private.bpay_next_destination_group%rowtype;v_r private.bpay_next_destination_posted_leg%rowtype;
begin
  if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_DELETE_FORBIDDEN';end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=new.anchor_transfer_id;
  if v_g.stage<>'COMPLETE' or new.posted_leg_count>v_g.expected_leg_count then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_SCOPE_INVALID';end if;
  if tg_op='INSERT' then
    if new.posted_leg_count<>0 or new.last_posted_transfer_id is not null or new.completed_at_utc is not null then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_INITIAL_INVALID';end if;
  else
    select * into strict v_r from private.bpay_next_destination_posted_leg
      where anchor_transfer_id=new.anchor_transfer_id and original_transfer_id=new.last_posted_transfer_id;
    if old.completed_at_utc is not null or new.anchor_transfer_id<>old.anchor_transfer_id
       or new.posted_leg_count<>old.posted_leg_count+1 or v_r.posting_no<>new.posted_leg_count
       or ((new.posted_leg_count=v_g.expected_leg_count) is distinct from (new.completed_at_utc is not null))
       or (new.completed_at_utc is not null and new.completed_at_utc<>pg_catalog.transaction_timestamp()) then
      raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTING_PROGRESS_INVALID';end if;
  end if;
  return new;
end $function$;

create or replace function private.bpay_next_destination_posted_leg_guard_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_l private.bpay_next_destination_group_leg%rowtype;v_j private.bpay_next_job%rowtype;
  v_t private.bpay_next_transfer%rowtype;v_r private.bpay_next_outcome_request%rowtype;
  v_s private.bpay_next_destination_posting%rowtype;
begin
  if tg_op<>'INSERT' then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_IMMUTABLE';end if;
  v_l:=private.bpay_next_destination_execution_leg_v1(new.original_transfer_id);
  select * into strict v_s from private.bpay_next_destination_posting where anchor_transfer_id=new.anchor_transfer_id;
  select * into strict v_j from private.bpay_next_job where id=new.job_id;
  select * into strict v_r from private.bpay_next_outcome_request where command_id=v_j.command_id;
  select * into strict v_t from private.bpay_next_transfer where id=new.original_transfer_id;
  if v_l.transfer_id is null or v_l.anchor_transfer_id<>new.anchor_transfer_id
     or v_s.completed_at_utc is not null or new.posting_no<>v_s.posted_leg_count+1
     or new.posted_at_utc<>pg_catalog.transaction_timestamp() or v_j.status<>'LEASED' or v_j.phase<>'MEMBERS'
     or v_j.job_kind not in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') or v_j.lease_nonce is null
     or v_j.lease_until_utc<=pg_catalog.clock_timestamp()
     or not exists(select 1 from private.bpay_next_worker_control where candidate_id=v_j.candidate_id and active_owner_epoch=v_j.owner_epoch)
     or v_r.transfer_id<>v_t.id or v_r.run_worker_id<>v_t.run_worker_id or v_r.candidate_id<>v_j.candidate_id
     or v_r.posting_complete is not true or v_r.posted_member_count<>v_t.member_count
     or v_t.original_transfer_id is not null or v_t.return_cash_id is not null
     or (v_j.job_kind='CSV_SETTLEMENT' and (v_t.execution_kind<>'BANK' or v_t.status not in ('SETTLED','RETURNED')
       or v_r.internal_receipt_id is not null or not exists(select 1 from private.bpay_next_transfer_outcome o
         where o.id=v_r.outcome_id and o.transfer_id=v_t.id and o.outcome_kind='SETTLED' and o.whole_transfer_amount=v_t.cash_amount)))
     or (v_j.job_kind='INTERNAL_SETTLEMENT' and (v_l.leg_kind<>'OWN' or v_t.execution_kind<>'INTERNAL_ZERO'
       or v_t.status<>'INTERNAL_SETTLED' or v_t.cash_amount<>0 or v_r.outcome_id is not null
       or not exists(select 1 from private.bpay_next_internal_receipt i where i.id=v_r.internal_receipt_id
         and i.command_id=v_j.command_id and i.transfer_id=v_t.id and i.run_worker_id=v_t.run_worker_id))) then
    raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_SCOPE_INVALID';end if;
  return new;
end $function$;

create or replace function private.bpay_next_complete_destination_leg_v1(p_job_id uuid)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_j private.bpay_next_job%rowtype;v_r private.bpay_next_outcome_request%rowtype;
  v_t private.bpay_next_transfer%rowtype;v_l private.bpay_next_destination_group_leg%rowtype;
  v_g private.bpay_next_destination_group%rowtype;v_s private.bpay_next_destination_posting%rowtype;
  v_old private.bpay_next_destination_posted_leg%rowtype;
begin
  select * into strict v_j from private.bpay_next_job where id=p_job_id;
  select * into strict v_r from private.bpay_next_outcome_request where command_id=v_j.command_id;
  select * into strict v_t from private.bpay_next_transfer where id=v_r.transfer_id;
  -- Reissue cash never posts original payroll/principal twice.
  if v_t.original_transfer_id is not null then return;end if;
  v_l:=private.bpay_next_destination_execution_leg_v1(v_t.id);
  if v_l.transfer_id is null then return;end if;
  select * into strict v_g from private.bpay_next_destination_group where anchor_transfer_id=v_l.anchor_transfer_id;
  select * into v_old from private.bpay_next_destination_posted_leg where original_transfer_id=v_t.id;
  if found then
    if v_old.job_id<>p_job_id then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_POSTED_LEG_CONFLICT';end if;
    return;
  end if;
  -- Caller already holds header -> Candidate -> job -> worker -> exact transfer.
  -- The shared header serialises lazy counter creation; no sibling/history scan.
  insert into private.bpay_next_destination_posting(anchor_transfer_id) values(v_g.anchor_transfer_id) on conflict do nothing;
  select * into strict v_s from private.bpay_next_destination_posting where anchor_transfer_id=v_g.anchor_transfer_id for update;
  insert into private.bpay_next_destination_posted_leg(original_transfer_id,anchor_transfer_id,job_id,posting_no)
    values(v_t.id,v_g.anchor_transfer_id,p_job_id,v_s.posted_leg_count+1);
  update private.bpay_next_destination_posting set posted_leg_count=posted_leg_count+1,last_posted_transfer_id=v_t.id,
    completed_at_utc=case when posted_leg_count+1=v_g.expected_leg_count then pg_catalog.transaction_timestamp() else null end
    where anchor_transfer_id=v_g.anchor_transfer_id returning * into v_s;
  if v_s.completed_at_utc is not null then
    update private.bpay_next_run_worker set status='COMPLETE' where id=v_g.run_worker_id
      and status='READY' and active_case_hold_count=0;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_DESTINATION_COMPLETION_HOLDS_OR_STATE_INVALID';end if;
  end if;
end $function$;

alter function private.bpay_next_destination_execution_leg_v1(uuid) owner to postgres;
alter function private.bpay_next_destination_posting_guard_v1() owner to postgres;
alter function private.bpay_next_destination_posted_leg_guard_v1() owner to postgres;
alter function private.bpay_next_complete_destination_leg_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_destination_execution_leg_v1(uuid),private.bpay_next_destination_posting_guard_v1(),
  private.bpay_next_destination_posted_leg_guard_v1(),private.bpay_next_complete_destination_leg_v1(uuid)
  from public,anon,authenticated,service_role;


create or replace function private.bpay_next_original_reservation_v1(p_worker_id uuid,p_transfer_id uuid)
returns table(reservation_mode text,reservation_projection_id uuid,remaining_reservation_cash text,
  reservation_internal_zero boolean,reservation_state text,reservation_holds_state text)
language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
  w record;r record;
  g record;t record;
  o record;p record;
  n record;l record;
  ns record;ds record;
  v_cash numeric;v_active boolean:=false;v_cancel boolean:=false;v_posted boolean:=false;
  v_receipt boolean:=false;v_return boolean:=false;v_internal boolean:=false;
begin
  -- Non-Case branches still qualify holds against an explicitly empty Case scope.
  select null::uuid as id,null::text as status,null::text as net_stage,
    null::bigint as projection_no,null::bigint as preparation_revision,null::bigint as selection_revision into ns;
  ds:=ns;
  select id,run_id,candidate_id,status,target_pay_channel,case_selection_revision,net_request_revision,net_projection_revision,entered_paye_net,target_umbrella_id,target_umbrella_vat_chargeable,gross_inc_vat,gross_ex_vat,gross_vat,captured_line_count,financial_resolution_count,expected_managed_position_count,preparation_revision into strict w from private.bpay_next_run_worker where id=p_worker_id;
  select id,status,confirmed_at_utc into strict r from private.bpay_next_pay_run where id=w.run_id;
  reservation_mode:='EVIDENCE_CONFLICT';reservation_state:='EVIDENCE_CONFLICT';
  reservation_internal_zero:=false;reservation_holds_state:='NONE';
  select anchor_transfer_id,run_worker_id,candidate_id,projection_id,net_state_id,preparation_revision,selection_revision,expected_leg_count,stage,completed_at_utc,created_leg_count,sealed_leg_count,sealed_cash_total into g from private.bpay_next_destination_group where run_worker_id=w.id;
  if g.anchor_transfer_id is not null then
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into o from private.bpay_next_transfer where id=g.anchor_transfer_id and run_worker_id=w.id;
  else
    -- The original is the native first instruction, never a newest reissue.
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into o from private.bpay_next_transfer where run_worker_id=w.id and transfer_no=1
      and original_transfer_id is null and return_cash_id is null;
  end if;
  if p_transfer_id is not null then
    select id,candidate_id,original_transfer_id,return_cash_id,projection_id,execution_kind,status,cash_amount into strict t from private.bpay_next_transfer where id=p_transfer_id and run_worker_id=w.id;
    if t.candidate_id<>w.candidate_id then return next;return;end if;
    if t.original_transfer_id is not null then
      reservation_mode:='ORIGINAL_LEGS';reservation_state:='REISSUE_EXCLUDED';
      remaining_reservation_cash:='0.00';return next;return;
    end if;
  end if;
  if o.id is not null then
    select id,run_worker_id,projection_no,cash_amount,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,request_command_id into p from private.bpay_next_net_projection where id=o.projection_id and run_worker_id=w.id;
    if p.id is null or o.candidate_id<>w.candidate_id or o.original_transfer_id is not null
      or (g.anchor_transfer_id is not null and (g.projection_id<>p.id or g.candidate_id<>w.candidate_id)) then
      return next;return;end if;
  else
    select np.id,np.run_worker_id,np.projection_no,np.cash_amount,np.input_kind,np.gross_ex_vat,np.gross_vat,np.gross_inc_vat,np.entered_paye_net,np.accepted_recoveries,np.request_command_id into p from private.bpay_next_case_cancel_binding b
      join private.bpay_next_cancel_request cr on cr.command_id=b.command_id
      join private.bpay_next_net_projection np on np.id=b.projection_id and np.run_worker_id=b.run_worker_id
      where b.run_worker_id=w.id and b.candidate_id=w.candidate_id and b.status=w.status
        and cr.status=b.status and b.status in ('CANCELLING','CANCELLED');
    if not found then
      select id,run_worker_id,projection_no,cash_amount,input_kind,gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,request_command_id into p from private.bpay_next_net_projection where run_worker_id=w.id
        and projection_no=w.net_projection_revision and retired_at_utc is null;
    end if;
  end if;
  reservation_projection_id:=p.id;
  -- CANCEL_REQUEST checking is not the cancellation fence. The native request
  -- and, for Case/group, exact captured binding must already be CANCELLING.
  select exists(select 1 from private.bpay_next_cancel_request cr
    where cr.run_worker_id=w.id and cr.candidate_id=w.candidate_id
      and cr.status in ('CANCELLING','CANCELLED') and w.status=cr.status
      and (w.case_selection_revision=0 or exists(
        select 1 from private.bpay_next_case_cancel_binding b
        where b.command_id=cr.command_id and b.run_worker_id=w.id and b.candidate_id=w.candidate_id
          and b.status=cr.status and b.projection_id is not distinct from p.id
          and b.transfer_id is not distinct from o.id
          and (g.anchor_transfer_id is null or (b.net_state_id=g.net_state_id
            and b.preparation_revision=g.preparation_revision and b.selection_revision=g.selection_revision
            and b.accepted_projection_revision=p.projection_no and exists(
              select 1 from private.bpay_next_destination_cancel dc where dc.command_id=b.command_id
                and dc.anchor_transfer_id=g.anchor_transfer_id and dc.expected_leg_count=g.expected_leg_count
                and dc.checked_leg_count=dc.expected_leg_count)))))
  ) into v_cancel;
  if v_cancel then
    reservation_mode:=w.status;reservation_state:=w.status;remaining_reservation_cash:='0.00';
    reservation_holds_state:=case when w.status='CANCELLING' then 'RELEASING' else 'NONE' end;
    return next;return;
  end if;
  if w.status in ('CANCELLING','CANCELLED') then return next;return;end if;
  if r.confirmed_at_utc is null then
    reservation_mode:='UNCONFIRMED';reservation_state:='SUPPRESSED_UNCONFIRMED';return next;return;
  end if;
  if o.id is null and w.status<>'READY' then
    reservation_mode:='PENDING_PREPARATION';reservation_state:='PENDING';return next;return;
  end if;
  if o.id is null and w.target_pay_channel='PAYE' and
    (p.id is null or w.net_request_revision<>w.net_projection_revision) then
    reservation_mode:='PENDING_NET';reservation_state:='PENDING';return next;return;
  end if;
  if p.id is null then
    -- Ordinary Umbrella has no projection until BEGIN, and then request0/projection1.
    if o.id is null and w.target_pay_channel='UMBRELLA' and r.status='DRAFT'
      and w.case_selection_revision=0 and w.net_request_revision=0 and w.net_projection_revision=0
      and w.entered_paye_net is null and w.target_umbrella_id is not null
      and w.target_umbrella_vat_chargeable is not null and w.gross_inc_vat>0
      and w.captured_line_count>0 and w.financial_resolution_count=0 and w.expected_managed_position_count=0
      and not exists(select 1 from private.bpay_next_paye_net_request q where q.run_worker_id=w.id) then
      v_cash:=w.gross_inc_vat;
    else return next;return;end if;
  else
    v_cash:=p.cash_amount;
    if (w.target_pay_channel='UMBRELLA' and (p.input_kind<>'UMBRELLA' or w.case_selection_revision<>0))
      or (w.target_pay_channel='PAYE' and p.input_kind not in ('PAYE_MANUAL','PAYE_IMPORT','CASE_PAYOUT')) then
      return next;return;end if;
    if o.id is null and (p.gross_ex_vat,p.gross_vat,p.gross_inc_vat,p.entered_paye_net)
      is distinct from (w.gross_ex_vat,w.gross_vat,w.gross_inc_vat,w.entered_paye_net) then return next;return;end if;
    if w.case_selection_revision>0 then
      select id,status,net_stage,projection_no,preparation_revision,selection_revision into ns from private.bpay_next_case_allocation_state where projection_id=p.id
        and run_worker_id=w.id and pass_kind='NET';
      select id,status,net_stage,projection_no,preparation_revision,selection_revision into ds from private.bpay_next_case_allocation_state where run_worker_id=w.id
        and preparation_revision=coalesce(g.preparation_revision,ns.preparation_revision)
        and selection_revision=coalesce(g.selection_revision,ns.selection_revision)
        and pass_kind='DRAFT' and projection_no=0;
      if ns.id is null or ds.id is null or ns.status not in ('COMPLETE','CANCELLED')
        or ns.net_stage<>'COMPLETE' or ns.projection_no<>p.projection_no
        or ns.preparation_revision<>ds.preparation_revision or ns.selection_revision<>ds.selection_revision
        or ds.status not in ('COMPLETE','CANCELLED') then return next;return;end if;
    end if;
    select state_id,run_worker_id,completed_at_utc,cash_amount,external_leg_count,external_amount,own_amount into n from private.bpay_next_net_destination_state where projection_id=p.id;
    if n.state_id is not null and (n.run_worker_id<>w.id or n.state_id is distinct from ns.id
      or n.completed_at_utc is null or n.cash_amount<>p.cash_amount or n.external_leg_count<1
      or n.external_amount<=0 or n.own_amount+n.external_amount<>p.cash_amount) then return next;return;end if;
  end if;
  if g.anchor_transfer_id is not null then
    if n.state_id is null or g.net_state_id<>n.state_id or g.expected_leg_count<>n.external_leg_count+1 then
      return next;return;end if;
    if g.stage='COMPLETE' and (g.completed_at_utc is null or g.created_leg_count<>g.expected_leg_count
      or g.sealed_leg_count<>g.expected_leg_count or g.sealed_cash_total<>p.cash_amount) then return next;return;end if;
  end if;
  if o.id is null or (g.anchor_transfer_id is not null and g.stage<>'COMPLETE')
    or (g.anchor_transfer_id is null and o.status='BUILDING') then
    reservation_mode:='ORIGINAL_OBLIGATION';reservation_state:='INCLUDED_IN_ORIGINAL_OBLIGATION';
    reservation_internal_zero:=v_cash=0;
    select exists(select 1 from private.bpay_next_run_line rl
      join private.bpay_next_hold h on h.run_line_id=rl.id
      where rl.run_worker_id=w.id and h.status='ACTIVE'
        and (h.work_id,h.component_key,h.source_reserved_ex_vat,h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
          is not distinct from (rl.work_id,rl.component_key,rl.source_consumed_ex_vat,rl.frozen_ex_vat,rl.frozen_vat,rl.frozen_inc_vat))
      or exists(select 1 from private.bpay_next_case_hold h
        join private.bpay_next_case_allocation_result ar on ar.id=h.allocation_result_id
        where h.run_worker_id=w.id and h.candidate_id=w.candidate_id and h.status='ACTIVE'
          and ar.state_id in (ds.id,ns.id)) into v_active;
    reservation_holds_state:=case when v_active then 'ACTIVE' else 'NONE' end;
    if v_cash>0 and not v_active then
      reservation_mode:='EVIDENCE_CONFLICT';reservation_state:='EVIDENCE_CONFLICT';
      reservation_internal_zero:=false;return next;return;
    end if;
    if p_transfer_id is null then remaining_reservation_cash:=v_cash::numeric(18,2)::text;end if;
    return next;return;
  end if;
  reservation_mode:='ORIGINAL_LEGS';
  if p_transfer_id is null then reservation_state:='PAGED_ORIGINAL_LEGS';return next;return;end if;
  reservation_internal_zero:=t.execution_kind='INTERNAL_ZERO';
  if t.projection_id is distinct from p.id then reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  if g.anchor_transfer_id is not null then
    select transfer_id,leg_kind,destination_id into l from private.bpay_next_destination_group_leg where transfer_id=t.id and anchor_transfer_id=g.anchor_transfer_id;
    if l.transfer_id is null then reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
    if l.leg_kind='OWN' and l.destination_id is null and t.id=g.anchor_transfer_id then v_cash:=n.own_amount;
    elsif l.leg_kind='ONEOFF' then
      select amount into v_cash from private.bpay_next_net_destination_amount where destination_id=l.destination_id and state_id=n.state_id;
    else reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  end if;
  if v_cash is null or t.cash_amount<>v_cash or (t.execution_kind='INTERNAL_ZERO')<>(v_cash=0) then
    reservation_mode:='EVIDENCE_CONFLICT';return next;return;end if;
  select exists(select 1 from private.bpay_next_transfer_outcome x
    join private.bpay_next_outcome_request q on q.outcome_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.outcome_kind='SETTLED' and x.whole_transfer_amount=t.cash_amount
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_transfer_outcome x
    join private.bpay_next_return_request q on q.outcome_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.outcome_kind='RETURNED' and x.whole_transfer_amount=t.cash_amount
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_internal_receipt x
    join private.bpay_next_outcome_request q on q.internal_receipt_id=x.id and q.transfer_id=x.transfer_id
    where x.transfer_id=t.id and x.run_worker_id=w.id and x.candidate_id=w.candidate_id
      and x.projection_id=p.id and x.cash_amount=0 and q.run_worker_id=w.id and q.candidate_id=w.candidate_id),
    exists(select 1 from private.bpay_next_outcome_request q where q.transfer_id=t.id
      and q.run_worker_id=w.id and q.candidate_id=w.candidate_id and q.posting_complete)
    into v_receipt,v_return,v_internal,v_posted;
  if v_return and v_receipt and t.status='RETURNED' then
    reservation_state:='RETURNED';remaining_reservation_cash:='0.00';
  elsif v_receipt and t.status='SETTLED' then
    reservation_state:=case when v_posted then 'POSTED' else 'EXECUTED_PENDING_POSTING' end;
    remaining_reservation_cash:='0.00';reservation_holds_state:=case when v_posted then 'NONE' else 'POSTING' end;
  elsif v_internal and reservation_internal_zero and t.status in ('INTERNAL_PROCESSING','INTERNAL_SETTLED') then
    reservation_state:=case when v_posted then 'CLOSED_INTERNAL' else 'INTERNAL_PENDING_POSTING' end;
    remaining_reservation_cash:='0.00';reservation_holds_state:=case when v_posted then 'NONE' else 'POSTING' end;
  elsif t.status in ('SUBMITTED','UNKNOWN') and not(v_receipt or v_return or v_internal) then
    reservation_state:='EXECUTION_UNCERTAIN';
  elsif t.status in ('MEMBERS_READY','DRAFT','SCHEDULED','REFUSED','ISSUED_CSV') and not(v_receipt or v_return or v_internal) then
    select exists(select 1 from private.bpay_next_transfer_member m
      join private.bpay_next_hold h on h.run_line_id=m.run_line_id
      where m.transfer_id=t.id and m.subject_kind='WORK' and h.status='ACTIVE')
      or exists(select 1 from private.bpay_next_transfer_member m
        join private.bpay_next_case_hold h on h.id=m.case_hold_id
        where m.transfer_id=t.id and m.subject_kind='CASE' and h.status='ACTIVE'
          and h.run_worker_id=w.id and h.candidate_id=w.candidate_id) into v_active;
    reservation_holds_state:=case when v_active then 'ACTIVE' else 'NONE' end;
    if t.cash_amount>0 and not v_active then
      reservation_state:='EVIDENCE_CONFLICT';return next;return;
    end if;
    reservation_state:=case when reservation_internal_zero then 'INTERNAL_RESERVED'
      when t.status='ISSUED_CSV' then 'ISSUED_AWAITING_RECEIPT' else 'RESERVED' end;
    remaining_reservation_cash:=t.cash_amount::text;
  end if;
  return next;
end
$function$;
alter function private.bpay_next_original_reservation_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_original_reservation_v1(uuid,uuid) from public,anon,authenticated,service_role;
create or replace function private.bpay_next_worker_status_page_v1(
  p_run_id uuid,p_worker_id uuid,p_after_transfer_no integer,p_limit integer
) returns jsonb language plpgsql stable security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_header jsonb;v_row record;v_rows jsonb:='[]'::jsonb;
  v_bytes integer;v_count integer:=0;v_more boolean:=false;
  v_query text;
begin
  if p_run_id is null or p_worker_id is null or p_limit is null or p_limit not between 1 and 100
     or (p_after_transfer_no is not null and p_after_transfer_no<1) then
    raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
  end if;
  if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  -- One run, one worker, one accepted-projection unique lookup. During a net
  -- replacement show the accepted projection AND an explicit pending flag;
  -- never portray the new request as already accepted. Original CSV advice
  -- uses the transfer's own projection, not this current progress pointer.
  select pg_catalog.jsonb_build_object(
    'run_id',r.id,'run_status',r.status,'selection_state',r.selection_state,
    'review_revision',r.review_revision::text,'run_worker_id',w.id,
    'candidate_id',w.candidate_id,'worker_status',w.status,
    'target_pay_channel',w.target_pay_channel,'umbrella_id',w.target_umbrella_id,
    'preparation_revision',w.preparation_revision::text,
    'case_selection_revision',w.case_selection_revision::text,
    'net_request_revision',w.net_request_revision::text,
    'net_projection_revision',w.net_projection_revision::text,
    'net_pending',w.net_request_revision<>w.net_projection_revision,
    'review_issue_code',w.review_issue_code,'review_issue_work_id',w.review_issue_work_id,
    'frozen_gross_ex_vat',w.gross_ex_vat::text,'frozen_gross_vat',w.gross_vat::text,
    'frozen_gross_inc_vat',w.gross_inc_vat::text,
    'projection_id',p.id,'projection_input_kind',p.input_kind,
    'entered_paye_net',p.entered_paye_net::text,
    'accepted_recoveries',p.accepted_recoveries::text,
    'accepted_net_additions',p.accepted_net_additions::text,
    'cash_amount',p.cash_amount::text,
    'reservation_mode',rv.reservation_mode,'reservation_projection_id',rv.reservation_projection_id,
    'remaining_reservation_cash',rv.remaining_reservation_cash::text,'reservation_internal_zero',rv.reservation_internal_zero,
    'reservation_holds_state',rv.reservation_holds_state
  ) into v_header
  from private.bpay_next_pay_run r
  join private.bpay_next_run_worker w on w.run_id=r.id and w.id=p_worker_id
  left join private.bpay_next_net_projection p
    on p.run_worker_id=w.id and p.projection_no=w.net_projection_revision and p.retired_at_utc is null
  cross join lateral private.bpay_next_original_reservation_v1(w.id,null) rv
  where r.id=p_run_id;
  if not found then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_SCOPE_INVALID';end if;
  v_bytes:=512+pg_catalog.octet_length(v_header::text);
  v_query:=$query$select pg_catalog.to_jsonb(page) as row_value from (
      select t.id as transfer_id,t.transfer_no::text,t.projection_id,t.execution_kind,
        t.status,t.cash_amount::text,t.original_transfer_id,
        rv.reservation_state,rv.remaining_reservation_cash,rv.reservation_holds_state
      from private.bpay_next_transfer t
      cross join lateral private.bpay_next_original_reservation_v1(t.run_worker_id,t.id) rv where t.run_worker_id=$1$query$;
  if p_after_transfer_no is not null then
    v_query:=v_query||' and t.transfer_no>$2';
  end if;
  v_query:=v_query||' order by t.transfer_no,t.id limit $3) page';
  for v_row in execute v_query using p_worker_id,p_after_transfer_no,p_limit
  loop
    if v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1>120000 then
      if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
      v_more:=true;exit;
    end if;
    v_bytes:=v_bytes+pg_catalog.octet_length(v_row.row_value::text)+1;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row.row_value);v_count:=v_count+1;
  end loop;
  return pg_catalog.jsonb_build_object('header',v_header,'rows',v_rows,'complete',not(v_more or v_count=p_limit));
end
$function$;
alter function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) owner to postgres;
revoke all on function private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer) from public,anon,authenticated,service_role;
commit;
