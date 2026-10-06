-- One maintained original weekly contribution follows its actual owned outcome.
-- No history scan/backfill, earnings reprice, payroll/principal inverse or bank
-- action. B1 remains UNBOUND for returned payroll, not non-earnings CASE_PAYOUT.
-- Install additive 0440 before first use of the CASE_PAYOUT return branch.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_apply_week_outcome_v1(
  p_job_id uuid,p_return_cash_id uuid default null
) returns void
language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;
  v_original private.bpay_next_transfer%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;
  v_outcome private.bpay_next_transfer_outcome%rowtype;
  v_settlement private.bpay_next_outcome_request%rowtype;
  v_return private.bpay_next_return_request%rowtype;
  v_cash private.bpay_next_return_cash%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_epoch bigint;v_worker_id uuid;v_candidate uuid;v_transfer_id uuid;
  v_outcome_id uuid;v_posted boolean;v_next_state text;v_payout boolean;
  v_old_resolved numeric;v_unresolved_delta integer:=0;
  v_leg private.bpay_next_destination_group_leg%rowtype;
  v_destination private.bpay_next_net_destination_state%rowtype;
begin
  if p_job_id is null then
    raise exception using errcode='22023',message='BPAY_NEXT_WEEK_OUTCOME_INPUT_INVALID';
  end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind='CSV_SETTLEMENT' then
    select r.* into strict v_settlement from private.bpay_next_outcome_request r where r.command_id=v_job.command_id;
    v_worker_id:=v_settlement.run_worker_id;v_candidate:=v_settlement.candidate_id;
    v_transfer_id:=v_settlement.transfer_id;v_outcome_id:=v_settlement.outcome_id;
    if p_return_cash_id is not null then
      raise exception using errcode='22023',message='BPAY_NEXT_WEEK_SETTLEMENT_CASH_ARGUMENT_INVALID';
    end if;
  elsif v_job.job_kind='CSV_RETURN' then
    select r.* into strict v_return from private.bpay_next_return_request r where r.command_id=v_job.command_id;
    v_worker_id:=v_return.run_worker_id;v_candidate:=v_return.candidate_id;
    v_transfer_id:=v_return.transfer_id;v_outcome_id:=v_return.outcome_id;
    if p_return_cash_id is null then
      raise exception using errcode='22023',message='BPAY_NEXT_WEEK_RETURN_CASH_REQUIRED';
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_WEEK_OUTCOME_JOB_KIND_INVALID';end if;
  -- Same order as the posting caller, also safe for an owner-only first-use
  -- probe: header -> Candidate control -> job -> worker -> transfer/cash -> week.
  select r.* into strict v_run from private.bpay_next_pay_run r
    join private.bpay_next_run_worker w on w.run_id=r.id where w.id=v_worker_id for update of r;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_worker_id for update;
  select t.* into strict v_transfer from private.bpay_next_transfer t where t.id=v_transfer_id for update;
  select o.* into strict v_outcome from private.bpay_next_transfer_outcome o where o.id=v_outcome_id;
  if v_job.job_kind='CSV_SETTLEMENT' then
    select r.* into strict v_settlement from private.bpay_next_outcome_request r where r.command_id=v_job.command_id;
    v_posted:=v_settlement.posting_complete and v_settlement.posted_member_count=v_transfer.member_count;
  else
    select r.* into strict v_return from private.bpay_next_return_request r where r.command_id=v_job.command_id;
    v_posted:=v_return.posting_complete;
  end if;
  if v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_worker.candidate_id<>v_candidate or v_worker.run_id<>v_run.id
     or v_transfer.candidate_id<>v_candidate or v_transfer.run_worker_id<>v_worker.id
     or v_transfer.beneficiary_kind<>'CANDIDATE' or v_transfer.beneficiary_id<>v_candidate
     or v_outcome.transfer_id<>v_transfer.id or v_outcome.whole_transfer_amount<>v_transfer.cash_amount
     or v_outcome.outcome_kind<>(case when v_job.job_kind='CSV_SETTLEMENT' then 'SETTLED' else 'RETURNED' end)
     or v_posted is not true then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_OUTCOME_SCOPE_INVALID';
  end if;
  select t.* into strict v_original from private.bpay_next_transfer t
    where t.id=coalesce(v_transfer.original_transfer_id,v_transfer.id) for update;
  if v_original.original_transfer_id is not null or v_original.return_cash_id is not null
     or v_original.run_worker_id<>v_worker.id or v_original.candidate_id<>v_candidate
     or v_original.beneficiary_kind<>'CANDIDATE' or v_original.beneficiary_id<>v_candidate then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_TRANSFER_INVALID';
  end if;
  if v_job.job_kind='CSV_RETURN' or v_transfer.return_cash_id is not null then
    select c.* into strict v_cash from private.bpay_next_return_cash c
      where c.id=coalesce(p_return_cash_id,v_transfer.return_cash_id) for update;
    if v_cash.original_transfer_id<>v_original.id or v_cash.candidate_id<>v_candidate
       or v_cash.amount_owed<>v_original.cash_amount
       or (v_transfer.return_cash_id is not null and v_transfer.return_cash_id<>v_cash.id) then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_CASH_NOT_EXACT';
    end if;
  end if;
  -- The posting transition and this helper commit atomically. A delayed old
  -- DONE helper is readback, not adoption/backfill of its old reissue pointer.
  if v_job.status='DONE' then return;end if;
  if v_job.status<>'LEASED' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_transfer.status not in ('SETTLED','RETURNED')
     or (v_job.job_kind='CSV_RETURN' and v_transfer.status<>'RETURNED') then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_OUTCOME_OWNER_INVALID';
  end if;
  if v_worker.target_pay_channel<>'PAYE' then return;end if;
  v_leg:=private.bpay_next_destination_execution_leg_v1(v_original.id);
  if v_leg.transfer_id is not null then
    -- One-off cash is not the Candidate's own weekly take-home contribution.
    -- Its exact receipt still posts its CASE members through the normal owner.
    if v_leg.leg_kind='ONEOFF' then return;end if;
    select n.* into strict v_destination from private.bpay_next_destination_group g
      join private.bpay_next_net_destination_state n on n.state_id=g.net_state_id
      where g.anchor_transfer_id=v_leg.anchor_transfer_id;
  end if;
  select w.* into v_week from private.bpay_next_worker_week w
    where w.candidate_id=v_candidate
      and w.pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select x.* into v_prior from private.bpay_next_worker_week_contribution x
    where x.original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then
    if v_worker.case_selection_revision<>0 then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CASE_CONTRIBUTION_MISSING';
    end if;
    -- An explicitly unbound no-case READY floor has no contribution. Do not
    -- reconstruct one from a now-paid transfer or guess its earlier earnings.
    raise notice 'BPAY_NEXT_WEEK_ORIGINAL_CONTRIBUTION_UNBOUND';return;
  end if;
  select p.* into strict v_projection from private.bpay_next_net_projection p where p.id=v_original.projection_id;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_candidate
     or v_prior.pay_week_start<>v_week.pay_week_start
     or v_prior.original_pay_date<>v_run.pay_date or v_prior.original_created_at_utc<>v_run.created_at_utc
     or v_prior.original_gross_amount<>v_projection.gross_inc_vat
     or v_prior.accepted_projection_id is distinct from v_projection.id
     or v_prior.accepted_bank_cash is distinct from v_projection.cash_amount
     or v_projection.run_worker_id<>v_worker.id
     or v_original.cash_amount<>(case when v_leg.transfer_id is null then v_projection.cash_amount else v_destination.own_amount end)
     or (v_prior.original_transfer_id is not null and v_prior.original_transfer_id<>v_original.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_CONTRIBUTION_NOT_EXACT';
  end if;
  v_payout:=v_projection.input_kind='CASE_PAYOUT';
  if v_payout and (v_prior.basis_kind<>'GROSS_FALLBACK' or v_prior.original_gross_amount<>0
     or v_prior.original_payroll_net is not null or v_prior.eligibility_state<>'EXCLUDED'
     or v_prior.eligible_arranged_amount is distinct from 0::numeric) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_NON_EARNINGS_BASIS_INVALID';
  end if;
  if v_job.job_kind='CSV_SETTLEMENT' and v_transfer.original_transfer_id is null then
    -- RETURNED may already be RECEIVED: original payroll posting still comes
    -- first. The ordered return owner will later publish the actual cash FK.
    if v_prior.payment_state='PAID' and v_prior.original_transfer_id=v_original.id then return;end if;
    if v_prior.payment_state<>'ARRANGED' or v_prior.eligibility_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_ORIGINAL_SETTLEMENT_STATE_INVALID';
    end if;
    update private.bpay_next_worker_week_contribution set payment_state='PAID',original_transfer_id=v_original.id,
      contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
    update private.bpay_next_worker_week set period_revision=period_revision+1
      where candidate_id=v_candidate and pay_week_start=v_week.pay_week_start;
    return;
  end if;
  if v_prior.original_transfer_id is distinct from v_original.id
     or (v_prior.returned_cash_id is not null and v_prior.returned_cash_id<>v_cash.id) then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_ORIGINAL_NOT_PAID';
  end if;
  if v_job.job_kind='CSV_SETTLEMENT' then
    v_next_state:='REISSUED_PAID';
    if v_prior.payment_state='REISSUED_PAID' and v_prior.reissue_transfer_id=v_transfer.id then return;end if;
    if v_prior.payment_state<>'RETURNED_OWED' or v_prior.returned_cash_id is distinct from v_cash.id
       or v_cash.amount_reissued_paid<v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_REISSUE_SETTLEMENT_STATE_INVALID';
    end if;
  else
    v_next_state:='RETURNED_OWED';
    if v_prior.payment_state='RETURNED_OWED' and v_prior.returned_cash_id=v_cash.id
       and (v_transfer.original_transfer_id is null or v_prior.reissue_transfer_id=v_transfer.id) then return;end if;
    if (v_transfer.original_transfer_id is null and v_prior.payment_state<>'PAID')
       or (v_transfer.original_transfer_id is not null and
         (v_prior.payment_state<>'REISSUED_PAID' or v_prior.reissue_transfer_id is distinct from v_transfer.id)) then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RETURN_PAYMENT_STATE_INVALID';
    end if;
  end if;
  v_old_resolved:=coalesce(v_prior.eligible_arranged_amount,0);
  if not v_payout and v_prior.eligibility_state<>'UNBOUND' then v_unresolved_delta:=1;end if;
  update private.bpay_next_worker_week_contribution set payment_state=v_next_state,returned_cash_id=v_cash.id,
    reissue_transfer_id=case when v_transfer.original_transfer_id is not null then v_transfer.id else reissue_transfer_id end,
    eligibility_state=case when v_payout then 'EXCLUDED' else 'UNBOUND' end,
    eligible_arranged_amount=case when v_payout then 0 else null end,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old_resolved,
    unresolved_contribution_count=unresolved_contribution_count+v_unresolved_delta,period_revision=period_revision+1
    where candidate_id=v_candidate and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old_resolved;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_RESOLVED_DELTA_INVALID';end if;
end
$function$;

create or replace function private.bpay_next_apply_cancelled_week_v1(p_job_id uuid)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_cancel_request%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;v_week private.bpay_next_worker_week%rowtype;
  v_prior private.bpay_next_worker_week_contribution%rowtype;v_transfer private.bpay_next_transfer%rowtype;
  v_case_cancel private.bpay_next_case_cancel_binding%rowtype;
  v_group private.bpay_next_destination_group%rowtype;v_group_cancel private.bpay_next_destination_cancel%rowtype;
  v_epoch bigint;v_transfer_count integer:=0;v_old_resolved numeric;v_old_unresolved integer;
begin
  if p_job_id is null then raise exception using errcode='22023',message='BPAY_NEXT_WEEK_CANCEL_INPUT_INVALID';end if;
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
  if v_job.job_kind<>'SIMPLE_CANCEL' then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_JOB_KIND_INVALID';
  end if;
  select r.* into strict v_request from private.bpay_next_cancel_request r where r.command_id=v_job.command_id;
  select r.* into strict v_run from private.bpay_next_pay_run r
    join private.bpay_next_run_worker w on w.run_id=r.id where w.id=v_request.run_worker_id for update of r;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_request.candidate_id for update;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update;
  select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_request.run_worker_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch
     or v_job.candidate_id<>v_worker.candidate_id or v_request.candidate_id<>v_worker.candidate_id
     or v_request.status<>'CANCELLED' or v_worker.status<>'CANCELLED'
     or v_request.released_line_count<>v_request.expected_line_count
     or v_job.applied_line_count<>v_request.released_line_count
     or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0
     then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_SCOPE_INVALID';
  end if;
  if v_worker.case_selection_revision>0 then
    select b.* into strict v_case_cancel from private.bpay_next_case_cancel_binding b
      where b.command_id=v_job.command_id;
    if v_case_cancel.run_worker_id<>v_worker.id or v_case_cancel.candidate_id<>v_worker.candidate_id
       or v_case_cancel.preparation_revision<>v_worker.preparation_revision
       or v_case_cancel.selection_revision<>v_worker.case_selection_revision
       or v_case_cancel.status<>'CANCELLED' or v_case_cancel.stage<>'COMPLETE'
       or v_case_cancel.expected_work_count<>v_request.expected_line_count
       or v_case_cancel.released_work_count<>v_request.released_line_count
       or v_case_cancel.expected_case_hold_count is null
       or v_case_cancel.released_case_hold_count<>v_case_cancel.expected_case_hold_count
       or v_worker.active_case_hold_count<>0
       or v_request.cursor_line_no is distinct from v_case_cancel.checkpoint
       or v_job.cursor_key is distinct from v_case_cancel.checkpoint::text
       or exists(select 1 from private.bpay_next_case_hold h
           where h.run_worker_id=v_worker.id and h.status='ACTIVE') then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CASE_CANCEL_BINDING_INVALID';
    end if;
  end if;
  if v_job.status='DONE' then return;end if;
  if v_job.status<>'LEASED' or v_job.phase<>'RELEASE' or v_job.lease_nonce is null
     or v_job.owner_epoch<>v_control.active_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or v_control.pending_outcome_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_WEEK_CANCEL_OWNER_INVALID';
  end if;
  select g.* into v_group from private.bpay_next_destination_group g where g.run_worker_id=v_worker.id;
  if found then
    -- The guarded certificate counts exact <=100-row CHECK and FINAL ranges.
    -- Do not scan every sibling again or use an arbitrary counter as authority.
    select c.* into strict v_group_cancel from private.bpay_next_destination_cancel c where c.command_id=v_job.command_id;
    if v_worker.case_selection_revision<1 or v_group.candidate_id<>v_worker.candidate_id
       or v_group.stage<>'COMPLETE' or v_group.sealed_leg_count<>v_group.expected_leg_count
       or v_group_cancel.anchor_transfer_id<>v_group.anchor_transfer_id
       or v_group_cancel.expected_leg_count<>v_group.expected_leg_count
       or v_group_cancel.checked_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.cancelled_leg_count<>v_group_cancel.expected_leg_count
       or v_group_cancel.check_cursor is null or v_group_cancel.cancel_cursor is null
       or v_group_cancel.check_cursor<>v_group_cancel.cancel_cursor
       or v_case_cancel.transfer_id is distinct from v_group.anchor_transfer_id
       or v_case_cancel.projection_id is distinct from v_group.projection_id
       or v_case_cancel.net_state_id is distinct from v_group.net_state_id then
      raise exception using errcode='23514',message='BPAY_NEXT_WEEK_DESTINATION_CANCEL_CERTIFICATE_INVALID';
    end if;
  else
    -- Ordinary no-split scope retains its exact one-original-transfer guard.
    for v_transfer in select t.* from private.bpay_next_transfer t where t.run_worker_id=v_worker.id
      order by t.transfer_no limit 2 for update
    loop
      v_transfer_count:=v_transfer_count+1;
      if v_transfer_count>1 or v_transfer.status<>'CANCELLED' or v_transfer.original_transfer_id is not null
         or v_transfer.return_cash_id is not null or v_transfer.candidate_id<>v_worker.candidate_id
         or v_transfer.account_approval_ref is not null
         or exists(select 1 from private.bpay_next_csv_instruction i where i.transfer_id=v_transfer.id)
         or exists(select 1 from private.bpay_next_transfer_outcome o where o.transfer_id=v_transfer.id) then
        raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_TRANSFER_PROTECTED';
      end if;
    end loop;
  end if;
  select w.* into v_week from private.bpay_next_worker_week w where w.candidate_id=v_worker.candidate_id
    and w.pay_week_start=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1) for update;
  select x.* into v_prior from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=v_worker.id for update;
  if v_prior.id is null then raise notice 'BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_UNBOUND';return;end if;
  if v_week.candidate_id is null or v_prior.candidate_id<>v_worker.candidate_id
     or v_prior.pay_week_start<>v_week.pay_week_start or v_prior.original_pay_date<>v_run.pay_date
     or v_prior.original_created_at_utc<>v_run.created_at_utc or v_prior.original_gross_amount<>v_worker.gross_inc_vat
     or v_prior.original_transfer_id is not null or v_prior.returned_cash_id is not null or v_prior.reissue_transfer_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_CONTRIBUTION_NOT_EXACT';
  end if;
  if v_prior.payment_state='CANCELLED' and v_prior.eligibility_state='EXCLUDED' and v_prior.eligible_arranged_amount=0 then return;end if;
  if v_prior.payment_state<>'ARRANGED' then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_NOT_UNPAID';end if;
  v_old_resolved:=coalesce(v_prior.eligible_arranged_amount,0);
  v_old_unresolved:=case when v_prior.eligibility_state='UNBOUND' then 1 else 0 end;
  update private.bpay_next_worker_week_contribution set payment_state='CANCELLED',eligibility_state='EXCLUDED',eligible_arranged_amount=0,
    contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1 where id=v_prior.id;
  update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home-v_old_resolved,
    unresolved_contribution_count=unresolved_contribution_count-v_old_unresolved,period_revision=period_revision+1
    where candidate_id=v_worker.candidate_id and pay_week_start=v_week.pay_week_start
      and resolved_arranged_take_home>=v_old_resolved and unresolved_contribution_count>=v_old_unresolved;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_WEEK_CANCEL_DELTA_INVALID';end if;
end
$function$;
alter function private.bpay_next_apply_week_outcome_v1(uuid,uuid) owner to postgres;
alter function private.bpay_next_apply_cancelled_week_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_apply_week_outcome_v1(uuid,uuid),
  private.bpay_next_apply_cancelled_week_v1(uuid) from public,anon,authenticated,service_role;

commit;
