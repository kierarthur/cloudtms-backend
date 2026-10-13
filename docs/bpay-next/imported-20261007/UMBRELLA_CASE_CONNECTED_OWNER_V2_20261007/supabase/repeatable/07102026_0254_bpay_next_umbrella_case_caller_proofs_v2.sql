-- Immutable captured economics remain separate from the live projection gate.
-- These point proofs add no mutation, external leg, finance quote or pay authority.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_umbrella_case_completed_transfer_valid_v2(
  w private.bpay_next_run_worker,t private.bpay_next_transfer)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare p private.bpay_next_net_projection%rowtype;b private.bpay_next_case_transfer_build%rowtype;
begin
  if w.target_pay_channel<>'UMBRELLA' or w.case_selection_revision<1
    or t.run_worker_id<>w.id or t.candidate_id<>w.candidate_id or t.original_transfer_id is not null
    or t.return_cash_id is not null or t.projection_id is null
    or t.beneficiary_kind<>'UMBRELLA' or t.beneficiary_id is distinct from w.target_umbrella_id
    or t.cash_amount<0 or t.member_count<0 or t.member_cash_sum<>t.cash_amount then return false;end if;
  select * into p from private.bpay_next_net_projection where id=t.projection_id;
  if not found or p.projection_no<>w.net_projection_revision
    or not private.bpay_next_umbrella_case_projection_valid_v2(w,p) then return false;end if;
  select * into b from private.bpay_next_case_transfer_build where transfer_id=t.id;
  if not found then return false;end if;
  return (b.run_worker_id,b.candidate_id,b.projection_id,b.draft_state_id,b.preparation_revision,b.selection_revision,b.target_pay_channel)
      is not distinct from (w.id,w.candidate_id,p.id,p.umbrella_case_draft_state_id,w.preparation_revision,w.case_selection_revision,'UMBRELLA'::text)
    and b.net_state_id is null and b.stage='COMPLETE' and b.completed_at_utc is not null
    and b.processed_work_count=b.expected_work_count and b.expected_work_count=w.captured_line_count
    and b.processed_case_count=b.expected_case_count and b.expected_case_count=w.case_instruction_count
    and t.member_count=b.expected_work_count+b.expected_case_count
    and (b.gross_additions_total,b.gross_deductions_total,b.net_additions_total,b.net_recoveries_total)
      is not distinct from (p.accepted_gross_additions,p.accepted_gross_deductions,p.accepted_net_additions,p.accepted_recoveries)
    and b.work_cash_total+b.gross_additions_total-b.gross_deductions_total=p.gross_inc_vat
    and t.cash_amount=p.cash_amount
    and exists(select 1 from private.bpay_next_command c where c.id=t.build_command_id
      and c.command_kind='TRANSFER_BUILD' and c.status='COMPLETE')
    and not exists(select 1 from private.bpay_next_destination_group_leg l where l.transfer_id=t.id);
end $function$;

create or replace function private.bpay_next_umbrella_case_csv_transfer_valid_v2(
  w private.bpay_next_run_worker,t private.bpay_next_transfer)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
begin
  if w.status<>'READY' or t.status<>'MEMBERS_READY' or t.cash_amount<=0 or t.member_count<=0 then return false;end if;
  return private.bpay_next_umbrella_case_completed_transfer_valid_v2(w,t);
end $function$;

-- The live validator remains retirement-strict. Only an actual completed
-- SIMPLE_CANCEL fact can qualify a retired projection for CANCELLED week state.
create or replace function private.bpay_next_umbrella_case_cancelled_projection_valid_v2(
  w private.bpay_next_run_worker,p private.bpay_next_net_projection)
returns boolean language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare b private.bpay_next_case_cancel_binding%rowtype;r private.bpay_next_cancel_request%rowtype;
  j private.bpay_next_job%rowtype;
begin
  if w.target_pay_channel<>'UMBRELLA' or w.status<>'CANCELLED' or p.retired_at_utc is null
    or not pg_catalog.isfinite(p.retired_at_utc)
    or not private.bpay_next_umbrella_case_projection_snapshot_valid_v2(w,p) then return false;end if;
  select * into b from private.bpay_next_case_cancel_binding where projection_id=p.id;
  if not found or b.status<>'CANCELLED' or b.stage<>'COMPLETE'
    or (b.run_worker_id,b.candidate_id,b.projection_id,b.umbrella_draft_state_id,b.draft_state_id,
      b.preparation_revision,b.selection_revision,b.accepted_projection_revision)
    is distinct from (w.id,w.candidate_id,p.id,p.umbrella_case_draft_state_id,p.umbrella_case_draft_state_id,
      w.preparation_revision,w.case_selection_revision,p.projection_no)
    or b.net_state_id is not null or b.finished_at_utc is distinct from p.retired_at_utc
    or b.started_at_utc is null or b.finished_at_utc<b.started_at_utc
    or b.released_work_count<>b.expected_work_count or b.released_case_hold_count<>b.expected_case_hold_count
    or w.active_case_hold_count<>0 then return false;end if;
  select * into r from private.bpay_next_cancel_request where command_id=b.command_id;
  if not found or r.run_worker_id<>w.id or r.candidate_id<>w.candidate_id or r.status<>'CANCELLED'
    or (r.expected_line_count,r.released_line_count,r.cursor_line_no,r.finished_at_utc)
      is distinct from (b.expected_work_count,b.released_work_count,b.checkpoint,b.finished_at_utc) then return false;end if;
  select * into j from private.bpay_next_job where command_id=b.command_id and candidate_id=w.candidate_id;
  if not found or j.job_kind<>'SIMPLE_CANCEL' or j.cursor_key is distinct from b.checkpoint::text
    or j.applied_line_count<>b.released_work_count
    or not ((j.status='LEASED' and j.phase='RELEASE' and j.lease_nonce is not null
        and j.lease_until_utc>pg_catalog.statement_timestamp()) or (j.status='DONE' and j.phase='CANCELLED'))
    or not exists(select 1 from private.bpay_next_command c where c.id=b.command_id
      and c.command_kind='SIMPLE_CANCEL' and c.agency_sequence=j.command_sequence and c.module_epoch=j.module_epoch
      and c.expected_member_count=1 and c.enrolled_member_count=1)
    or exists(select 1 from private.bpay_next_case_hold h where h.run_worker_id=w.id and h.status='ACTIVE') then return false;end if;
  return true;
end $function$;
alter function private.bpay_next_umbrella_case_completed_transfer_valid_v2(private.bpay_next_run_worker,private.bpay_next_transfer) owner to postgres;
alter function private.bpay_next_umbrella_case_csv_transfer_valid_v2(private.bpay_next_run_worker,private.bpay_next_transfer) owner to postgres;
alter function private.bpay_next_umbrella_case_cancelled_projection_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection) owner to postgres;
revoke all on function private.bpay_next_umbrella_case_completed_transfer_valid_v2(private.bpay_next_run_worker,private.bpay_next_transfer),
  private.bpay_next_umbrella_case_csv_transfer_valid_v2(private.bpay_next_run_worker,private.bpay_next_transfer),
  private.bpay_next_umbrella_case_cancelled_projection_valid_v2(private.bpay_next_run_worker,private.bpay_next_net_projection)
  from public,anon,authenticated,service_role;
commit;
