-- One-time CloudTMS schema/data migration: banking_pay_next_captured_other_draft_capacity
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- DR14 observational evidence only. Old immutable instructions remain NULL;
-- no historical amount is guessed and no business row is rewritten.
alter table private.bpay_next_run_case_instruction
  add column other_run_capacity_reduction_source_ex_vat private.bpay_next_penny_amount,
  add column other_run_capacity_reduction_target_ex_vat private.bpay_next_penny_amount,
  add column other_run_capacity_reason text,
  add constraint bpay_next_instruction_other_run_capacity_shape check (
    (other_run_capacity_reduction_source_ex_vat is null and other_run_capacity_reduction_target_ex_vat is null and other_run_capacity_reason is null)
    or (other_run_capacity_reduction_source_ex_vat is not null and other_run_capacity_reduction_target_ex_vat is not null
      and other_run_capacity_reduction_source_ex_vat>=0 and other_run_capacity_reduction_target_ex_vat>=0
      and other_run_capacity_reduction_source_ex_vat<=nominal_source_ex_vat-capacity_source_ex_vat
      and other_run_capacity_reduction_target_ex_vat<=nominal_target_ex_vat-capacity_target_ex_vat
      and ((other_run_capacity_reduction_source_ex_vat=0 and other_run_capacity_reduction_target_ex_vat=0 and other_run_capacity_reason is null)
        or (other_run_capacity_reduction_source_ex_vat>0 and other_run_capacity_reduction_target_ex_vat>0
          and other_run_capacity_reason is not null and other_run_capacity_reason in ('SAME_WEEK_INSTALMENT','CASE_BALANCE')))));

-- The existing active Case-hold seek supports the capture-only attribution.
-- This native Candidate/owner seek identifies a current lease without a scan
-- of the Candidate directory or a second maintained ownership pointer.
create index bpay_next_job_candidate_live_owner_idx
  on private.bpay_next_job(candidate_id,owner_epoch,module_epoch,id)
  include(job_kind,lease_until_utc,lease_nonce,command_id,retry_run_id)
  where status='LEASED';
create index bpay_next_instruction_other_run_capacity_idx
  on private.bpay_next_run_case_instruction(run_worker_id,preparation_revision,id)
  where other_run_capacity_reduction_target_ex_vat>0;

commit;
