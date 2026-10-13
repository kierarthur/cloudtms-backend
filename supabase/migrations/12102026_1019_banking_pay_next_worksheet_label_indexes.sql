-- DR21 / BD141: additive private display evidence/indexes. No backfill or money change.

\set ON_ERROR_STOP on

begin;

alter table private.bpay_next_selection_candidate
 add column captured_candidate_display_name text check(octet_length(captured_candidate_display_name)<=8192),
 add column captured_candidate_reference text check(octet_length(captured_candidate_reference)<=8192),
 add column captured_candidate_surname text check(octet_length(captured_candidate_surname)<=8192);
-- Prefixes support scoped label lookups; full-key residual ordering preserves collisions.
create index bpay_next_worksheet_surname_asc_idx on private.bpay_next_selection_candidate
 (run_id,(captured_candidate_surname is null),(left(lower(captured_candidate_surname),512) collate "C") asc,candidate_id);
create index bpay_next_worksheet_surname_desc_idx on private.bpay_next_selection_candidate
 (run_id,(captured_candidate_surname is null),(left(lower(captured_candidate_surname),512) collate "C") desc,candidate_id);
create index bpay_next_worksheet_projection_idx on private.bpay_next_net_projection
 (run_worker_id,projection_no) include(retired_at_utc,entered_paye_net,cash_amount,input_kind,request_command_id,accepted_recoveries,accepted_net_additions);
create index bpay_next_worksheet_request_idx on private.bpay_next_paye_net_request
 (run_worker_id,request_no) include(command_id,entered_paye_net,input_kind);
create index bpay_next_worksheet_state_idx on private.bpay_next_case_allocation_state
 (run_worker_id,preparation_revision,selection_revision,pass_kind,projection_no)
 include(status,completed_at_utc,prepare_stage,net_stage,projection_id,captured_work_gross,allocated_gross_additions,allocated_gross_deductions,allocated_net_additions);
create index bpay_next_worksheet_instruction_idx on private.bpay_next_run_case_instruction
 (run_worker_id,preparation_revision,selection_revision,payroll_stage,case_kind,id) include(case_subtype,instruction_kind);
create index bpay_next_worksheet_result_idx on private.bpay_next_case_allocation_result
 (state_id,instruction_id) include(run_worker_id,candidate_id,preparation_revision,selection_revision,allocated_target_ex_vat);

commit;
