-- Banking Pay bounded-scope Version 1.2.4 missing financial invalidation backstops.
-- Statement-level transition tables keep parent DML set based.  The trigger
-- adapter performs identity resolution and durable dirty-state coalescing only.

-- Retained row-trigger identities become BEFORE triggers so a validated
-- reconciliation declares the exact OLD/NEW transition and full digests in
-- pg_temp before the finance row is applied. External mutations still use the
-- same functions and roll their dirty event back if the parent DML fails.
DROP TRIGGER IF EXISTS trg_pay_workbench_mark_candidate_dirty__pay_advances
  ON public.pay_advances;
CREATE TRIGGER trg_pay_workbench_mark_candidate_dirty__pay_advances
BEFORE INSERT OR DELETE OR UPDATE ON public.pay_advances
FOR EACH ROW EXECUTE FUNCTION public.pay_workbench_mark_candidate_dirty();

DROP TRIGGER IF EXISTS trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com
  ON public.pay_finance_case_components;
CREATE TRIGGER trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_com
BEFORE INSERT OR DELETE OR UPDATE ON public.pay_finance_case_components
FOR EACH ROW EXECUTE FUNCTION public.pay_workbench_mark_finance_case_dirty();

DROP TRIGGER IF EXISTS trg_bpay_wb_observe_advances_insert ON public.pay_advances;
CREATE TRIGGER trg_bpay_wb_observe_advances_insert AFTER INSERT ON public.pay_advances
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_advances_update ON public.pay_advances;
CREATE TRIGGER trg_bpay_wb_observe_advances_update AFTER UPDATE ON public.pay_advances
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_advances_delete ON public.pay_advances;
CREATE TRIGGER trg_bpay_wb_observe_advances_delete AFTER DELETE ON public.pay_advances
REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_observe_components_insert ON public.pay_finance_case_components;
CREATE TRIGGER trg_bpay_wb_observe_components_insert AFTER INSERT ON public.pay_finance_case_components
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_components_update ON public.pay_finance_case_components;
CREATE TRIGGER trg_bpay_wb_observe_components_update AFTER UPDATE ON public.pay_finance_case_components
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_components_delete ON public.pay_finance_case_components;
CREATE TRIGGER trg_bpay_wb_observe_components_delete AFTER DELETE ON public.pay_finance_case_components
REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_observe_events_insert ON public.pay_finance_case_events;
CREATE TRIGGER trg_bpay_wb_observe_events_insert AFTER INSERT ON public.pay_finance_case_events
REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_events_update ON public.pay_finance_case_events;
CREATE TRIGGER trg_bpay_wb_observe_events_update AFTER UPDATE ON public.pay_finance_case_events
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
DROP TRIGGER IF EXISTS trg_bpay_wb_observe_events_delete ON public.pay_finance_case_events;
CREATE TRIGGER trg_bpay_wb_observe_events_delete AFTER DELETE ON public.pay_finance_case_events
REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve
  ON public.pay_finance_case_events;
CREATE TRIGGER trg_pay_workbench_mark_finance_case_dirty__pay_finance_case_eve
BEFORE INSERT OR DELETE OR UPDATE ON public.pay_finance_case_events
FOR EACH ROW EXECUTE FUNCTION public.pay_workbench_mark_finance_case_dirty();

DROP TRIGGER IF EXISTS trg_bpay_wb_batch_items_insert_dirty_v1 ON public.pay_batch_items;
CREATE TRIGGER trg_bpay_wb_batch_items_insert_dirty_v1
AFTER INSERT ON public.pay_batch_items
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_batch_items_update_dirty_v1 ON public.pay_batch_items;
CREATE TRIGGER trg_bpay_wb_batch_items_update_dirty_v1
AFTER UPDATE ON public.pay_batch_items
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_batch_items_delete_dirty_v1 ON public.pay_batch_items;
CREATE TRIGGER trg_bpay_wb_batch_items_delete_dirty_v1
AFTER DELETE ON public.pay_batch_items
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_breakdowns_insert_dirty_v1 ON public.pay_batch_item_breakdowns;
CREATE TRIGGER trg_bpay_wb_breakdowns_insert_dirty_v1
AFTER INSERT ON public.pay_batch_item_breakdowns
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_breakdowns_update_dirty_v1 ON public.pay_batch_item_breakdowns;
CREATE TRIGGER trg_bpay_wb_breakdowns_update_dirty_v1
AFTER UPDATE ON public.pay_batch_item_breakdowns
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_breakdowns_delete_dirty_v1 ON public.pay_batch_item_breakdowns;
CREATE TRIGGER trg_bpay_wb_breakdowns_delete_dirty_v1
AFTER DELETE ON public.pay_batch_item_breakdowns
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_snapshots_insert_dirty_v1 ON public.pay_batch_timesheet_snapshots;
CREATE TRIGGER trg_bpay_wb_snapshots_insert_dirty_v1
AFTER INSERT ON public.pay_batch_timesheet_snapshots
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_snapshots_update_dirty_v1 ON public.pay_batch_timesheet_snapshots;
CREATE TRIGGER trg_bpay_wb_snapshots_update_dirty_v1
AFTER UPDATE ON public.pay_batch_timesheet_snapshots
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_snapshots_delete_dirty_v1 ON public.pay_batch_timesheet_snapshots;
CREATE TRIGGER trg_bpay_wb_snapshots_delete_dirty_v1
AFTER DELETE ON public.pay_batch_timesheet_snapshots
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_transfer_events_insert_dirty_v1 ON public.pay_bank_transfer_events;
CREATE TRIGGER trg_bpay_wb_transfer_events_insert_dirty_v1
AFTER INSERT ON public.pay_bank_transfer_events
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_transfer_events_update_dirty_v1 ON public.pay_bank_transfer_events;
CREATE TRIGGER trg_bpay_wb_transfer_events_update_dirty_v1
AFTER UPDATE ON public.pay_bank_transfer_events
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_transfer_events_delete_dirty_v1 ON public.pay_bank_transfer_events;
CREATE TRIGGER trg_bpay_wb_transfer_events_delete_dirty_v1
AFTER DELETE ON public.pay_bank_transfer_events
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_reservations_insert_dirty_v1 ON public.pay_advance_reservations;
CREATE TRIGGER trg_bpay_wb_reservations_insert_dirty_v1
AFTER INSERT ON public.pay_advance_reservations
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_reservations_update_dirty_v1 ON public.pay_advance_reservations;
CREATE TRIGGER trg_bpay_wb_reservations_update_dirty_v1
AFTER UPDATE ON public.pay_advance_reservations
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_reservations_delete_dirty_v1 ON public.pay_advance_reservations;
CREATE TRIGGER trg_bpay_wb_reservations_delete_dirty_v1
AFTER DELETE ON public.pay_advance_reservations
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_batch_candidates_delete_dirty_v1 ON public.pay_batch_candidates;
CREATE TRIGGER trg_bpay_wb_batch_candidates_delete_dirty_v1
AFTER DELETE ON public.pay_batch_candidates
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_batches_delete_dirty_v1 ON public.pay_batches;
CREATE TRIGGER trg_bpay_wb_batches_delete_dirty_v1
AFTER DELETE ON public.pay_batches
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();

DROP TRIGGER IF EXISTS trg_bpay_wb_transfers_delete_dirty_v1 ON public.pay_bank_transfers;
CREATE TRIGGER trg_bpay_wb_transfers_delete_dirty_v1
AFTER DELETE ON public.pay_bank_transfers
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT EXECUTE FUNCTION private.pay_workbench_financial_scope_dirty_transition_v1();
