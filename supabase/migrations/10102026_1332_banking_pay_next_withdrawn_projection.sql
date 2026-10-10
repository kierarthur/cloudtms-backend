-- WB02: a known retained approval display is neither payable nor certified zero.
\set ON_ERROR_STOP on
begin;
alter table private.bpay_next_workbench_item
  drop constraint bpay_next_workbench_item_portion_kind_check,
  add constraint bpay_next_workbench_item_portion_kind_check
    check(portion_kind in ('AVAILABLE','RESERVED','RECOVERY','ZERO','EVIDENCE','WITHDRAWN')),
  add constraint bpay_next_workbench_withdrawn_shape check(portion_kind<>'WITHDRAWN' or
    (member_kind='WORK_COMPONENT' and amount_known and direction='INFORMATIONAL' and not selectable
      and section in ('ACTION','BLOCKED') and reason_code in ('AWAITING_REAUTHORISATION','APPROVAL_WITHDRAWN')
      and nominal_source_ex_vat=approved_source_ex_vat and residual_source_ex_vat=approved_source_ex_vat-realised_source_ex_vat-active_reserved_source_ex_vat
      and gross_due_source_ex_vat=0 and included_source_ex_vat=0 and excluded_source_ex_vat=0
      and recovery_source_ex_vat=0 and held_overhang_source_ex_vat=0 and written_off_source_ex_vat=0));
commit;
