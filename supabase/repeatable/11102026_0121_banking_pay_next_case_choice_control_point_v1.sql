-- WB26 exact native Case control point; installed0102 remains unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_selection_memory_leaves_v1(p_scope jsonb)
returns table(leaf_key text,candidate_id uuid,work_id uuid,revision_id uuid,approved_line_id uuid,case_component_id uuid,direction text,basis jsonb)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select case when i.work_id is not null then 'WORK:'||i.work_id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
  else 'CASE:'||i.case_component_id::text end,i.candidate_id,i.work_id,i.revision_id,i.approved_line_id,i.case_component_id,i.direction,
 jsonb_build_object('revision_id',i.revision_id,'approved_line_id',i.approved_line_id,
  'approved_segment_id',i.approved_segment_id,'component_key',i.component_key,'segment_key',i.segment_key,
  'member_kind',i.member_kind,'source_pay_channel',i.source_pay_channel,'target_pay_channel',i.target_pay_channel,
  'direction',i.direction,'payroll_stage',i.payroll_stage,'approved_quantity',i.approved_quantity,'unit_label',i.unit_label,
  'approved_source_ex_vat',i.approved_source_ex_vat,'realised_source_ex_vat',i.realised_source_ex_vat,
  'active_reserved_source_ex_vat',i.active_reserved_source_ex_vat,'included_source_ex_vat',i.included_source_ex_vat,
  'deduction_balance_source_ex_vat',i.deduction_balance_source_ex_vat,'snooze_id',i.snooze_id,
  'snooze_version',i.snooze_version,'snooze_until_date',i.snooze_until_date,'pay_hold',
  exists(select 1 from private.bpay_next_work hold_work join public.timesheets hold_sheet on hold_sheet.booking_id=hold_work.booking_id and hold_sheet.is_current=true
   join public.timesheets_financials hold_financial on hold_financial.timesheet_id=hold_sheet.timesheet_id and hold_financial.is_current=true
   where hold_work.id=i.work_id and hold_financial.pay_on_hold is true),
  'query_state',i.query_state,'query_qualified',i.query_qualified,'rule_id',i.rule_id,
  'case_component_revision',cc.component_revision,'case_rule_revision',cr.rule_revision,
  'case_threshold',cr.minimum_earnings_threshold,'case_floor',cr.take_home_floor_override,
  'case_weekly_due',cr.weekly_due_source_ex_vat,'case_next_due',cr.next_due_monday,
  'week_ending_date',i.week_ending_date,'work_date',i.work_date,
  'target_umbrella_id',candidate.umbrella_id,'target_umbrella_vat_chargeable',umbrella.vat_chargeable,
  'min_take_home_wtd',candidate.min_take_home_wtd)||case when cc.id is not null then jsonb_build_object('case_id',cc.case_id,'case_component_id',cc.id,'case_revision',fc.case_revision,
  'case_state',fc.status,'case_due_date',fc.due_date,'case_kind',cc.case_kind,
  'case_subtype',cc.case_subtype,'case_tax_treatment',cc.tax_treatment,
  'case_instruction_kind',cc.instruction_kind,'case_component_ordinal',cc.component_ordinal,
  'case_control_sequence',coalesce((select sn.control_sequence
    from private.bpay_next_snooze sn where sn.case_component_id=cc.id order by sn.control_sequence desc limit 1),0)) else '{}'::jsonb end
 from private.bpay_next_workbench_item i join private.bpay_next_workbench_group g
  on g.group_key=i.group_key and g.candidate_id=i.candidate_id
 join public.candidates candidate on candidate.id=i.candidate_id
 left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
 left join private.bpay_next_case_component cc on cc.id=i.case_component_id
 left join private.bpay_next_case_rule cr on cr.id=i.rule_id and cr.case_id=cc.case_id and cr.candidate_id=cc.candidate_id
 left join private.bpay_next_finance_case fc on fc.id=cc.case_id and fc.candidate_id=cc.candidate_id
 where i.section='READY' and i.selectable and i.amount_known and not i.is_selected
  and i.portion_kind in ('AVAILABLE','ZERO') and g.complete and not g.dirty
  and g.progress_state='CURRENT' and i.progress_state='CURRENT'
  and i.projection_revision=g.projection_revision and i.financial_view_revision=g.financial_view_revision
  and ((i.work_id is not null and i.approved_line_id is not null) or i.case_component_id is not null)
  and (p_scope->>'candidate_id' is null or i.candidate_id=(p_scope->>'candidate_id')::uuid)
  and (p_scope->>'client_id' is null or i.client_id=(p_scope->>'client_id')::uuid)
  and (p_scope->>'channel'='ALL' or i.target_pay_channel=p_scope->>'channel')
;
$function$;
alter function private.bpay_next_selection_memory_leaves_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_selection_memory_leaves_v1(jsonb) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
