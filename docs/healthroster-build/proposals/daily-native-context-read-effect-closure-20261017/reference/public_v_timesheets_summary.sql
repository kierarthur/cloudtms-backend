create or replace view public.v_timesheets_summary
with (security_invoker=true) as
select
  v.timesheet_id,
  v.timesheet_status,
  v.week_ending_date,
  v.booking_id,
  v.occupant_key_norm,
  v.hospital_norm,
  v.sheet_scope,
  v.submission_mode,
  v.authorised_at_server,
  v.candidate_id,
  v.client_id,
  v.pay_method,
  v.processing_status,
  v.basis,
  v.total_hours,
  v.total_pay_ex_vat,
  v.total_charge_ex_vat,
  v.margin_ex_vat,
  v.paid_at_utc,
  v.pay_on_hold,
  v.ready_to_pay,
  v.locked_by_invoice_id,
  v.candidate_name,
  v.client_name,
  v.nhsp_shift_count,
  v.nhsp_shift_included_count,
  v.nhsp_shift_deferred_count,
  v.validation_status,
  v.summary_stage,
  v.route_type,
  v.contract_week_id,
  v.contract_week_ending_date,
  v.contract_week_status,
  v.additional_seq,

  -- Effective adjustment: do not let timesheets.is_adjustment = false mask linked contract_week adjustment truth
  (
    COALESCE(ts2.is_adjustment, FALSE) = TRUE
    OR COALESCE(cw.is_adjustment, FALSE) = TRUE
    OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0
    OR COALESCE(v.is_adjustment, FALSE) = TRUE
    OR ts2.parent_timesheet_id IS NOT NULL
    OR ts2.correction_id IS NOT NULL
    OR ts2.correction_kind IS NOT NULL
  ) as is_adjustment,

  v.qr_status,
  v.pay_adjustment_count,
  v.has_pay_adjustments,

  -- Keep is_adjusted consistent with effective adjustment truth
  (
    COALESCE(ts2.is_adjustment, FALSE) = TRUE
    OR COALESCE(cw.is_adjustment, FALSE) = TRUE
    OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0
    OR COALESCE(v.is_adjustment, FALSE) = TRUE
    OR ts2.parent_timesheet_id IS NOT NULL
    OR ts2.correction_id IS NOT NULL
    OR ts2.correction_kind IS NOT NULL
    OR COALESCE(v.has_pay_adjustments, FALSE) = TRUE
  ) as is_adjusted,

  v.is_qr,
  v.needs_attention,
  v.client_autoprocess_hr,
  v.has_rate_issue,
  v.has_pay_channel_issue,
  v.hr_crosscheck_status,
  v.hr_crosscheck_issues,
  v.external_source_rows_json,
  v.issue_codes,

  -- contract_id kept in same position (resolved for DAILY too)
  coalesce(ts2.contract_id, cw.contract_id) as contract_id,

  v.client_requires_hr,
  v.client_no_timesheet_required,
  v.client_is_nhsp,

  v.client_pay_reference_required,
  v.client_invoice_reference_required,
  v.client_hr_validation_required,
  v.client_ts_reference_required,

  v.require_reference_to_pay,
  v.require_reference_to_invoice,

  v.candidate_hint_text,

  v.expenses_pay_ex_vat,
  v.expenses_description,
  v.mileage_units,
  v.mileage_pay_rate,
  v.mileage_charge_rate,
  v.mileage_pay_ex_vat,

  v.travel_pay_ex_vat,
  v.travel_charge_ex_vat,
  v.accommodation_pay_ex_vat,
  v.accommodation_charge_ex_vat,
  v.other_pay_ex_vat,
  v.other_charge_ex_vat,

  -- existing appended column
  case
    when v.locked_by_invoice_id is null then null
    when inv.id is null then 'INVOICED_NOT_ISSUED'
    when inv.status in ('ISSUED'::public.invoice_status_enum, 'PAID'::public.invoice_status_enum) then 'INVOICED_ISSUED'
    else 'INVOICED_NOT_ISSUED'
  end as invoice_issue_stage,

  v.invoice_segments_total,
  v.invoice_segments_locked,
  v.invoice_segments_unlocked,
  v.invoice_segment_stage,

  v.tools_stage,
  v.processing_status_display,
  v.invoice_is_paid,

  v.refs_block_invoicing,
  v.refs_block_issuing_invoices,
  v.refs_block_invoice_and_issuing,

  -- ✅ NEW (APPENDED AT END ONLY): route_display (safe to add now)
  case
    when (
      COALESCE(ts2.is_adjustment, FALSE) = TRUE
      OR COALESCE(cw.is_adjustment, FALSE) = TRUE
      OR COALESCE(cw.additional_seq, v.additional_seq, 0) > 0
      OR COALESCE(v.is_adjustment, FALSE) = TRUE
      OR ts2.parent_timesheet_id IS NOT NULL
      OR ts2.correction_id IS NOT NULL
      OR ts2.correction_kind IS NOT NULL
    ) then
      case
        when upper(coalesce(ts2.adjustment_origin, '')) = 'MANUAL_ADJUSTMENT' then 'Manual Adjustment'
        when upper(coalesce(ts2.adjustment_origin, '')) like 'IMPORT_%'
          or ts2.correction_kind is not null
          or ts2.correction_id is not null
          then 'NHSP Adjustment'
        else
          -- Back-compat for historical manual adjustments where adjustment_origin is NULL
          'Manual Adjustment'
      end
    else
      case
        when v.route_type = 'DAILY_ELECTRONIC' then 'Daily Electronic'
        when v.route_type = 'DAILY_MANUAL' then 'Daily Manual'
        when v.route_type = 'WEEKLY_ELECTRONIC' then 'Weekly Electronic'
        when v.route_type = 'WEEKLY_MANUAL' then 'Weekly Manual'
        when v.route_type = 'WEEKLY_NHSP' then 'Weekly NHSP'
        when v.route_type = 'WEEKLY_NHSP_ADJUSTMENT' then 'Weekly NHSP'
        when v.route_type = 'WEEKLY_HEALTHROSTER' then 'Weekly HealthRoster'
        else 'Unknown'
      end
  end as route_display,

  -- ✅ NEW (APPENDED AT END ONLY): pay status rollup fields from v_timesheets_summary_base
  v.pay_icon_code,
  v.pay_status_code,
  v.pay_paid_at_utc,
  v.net_delta_ex_vat

from public.v_timesheets_summary_base v
left join public.contract_weeks cw
  on cw.id = v.contract_week_id
left join public.timesheets ts2
  on ts2.timesheet_id = v.timesheet_id
left join public.invoices inv
  on inv.id = v.locked_by_invoice_id
;