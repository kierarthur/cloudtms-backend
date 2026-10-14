// UNIT_ONLY synthetic owner-shaped frames, never native evidence or business rows.
const id = n => `00000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const I = { request: id(1), actor: id(2), original: id(3), p0: id(4), r0: id(5), candidate: id(6),
  client: id(7), root: id(8), originalFin: id(9), pFin: id(10), rFin: id(11), correction: `awc:${id(8)}:${id(1)}`, invoice: id(13), deleteOp: id(14) };
const H = 'a'.repeat(64), FH = 'b'.repeat(64);
const copy = x => JSON.parse(JSON.stringify(x));
const action = (rev = 1) => ({ request_id: I.request, expected_state_revision: rev });
function finance(ts = I.original, version = 7) {
  return {
    id: I.originalFin, is_current: true, timesheet_id: ts, timesheet_version: version,
    candidate_id: I.candidate, client_id: I.client, is_stale: false, has_rate_issue: false, has_pay_channel_issue: false,
    processing_status: 'READY_FOR_INVOICE', basis: 'SELF_REPORTED', pay_method: 'PAYE', role: 'Nurse', band: '6',
    occupant_key_norm: 'historical-occupant', policy_snapshot_json: { revision: 9, erni_pct: 15.05, no_timesheet_required: false },
    rate_source_refs_json: { kind: 'CONTRACT', override_id: id(15), historic_version: 12 },
    pay_day: 21.35, charge_day: 32.8, pay_night: 27.11, charge_night: 44.03,
    pay_sat: 29, charge_sat: 45, pay_sun: 33, charge_sun: 49, pay_bh: 41, charge_bh: 59,
    hours_day: '6.2500', hours_night: 1.5, hours_sat: 0, hours_sun: null, hours_bh: '0.00', total_hours: '7.7500',
    total_pay_ex_vat: '177.1400', total_charge_ex_vat: '271.0450', margin_ex_vat: '67.2444',
    pay_vat_rate_pct_snapshot: '20.00', pay_vat_amount_snapshot: '35.4280', pay_total_inc_vat_snapshot: '212.5680',
    pay_wtr_rate_pct_snapshot: '12.07',
    expenses_pay_ex_vat: '2.01', expenses_charge_ex_vat: '3.02', travel_pay_ex_vat: '4.03', travel_charge_ex_vat: '5.04',
    accommodation_pay_ex_vat: '6.05', accommodation_charge_ex_vat: '7.06', other_pay_ex_vat: '8.07', other_charge_ex_vat: '9.08',
    mileage_units: '11.2500', mileage_pay_rate: '0.4500', mileage_charge_rate: '0.5500',
    mileage_pay_ex_vat: '5.0625', mileage_charge_ex_vat: '6.1875',
    additional_pay_ex_vat: '12.3500', additional_charge_ex_vat: '14.0000', additional_margin_ex_vat: '-0.2087',
    additional_units_json: { ONCALL: { code: 'ONCALL', bucket_name: 'On call', unit_name: 'visits', unit_count: '2.50',
      pay_rate: '4.9400', charge_rate: '5.6000', pay_ex_vat: '12.3500', charge_ex_vat: '14.0000',
      margin_ex_vat: '-0.2087', pay_cost_ex_vat_including_erni: '14.2087', days: { '2026-10-12': '2.50' } } },
    invoice_breakdown_json: { mode: 'AGGREGATE', base_hours: {
      day: '6.2500', night: 1.5, sat: 0, sun: null, bh: '0.00',
      pay_rates: { day: 21.35, night: 27.11, sat: 29, sun: 33, bh: 41 },
      charge_rates: { day: 32.8, night: 44.03, sat: 45, sun: 49, bh: 59 },
      pay_ex_vat: '164.7900', charge_ex_vat: '257.0450'
    }, additional: { units: {}, pay_ex_vat: '12.3500', charge_ex_vat: '14.0000', margin_ex_vat: '-0.2087' },
    totals: { total_pay_ex_vat: '177.1400', total_charge_ex_vat: '271.0450', margin_ex_vat: '67.2444', vat_amount: '54.2090', total_inc_vat: '325.2540' } },
    worked_start_iso: '2026-10-12T20:00:00+01:00', worked_end_iso: '2026-10-13T04:00:00+01:00', break_minutes: 15,
    authorised_at_utc: '2026-10-12T10:00:00Z', authorised_by_user_id: I.actor,
    paid_at_utc: '2026-10-13T10:00:00Z', paid_by_user_id: I.actor, payment_reference: 'historic-payment',
    locked_by_invoice_id: I.invoice, locked_at_utc: '2026-10-13T12:00:00Z',
    created_at: '2026-10-12T10:00:00Z', computed_at_utc: '2026-10-12T10:00:00Z'
  };
}
function prepareFixture() {
  return {
    ok: true, contract_version: 'TIMESHEET_CORRECTION_PREPARE_V1', prepare_fingerprint: H,
    candidate_id: I.candidate, client_id: I.client, original_materialisation_required: false, original_authorisation_required: false,
    authority: { request_id: I.request, state_revision: 1, target_revision: H, proposal_revision: 1,
      root_timesheet_id: I.root, origin: 'SIGNED_TIMESHEET_CORRECTION', original_timesheet_id: I.original,
      original_timesheet_version: 7, original_financial_id: I.originalFin, original_financial_authority_hash: FH,
      replacement_timesheet_id: I.p0, replacement_version: 3, replacement_financial_id: null,
      replacement_financial_authority_hash: null, replacement_schedule: [{ work_date: '2026-10-12', start_time: '20:00', end_time: '04:00' }],
      replacement_signed_witness: { workflow_id: id(16), workflow_generation: 2, approval_request_id: id(17),
        hours_component_id: id(18), candidate_component_id: id(19), manager_component_id: id(20),
        candidate_sha256: H, manager_sha256: FH, manager_approved_at_utc: '2026-10-13T11:20:00Z', document_version_id: null },
      planned_reversal_timesheet_id: I.r0, planned_reversal_week_id: null, original_invoice_id: I.invoice, return_invoice_id: null },
    frozen_original: { timesheet: { timesheet_id: I.original, version: 7, booking_id: 'CANDIDATE-WEEKLY-original' },
      financial_authority_hash: FH, financial_snapshot: finance(), planned_reversal_timesheet_id: I.r0 },
    original_current_snapshot: finance(), replacement_current_snapshot: null,
    replacement_timesheet: { timesheet_id: I.p0, version: 3, parent_timesheet_id: I.root, booking_id: 'bk_awc_exact_p0',
      correction_id: I.correction, correction_kind: 'WHOLE_TIMESHEET_REPLACEMENT', is_current: true, archived_at_utc: null, authorised_at_server: null }
  };
}
function replacementFixture() {
  const f = finance(I.p0, 3);
  for (const k of ['id', 'authorised_at_utc', 'authorised_by_user_id', 'paid_at_utc', 'paid_by_user_id', 'payment_reference', 'locked_by_invoice_id', 'locked_at_utc']) delete f[k];
  return f;
}
function appliedFixture(overrides = {}) {
  const members = [I.r0, I.p0];
  return { ok: true, state: 'APPLIED', request_id: I.request, correction_id: I.correction,
    reversed_timesheet_id: I.original, reversal_timesheet_id: I.r0, replacement_timesheet_id: I.p0,
    state_revision: 2, prepare_fingerprint: H, original_authorisation_required: false, original_materialised_in_unit: false,
    original_timesheet_authority_hash: H, reversal_timesheet_authority_hash: H, replacement_timesheet_authority_hash: H,
    original_financial_id: I.originalFin, original_financial_authority_hash: FH,
    reversal_financial_id: I.rFin, reversal_financial_authority_hash: H, replacement_financial_id: I.pFin, replacement_financial_authority_hash: H,
    ordinary_notification_timesheet_ids: [I.p0],
    authorisation: { ok: true, all_success: true, success_count: 2, failure_count: 0,
      results: members.map(current_timesheet_id => ({ current_timesheet_id, action: 'AUTHORISE', success: true })) }, ...overrides };
}
function cancelledFixture(overrides = {}) {
  return { ok: true, state: 'CANCELLED', request_id: I.request, state_revision: 2, replacement_timesheet_id: I.p0,
    delete_operation_id: I.deleteOp, retained_immutable_component_asset_count: 3,
    lawful_removal: { ok: true, decision: 'PERMANENT_DELETE', apply_performed: true, deleted_timesheet_ids: [I.p0],
      r2_cleanup_keys: ['exact/p0/mutable.pdf'] }, ...overrides };
}

export { id, I, H, FH, copy, action, finance, prepareFixture, replacementFixture, appliedFixture, cancelledFixture };
