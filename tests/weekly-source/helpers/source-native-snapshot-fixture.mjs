// Complete native-shaped test input for transport/economic tests. Actual
// calculator output is separately executed in protected-component-calculator-owner.
export function nativeSnapshotFixture({ timesheet_id, candidate_id, client_id, contract_id, policy = {}, timesheet_version = 1 }, overrides = {}) {
  return { timesheet_id,timesheet_version,basis:'CONTRACT_WEEKLY',candidate_id,client_id,role:null,band:null,pay_method:'PAYE',
    policy_snapshot_json:policy,rate_source_refs_json:{mode:'CONTRACT_RATES_JSON',contract_id},
    hours_day:0,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,total_hours:0,
    pay_day:0,pay_night:0,pay_sat:0,pay_sun:0,pay_bh:0,charge_day:0,charge_night:0,charge_sat:0,charge_sun:0,charge_bh:0,
    additional_units_json:{},additional_pay_ex_vat:0,additional_charge_ex_vat:0,additional_margin_ex_vat:0,
    total_pay_ex_vat:0,total_charge_ex_vat:0,margin_ex_vat:0,expenses_pay_ex_vat:0,expenses_charge_ex_vat:0,
    expenses_description:null,expenses_evidence_r2_key:null,expenses_evidence_manifest:null,mileage_units:0,mileage_pay_ex_vat:0,
    mileage_charge_ex_vat:0,mileage_evidence_r2_key:null,mileage_evidence_manifest:null,mileage_pay_rate:null,mileage_charge_rate:null,
    candidate_assignment:'ASSIGNED',processing_status:'PENDING_AUTH',
    invoice_breakdown_json:{mode:'SEGMENTS',segments:[],totals:{total_pay_ex_vat:0,total_charge_ex_vat:0,margin_ex_vat:0}},...overrides };
}
export function nativeSegmentFixture(event, overrides = {}) {
  return {segment_id:`weekly-source-event:${event}`,date:'2026-10-12',start:'09:00',end:'17:00',overnight:false,
    start_utc:'2026-10-12T08:00:00.000Z',end_utc:'2026-10-12T16:00:00.000Z',break_mins:30,ref_num:null,breaks:[],
    hours_day:7.5,hours_night:0,hours_sat:0,hours_sun:0,hours_bh:0,pay_amount:90,charge_amount:150,is_reversal:false,exclude_from_pay:false,
    weekly_protected_component_identity:{schema_version:'WEEKLY_PROTECTED_COMPONENT_IDENTITY_V1',work_event_id:event},...overrides};
}
