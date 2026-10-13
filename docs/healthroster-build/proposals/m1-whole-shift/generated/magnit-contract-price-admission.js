// Proposal owner: server-resolved saved rules + actual dated Contract policy.
// No calculator, store, flag, source-price fallback or policy mutation.
export function assertMagnitContractPricing(values, policy) {
  if (!values || values.invoice_sender !== 'CLIENT_SELF_BILL'
      || values.checking_profile_kind !== 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') return false;
  if (values.self_bill_source !== 'SAME_FILE' || values.final_report_profile_kind !== 'NONE'
      || values.invoice_membership !== 'FINALISED_IN_CYCLE_WINDOW'
      || values.pay_membership !== 'ALL_FINALISED_IN_FILE'
      || values.invoice_amount_basis !== 'CONTRACT_CALCULATION' || values.source_amount_basis !== 'NONE'
      || values.rate_difference_policy !== 'NOT_APPLICABLE'
      || values.earliest_final_weekday !== null || values.earliest_final_local_time !== null) {
    const error = new Error('The weekly summary uses the whole-shift Contract calculation and its own cut-off.');
    error.code='WEEKLY_SOURCE_MAGNIT_PRESET_INVALID'; throw error;
  }
  if (policy?.weekly_rate_classification_method !== 'WHOLE_SHIFT_START_DAY') {
    const error = new Error('Set this client and each applicable Contract to Whole shift before using the weekly-summary self-bill preset.');
    error.code='WEEKLY_SOURCE_MAGNIT_WHOLE_SHIFT_REQUIRED'; throw error;
  }
  return true;
}

// Saved disabled preset. S14 and rate method live on the existing client/Contract
// policy and are deliberately not duplicate/new version fields. Nothing enables
// a group or changes that policy when this preset is constructed or saved.
export function magnitRuleSetPreset() {
  return Object.freeze({checking_profile_kind:'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1',invoice_sender:'CLIENT_SELF_BILL',
    self_bill_source:'SAME_FILE',final_report_profile_kind:'NONE',invoice_membership:'FINALISED_IN_CYCLE_WINDOW',
    pay_membership:'ALL_FINALISED_IN_FILE',invoice_amount_basis:'CONTRACT_CALCULATION',source_amount_basis:'NONE',
    line_vat_method:'ROUND_EACH_LINE_FROM_NET',invoice_vat_method:'SUM_OF_LINES',rate_difference_policy:'NOT_APPLICABLE',
    earliest_final_weekday:null,earliest_final_local_time:null});
}

// Qualifies output of the unchanged native calculator; does not recalculate
// rates, infer a source band or introduce an alternate category/price oracle.
export function assertMagnitWholeShiftResult(values, calculation) {
  if (!values || values.invoice_sender !== 'CLIENT_SELF_BILL'
      || values.checking_profile_kind !== 'ROSTER_WEEKLY_SUMMARY_ACTUAL_V1') return false;
  const buckets=['day','night','sat','sun','bh'];
  if (calculation?.mode !== 'WHOLE_SHIFT_START_DAY' || !buckets.includes(calculation.category)
      || !Number.isSafeInteger(calculation.paidMinutes) || calculation.paidMinutes<=0
      || buckets.some(bucket => calculation.bucketMinutes?.[bucket] !== (bucket===calculation.category?calculation.paidMinutes:0))) {
    const error=new Error('The whole shift must use the one Contract rate category selected by the native calculator.');
    error.code='WEEKLY_SOURCE_MAGNIT_BUCKET_INVALID'; throw error;
  }
  return true;
}
