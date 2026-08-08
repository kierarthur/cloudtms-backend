const RECOVERY_LINE_TYPES = new Set([
  'MANUAL_DEBT_RECOVERY',
  'OVERPAYMENT_RECOVERY',
  'LOAN_REPAYMENT',
  'PAYMENT_ADVANCE_REPAYMENT'
]);

const text = (value) => String(value == null ? '' : value).trim();
const upper = (value) => text(value).toUpperCase();
const toPence = (value) => {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? Math.round(numberValue * 100) : null;
};

export function evaluateCreateDraftRecoveryHeadroom(contracts = []) {
  const groups = new Map();

  for (const contractLike of Array.isArray(contracts) ? contracts : []) {
    const contract = contractLike && typeof contractLike === 'object' && !Array.isArray(contractLike)
      ? contractLike
      : {};
    const candidateId = text(contract.candidate_id);
    const payChannel = upper(contract.pay_channel);
    const lineType = upper(contract.line_type);
    const amountPence = toPence(contract.amount_ex_vat);
    if (!candidateId || !['PAYE', 'UMBRELLA'].includes(payChannel) || amountPence === null) continue;

    const groupKey = `${candidateId}|${payChannel}`;
    const current = groups.get(groupKey) || {
      candidate_id: candidateId,
      pay_channel: payChannel,
      positive_pence: 0,
      recovery_pence: 0,
      uncertified_recovery_pence: 0,
      recovery_row_count: 0,
      certified_recovery_row_count: 0
    };

    if (amountPence > 0) current.positive_pence += amountPence;
    if (amountPence < 0 && RECOVERY_LINE_TYPES.has(lineType)) {
      const recoveryPence = Math.abs(amountPence);
      const certifiedRecoverablePence = toPence(contract.recoverable_this_pay_run_ex_vat);
      const certifiedByCurrentPreDraftAuthority = upper(contract.policy_x_authority_scope) === 'PRE_DRAFT_LIVE_TRUTH'
        && certifiedRecoverablePence !== null
        && certifiedRecoverablePence >= recoveryPence;

      current.recovery_pence += recoveryPence;
      current.recovery_row_count += 1;
      if (certifiedByCurrentPreDraftAuthority) {
        current.certified_recovery_row_count += 1;
      } else {
        current.uncertified_recovery_pence += recoveryPence;
      }
    }

    groups.set(groupKey, current);
  }

  const invalidGroups = [];
  for (const totals of groups.values()) {
    const allRecoveriesCertified = totals.recovery_row_count > 0
      && totals.certified_recovery_row_count === totals.recovery_row_count;
    const selectedPositiveRequirementPence = allRecoveriesCertified
      ? 0
      : totals.recovery_pence;
    if (selectedPositiveRequirementPence > totals.positive_pence) {
      invalidGroups.push({
        candidate_id: totals.candidate_id,
        pay_channel: totals.pay_channel,
        retained_selected_positive_pay_ex_vat: totals.positive_pence / 100,
        selected_finance_recovery_ex_vat: totals.recovery_pence / 100,
        uncertified_finance_recovery_ex_vat: totals.uncertified_recovery_pence / 100,
        selected_positive_requirement_ex_vat: selectedPositiveRequirementPence / 100,
        selected_finance_recovery_row_count: totals.recovery_row_count,
        certified_finance_recovery_row_count: totals.certified_recovery_row_count
      });
    }
  }

  return {
    ok: invalidGroups.length === 0,
    invalid_groups: invalidGroups,
    group_count: groups.size
  };
}
