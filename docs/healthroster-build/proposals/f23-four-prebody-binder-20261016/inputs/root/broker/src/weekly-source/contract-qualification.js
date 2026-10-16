function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

const text = (value) => String(value ?? '').trim();
const upper = (value) => text(value).toUpperCase();

function dateInContract(workDate, contract) {
  const date = text(workDate).slice(0, 10);
  const from = text(contract?.validFrom).slice(0, 10);
  const to = text(contract?.validTo).slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(date)
    && /^\d{4}-\d{2}-\d{2}$/.test(from)
    && date >= from
    && (!to || date <= to);
}

function orderedUniqueContracts(contracts) {
  const seen = new Set();
  return [...contracts]
    .sort((a, b) => text(a.contractId).localeCompare(text(b.contractId), 'en', { sensitivity: 'variant' }))
    .filter((contract) => {
      const id = text(contract.contractId);
      if (!id || seen.has(id)) return false;
      seen.add(id);
      return true;
    });
}

/**
 * Pure qualification/cardinality owner. The caller supplies only exact
 * candidate/client/date candidates and canonical price observations. This
 * function never infers from source money, fuzzy names, UUID order, pay type,
 * pay rate or a historical Timesheet calculation.
 */
export function qualifyWeeklySourceContract(input = {}) {
  const sourceMode = upper(input.sourceMode);
  if (!['NHSP_WEEKLY', 'HEALTHROSTER_WEEKLY'].includes(sourceMode)) {
    fail('WEEKLY_SOURCE_MODE_INVALID', 'The weekly source mode is invalid.');
  }
  const candidateId = text(input.candidateId);
  const clientId = text(input.clientId);
  const workDate = text(input.workDate).slice(0, 10);
  if (!candidateId || !clientId || !/^\d{4}-\d{2}-\d{2}$/.test(workDate)) {
    fail('WEEKLY_SOURCE_QUALIFICATION_SCOPE_INVALID', 'Candidate, client and work date are required.');
  }

  // Plan 6.2 `25 §7` Removed: "Rejecting an otherwise unique safe Contract
  // merely because band/role text differs." `24 §8` makes the shortlist exact
  // Candidate, exact actual Client, worked date inside the Contract's active
  // dates, and source route applicable to that Contract. Band and role are
  // never part of that base set (XSG-011, G6-14).
  const base = orderedUniqueContracts(Array.isArray(input.contracts) ? input.contracts : []).filter((contract) => (
    text(contract.candidateId) === candidateId
    && text(contract.clientId) === clientId
    && contract.weeklySourceApplicable === true
    && dateInContract(workDate, contract)
  ));

  // PHD-019 / PRC-049: price is validation evidence, never Contract identity.
  // A zero source charge or genuine disparity therefore cannot remove an
  // otherwise valid Candidate/Client/date/source-route Contract.  A missing or
  // unsafe canonical Contract calculation remains a later hard blocker for the
  // selected Contract, but it cannot silently choose another Contract here.
  let eligible = base;
  const unverifiableExcludedIds = [];

  if (eligible.length === 0) {
    return Object.freeze({
      state: 'NO_ELIGIBLE_CONTRACT',
      selectedContractId: null,
      selectionMethod: null,
      baseContractIds: base.map((contract) => text(contract.contractId)),
      eligibleContractIds: [],
      unverifiableContractIds: unverifiableExcludedIds,
      narrowedBy: [],
      choices: [],
    });
  }

  const baseIds = base.map((contract) => text(contract.contractId));
  const qualifiedIds = eligible.map((contract) => text(contract.contractId));

  // An explicit Office decision and a durable prior relationship both outrank
  // an automatic tie-break (`24 §9` order: explicit relationship first,
  // Office confirmation last), so they are validated against the full
  // price/route-qualified set, before any narrowing.
  const selected = text(input.officeSelectedContractId);
  if (selected) {
    if (!qualifiedIds.includes(selected)) {
      fail('WEEKLY_SOURCE_CONTRACT_SELECTION_STALE', 'The selected Contract no longer matches this shift.');
    }
    return Object.freeze({
      state: 'RESOLVED',
      selectedContractId: selected,
      // WP-37 (WP-31 finding F6). `24 §8`: "If one Contract remains, the system
      // selects it… The system must never offer a chooser when only one
      // Contract is eligible", and the server proves exactly that — it refuses
      // `OFFICE_SELECTED` with `WEEKLY_SOURCE_OFFICE_CHOICE_NOT_WARRANTED`
      // unless at least two Contracts qualify. The Office screen resends its
      // accumulated selection map on every re-accept, so a choice made while
      // two were eligible used to refuse the WHOLE upload once that row
      // narrowed to one. A selection that agrees with the only eligible
      // Contract is not a choice: it is the unique answer, and it is reported
      // as such so the broker and the server agree.
      selectionMethod: qualifiedIds.length === 1 ? 'AUTO_UNIQUE' : 'OFFICE_SELECTED',
      baseContractIds: baseIds,
      eligibleContractIds: qualifiedIds,
      unverifiableContractIds: unverifiableExcludedIds,
      narrowedBy: [],
      choices: [],
    });
  }

  const prior = text(input.priorAcceptedContractId);
  if (prior && qualifiedIds.includes(prior)) {
    return Object.freeze({
      state: 'RESOLVED',
      selectedContractId: prior,
      selectionMethod: 'DURABLE_LINEAGE',
      baseContractIds: baseIds,
      eligibleContractIds: qualifiedIds,
      unverifiableContractIds: unverifiableExcludedIds,
      narrowedBy: [],
      choices: [],
    });
  }

  // `25 §7`: "Otherwise use Candidate, actual Client, worked date, compatible
  // schedule, Contract validity and prior lineage… If several remain, use
  // verified band/role only as a tie-breaker." `24 §8`: a label filter that
  // leaves nothing is discarded and the original eligible shortlist returns,
  // so a narrowing can never remove the one safe answer (G6-5, G6-14).
  // Schedule compatibility is applied first so that band/role remains the
  // last resort, exactly as `25 §7` requires.
  const narrowedBy = [];
  const narrow = (label, predicate) => {
    if (eligible.length <= 1) return;
    const kept = eligible.filter(predicate);
    if (kept.length === 0 || kept.length === eligible.length) return;
    eligible = kept;
    narrowedBy.push(label);
  };
  narrow('SCHEDULE_COMPATIBILITY', (contract) => contract.scheduleCompatible === true);
  narrow('VERIFIED_ROLE_BAND', (contract) => (
    contract.verifiedRoleBandMatch === true
    || (input.mappedRoleCode != null
        && text(contract.mappedRoleCode) !== ''
        && text(contract.mappedRoleCode) === text(input.mappedRoleCode))
  ));

  const eligibleIds = eligible.map((contract) => text(contract.contractId));

  if (eligible.length === 1) {
    return Object.freeze({
      state: 'RESOLVED',
      selectedContractId: eligibleIds[0],
      selectionMethod: 'AUTO_UNIQUE',
      baseContractIds: baseIds,
      eligibleContractIds: eligibleIds,
      unverifiableContractIds: unverifiableExcludedIds,
      narrowedBy: Object.freeze([...narrowedBy]),
      choices: [],
    });
  }

  return Object.freeze({
    state: 'MULTIPLE_MATCHING_CONTRACTS',
    selectedContractId: null,
    selectionMethod: null,
    baseContractIds: baseIds,
    eligibleContractIds: eligibleIds,
    unverifiableContractIds: unverifiableExcludedIds,
    narrowedBy: Object.freeze([...narrowedBy]),
    choices: eligible.map((contract) => Object.freeze({
      contractId: text(contract.contractId),
      displayLabel: text(contract.displayLabel),
      payType: text(contract.payType),
      priceResult: sourceMode === 'NHSP_WEEKLY' ? upper(contract?.priceObservation?.result) : null,
    })),
  });
}
