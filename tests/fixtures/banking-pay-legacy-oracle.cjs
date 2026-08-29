// Pure test oracle. All numeric/filter helpers are unchanged source extracts.
// The friendly-label dependency is immaterial to numbers and deliberately does
// not pull the app's date/expense-label UI into a database comparison test.
const vm = require('node:vm');
const fixture = require('./banking-pay-legacy-display-oracle.json');
module.exports = function legacyOracle(filters = {}, channel = 'ALL', candidateMetadata = [], readinessInputs = {}) {
  const context = vm.createContext({ filters, channel, candidateMetadata, readinessInputs });
  const source = `
    const candFilterId = String(filters.candidate_id || filters.candidateId || '').trim();
    const clientFilterId = String(filters.client_id || filters.clientId || '').trim();
    const sessionCandidateFilterId = candFilterId;
    const sessionClientFilterId = clientFilterId;
    const draftScope = channel;
    const getOverpaymentComponentFriendlyLabel = () => 'Test-only component label';
    const candidateMetaById = new Map(candidateMetadata.map(row => [String(row.candidate_id || '').trim(),row]));
    const blockedPreviewLines = readinessInputs.blocked || [];
    const canonicalPreviewLines = readinessInputs.ready || [];
    const pv = readinessInputs.preview || {};
    const wiz = {workbench:readinessInputs.workbench || {}};
    const railProvider = 'TEST_PROVIDER';
    const railEnv = 'TEST';
    const enc = value => String(value).replaceAll('&','&amp;').replaceAll('"','&quot;');
    const getCandidateActionDisabledAttrs = () => '';
    // Formatting is irrelevant to the component-button oracle. No calculation
    // or saved component value is replaced by this test-only string formatter.
    const fmtMoney = value => String(value ?? '');
    ${fixture.snippets.map(item => item.source).join('\n')}
    const buildCandidateBankMeta = allCandidates => {
      ${fixture.candidate_metadata_builder.source}
      return Array.from(candidateMetaById.values());
    };
    ({ getPreviewLineDisplayAmount, getLineSectionAmount, getLineRowLevelAmount,
       getReadyTimesheetGroupKey, getOverpaymentRecoveryPresentationGroupKey,
       getOverpaymentRecoveryPresentation, getManualDebtRecoveryPresentation,
       isPreviewRowSelectionAllowed, isSyntheticTimesheetResidualLine,
       isReadyTimesheetDisplayContextLine, getRelatedTimesheetIds, rowMatchesActivePayFilters,
       getBankBlockerCodes, hasBankBlockerContract, getExactBankTargetHash,
       getLineBankActionMeta, renderAcceptBankDetailsButton, getSnoozeInfo, isHiddenDisplayRow,
       buildCaseResolutionDisplayState, renderCaseActionButtons, renderComponentRows,
       payeeRouteKeyFrom, collectPayeeReadinessBlockedLines, buildCandidateBankMeta });
  `;
  return new vm.Script(source).runInContext(context, { timeout: 1000 });
};
