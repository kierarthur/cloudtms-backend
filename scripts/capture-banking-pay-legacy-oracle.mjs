// Mechanical test-fixture extraction only. Never loaded by a Worker/browser.
// An explicit frontend worktree is required; saved source is the authority.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const frontend = process.argv[2] && path.resolve(process.argv[2]);
assert.ok(frontend && fs.existsSync(path.join(frontend, 'js/main.js')), 'Explicit frontend worktree required');
const source = fs.readFileSync(path.join(frontend, 'js/main.js'), 'utf8').replaceAll('\r\n', '\n');
const renderStart = source.indexOf('function renderPayNewBatchWizard() {');
const renderEnd = source.indexOf('\nfunction ', renderStart + 1);
assert.ok(renderStart > 0 && renderEnd > renderStart);
const render = source.slice(renderStart, renderEnd);
const names = [
  'trimStr', 'upperTrim', 'isPlainObject', 'asArray', 'asBool', 'toNum',
  'getNestedLinePayload', 'firstPresentPreviewValue', 'firstFinitePreviewNumber',
  'getLinePresentationSection', 'isPreviewChildRow', 'getLineRowLevelAmount', 'getLineSectionAmount',
  'getLineSourceBasisJson', 'getLineCaseComponents', 'getLineKeyType', 'getLineKeyValue',
  'getLineCandidateId', 'getLineTimesheetId', 'getRelatedTimesheetIds', 'isSyntheticTimesheetResidualLine',
  'isOverpaymentRecoveryLine', 'isManualDebtRecoveryLine', 'getOverpaymentRecoveryPresentationGroupKey',
  'getOverpaymentRecoveryPresentation', 'getManualDebtRecoveryPresentation', 'getPreviewLineDisplayAmount',
  'getReadyTimesheetGroupKey', 'isReadyTimesheetDisplayContextLine', 'getPreviewRowId', 'isPreviewRowSelectionAllowed',
  'normaliseWorkbenchPayRoute', 'collectWorkbenchFilterIds',
  'workbenchResolutionTargetRouteKeys', 'workbenchEffectiveRouteKeys', 'workbenchSourceRouteKeys',
  'collectWorkbenchRouteSignals', 'rowMatchesCandidateClientFilters', 'rowMatchesPayRouteFilter', 'rowMatchesActivePayFilters'
  , 'parseBlockerCodes', 'getBankPayeeContext', 'hasBankBlockerContract', 'getBankBlockerCodes',
  'getExactBankTargetHash', 'getLineBankActionMeta', 'renderAcceptBankDetailsButton',
  'getSnoozeInfo', 'isHiddenDisplayRow', 'deep', 'groupEntriesByCandidate', 'buildCaseResolutionDisplayState', 'renderCaseActionButtons', 'renderComponentRows',
  'payeeReadinessBlockerCodes', 'payeeReadinessRunningStates', 'payeeReadinessFailedStates', 'payeeRouteKeyFrom', 'collectPayeeReadinessBlockedLines'
];
const snippets = names.map(name => {
  const start = render.indexOf(`  const ${name} = `);
  assert.ok(start >= 0, `Missing exact legacy declaration: ${name}`);
  const rest = render.slice(start);
  // The first syntactically complete semicolon-terminated declaration handles
  // block functions, multiline expressions and Set literals without rewriting.
  let end = 0;
  for (const terminal of rest.matchAll(/;(?=\n)/g)) {
    try { new vm.Script(rest.slice(0, terminal.index + 1)); end = terminal.index + 1; break; }
    catch { /* An internal statement is not yet the complete declaration. */ }
  }
  assert.ok(end, `Missing exact legacy declaration end: ${name}`);
  const text = rest.slice(0, end);
  new vm.Script(text); // Extraction must form a complete unchanged declaration.
  return { name, source: text, sha256: createHash('sha256').update(text).digest('hex') };
});
const fixture = {
  purpose: 'Test-only frozen legacy display/filter/action oracle; no application authority or new economic rules.',
  repository: 'kierarthur/TEST-Frontend',
  observed_head: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: frontend, encoding: 'utf8' }).trim(),
  source_path: 'js/main.js',
  normalized_source_sha256: createHash('sha256').update(source).digest('hex'),
  snippets,
  candidate_metadata_builder: (() => {
    const start=render.indexOf('  const candidateMetaById = new Map();');
    const end=render.indexOf('  const payeGuardrails = ',start);
    assert.ok(start>=0 && end>start);
    const text=render.slice(start,end).trimEnd();
    new vm.Script(`(()=>{const allCandidates=[];${text}})`);
    return {source:text,sha256:createHash('sha256').update(text).digest('hex')};
  })()
};
const serialized = JSON.stringify(fixture, null, 2) + '\n';
for (const destination of [
  path.join(root, 'tests/fixtures/banking-pay-legacy-display-oracle.json'),
  path.join(frontend, 'tests/fixtures/banking-pay-legacy-display-oracle.json')
]) fs.writeFileSync(destination, serialized);
console.log(JSON.stringify({ extracted_declarations: snippets.length, source_sha256: fixture.normalized_source_sha256 }));
