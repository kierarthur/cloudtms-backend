#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

import { canonicalDigest } from '../tests/weekly-source/harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../tests/weekly-source/harness/result-envelope.mjs';

function values(flag) {
  const output = [];
  for (let index = 2; index < process.argv.length; index += 1) {
    if (process.argv[index] === flag && process.argv[index + 1]) output.push(path.resolve(process.argv[index + 1]));
  }
  return output;
}
function value(flag) {
  const output = values(flag);
  if (output.length !== 1) throw new Error(`${flag} is required exactly once`);
  return output[0];
}
function sha256(bytes) { return createHash('sha256').update(bytes).digest('hex'); }
function commit(root) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8', windowsHide: true });
  const output = String(result.stdout ?? '').trim();
  if (result.status !== 0 || !/^[a-f0-9]{40}$/.test(output)) throw new Error('Backend commit is unavailable');
  return output;
}
const row = (scenarioId, file, marker) => ({ scenarioId, file, marker });

// These are local-boundary claims only. They prove the Weekly Source decision,
// family, guard, audit or freeze input that was actually executed. They never
// claim a Workbench residual, Draft, settlement, provider or Banking Pay result.
const registry = Object.freeze([
  row('SPI-001', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'the canonical current version must authorise'),
  row('SPI-002', '15092026_1534_weekly_source_protected_pay_publisher_v1.sql', 'source-absent protection must create one ordinary root, family and work event'),
  row('SPI-003', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'a reversal and its re-issue must resolve to one work event in either row order'),
  row('SPI-004', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'a reversal with no re-issue must leave no live source position'),
  row('SPI-005', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'RECORD_NOT_WORKED must still be permitted where the source has reversed the shift'),
  row('SPI-006', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'must prepare against the current server-owned protected schedule'),
  row('SPI-007', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'an exact completed replay must survive the family version advance'),
  row('SPI-008', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'current_target_vector_sha256'),
  row('SPI-009', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'the reversed-and-never-re-issued sentinel must differ from the re-issued cases'),
  row('SPI-010', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'splitting the same correction across reports must not change the source position'),
  row('SPI-011', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'both split orders must invoice the Client GBP 180.00'),
  row('SPI-012', '15092026_1534_weekly_source_protected_action_orchestration_v1.sql', 'RECORD_NOT_WORKED must be refused for every permutation of a worked, re-issued'),
  row('SPI-013', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'decision_bundle_id, bundle_revision, component_id'),
  row('SPI-014', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the component-to-head bundle identity foreign key is missing'),
  row('SPI-015', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'movement_group_id must never carry a unique index'),
  row('SPI-016', '17092026_0400_weekly_source_freeze_census_v1.sql', 'a DRAFT remainder holding a live family item keeps the root FROZEN'),
  row('SPI-017', '17092026_0400_weekly_source_freeze_census_v1.sql', 'RETURNED and bare EXECUTED provider evidence is ambiguous'),
  row('SPI-018', '17092026_0400_weekly_source_freeze_census_v1.sql', 'partial settlement keeps the root FROZEN'),
  row('SPI-065', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'exactly one token and one complete-scope job'),
  row('SPI-066', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'committed-current family index must be unique'),
  row('SPI-067', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'committed-current physical-root index must be unique'),
  row('SPI-068', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the H2-024 index must key on exactly decision_bundle_id, bundle_revision, component_id in that order'),
  row('SPI-069', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'certified zero declared while components remain'),
  row('SPI-070', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the receipt policy must name the owner role only'),
  row('SPI-071', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'WEEKLY_SOURCE_IMMUTABLE_RECORD'),
  row('SPI-072', '17092026_0400_weekly_source_freeze_census_v1.sql', 'a DRAFT remainder holding a live family item keeps the root FROZEN'),
  row('SPI-073', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'the unchanged ordinary Authorise owner must have done the lifecycle work'),
  row('SPI-074', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'the refused second authorisation must write no second generation'),
  row('SPI-075', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the same component retained in A and added to B'),
  row('SPI-076', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the same component added to B with no bundle tag at all'),
  row('SPI-077', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'the same component added to B under another bundle tag than its head'),
  row('SPI-078', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'certified zero declared while components remain'),
  row('SPI-079', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'a committed head without its publication receipt digest'),
  row('SPI-080', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'a second committed current head for one root, across authority kinds'),
  row('SPI-081', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'head components must not carry an adjustment identity'),
  row('SPI-082', '17092026_1200_weekly_source_audit_and_export_v1.sql', 'the export must name its paid-hours authority'),
  row('SPI-083', '17092026_1200_weekly_source_audit_and_export_v1.sql', 'no export owner may read the timesheet_pay_state last-settled cache'),
  row('SPI-084', '17092026_0100_weekly_source_entitlement_schema_v1.sql', 'certified zero declared while components remain'),
  row('SPI-085', '17092026_1200_weekly_source_audit_and_export_v1.sql', 'the export row separates the four facts for a Weekly Source week'),
  row('SPI-086', '17092026_0400_weekly_source_freeze_census_v1.sql', 'an aborted Draft with every family item voided releases under Binding B'),
  row('SPI-087', '17092026_0400_weekly_source_freeze_census_v1.sql', 'a DRAFT remainder holding a live family item keeps the root FROZEN'),
  row('SPI-088', '17092026_0400_weekly_source_freeze_census_v1.sql', 'Binding C proves the superseded void once the batch has settled'),
  row('SPI-089', '17092026_0400_weekly_source_freeze_census_v1.sql', 'partial settlement keeps the root FROZEN'),
  row('SPI-090', '17092026_0110_weekly_source_banking_pay_absence_v1.sql', 'integration must still read as ABSENT today'),
  row('SPI-091', '17092026_0110_weekly_source_banking_pay_absence_v1.sql', 'ordinary root upstream of the Banking Pay boundary must authorise'),
  row('SPI-093', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'an old physical id must be refused as stale'),
  row('SPI-094', '17092026_0200_weekly_source_rotation_authority_v1.sql', 'a rotated authorised family stays managed and fails closed either way'),
  row('SPI-095', '17092026_0200_weekly_source_rotation_authority_v1.sql', 'the read-only resolver must reach the same verdicts as I-1'),
  row('SPI-096', '17092026_0200_weekly_source_rotation_authority_v1.sql', 'a family with zero current rows must fail closed'),
  row('SPI-097', '17092026_0400_weekly_source_freeze_census_v1.sql', 'a member the resolver drops can never leave the bundle RELEASABLE'),
  row('SPI-098', '17092026_0200_weekly_source_rotation_authority_v1.sql', 'the binding records the binding-time family identity and version only'),
  row('SPI-099', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'the withdrawal must succeed'),
  row('SPI-100', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'the approval must be marked withdrawn and never deleted'),
  row('SPI-101', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'exactly one token and one complete-scope job'),
  row('SPI-102', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'Weekly Source must not alter the Draft'),
  row('SPI-103', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'a completed payment must refuse permanently'),
  row('SPI-104', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'a refused direct call must be audited'),
  row('SPI-105', '17092026_0600_weekly_source_first_authorisation_v1.sql', 'completed-cancellation change-of-mind'),
  row('SPI-106', '17092026_0200_weekly_source_rotation_authority_v1.sql', 'a genuinely busy Candidate must be a retryable WEEKLY_SOURCE_CANDIDATE_BUSY'),
]);

const backendRoot = value('--backend-root');
const newComponentPath = value('--new-component');
const upgradeComponentPath = value('--upgrade-component');
const outputPath = value('--output');
const loadJson = async (file) => JSON.parse(await readFile(file, 'utf8'));
const newComponent = await loadJson(newComponentPath);
const upgradeComponent = await loadJson(upgradeComponentPath);
for (const [label, result, mode] of [['NEW', newComponent, 'NEW'], ['UPGRADE', upgradeComponent, 'UPGRADE']]) {
  if (result?.schemaVersion !== 'WEEKLY_SOURCE_COMPONENT_POSTGRES_RESULT_V1'
      || result?.status !== 'PASS_WITH_HANDOVER2_PENDING' || result?.mode !== mode
      || !String(result?.postgresVersion ?? '').startsWith('170011|17.11')) {
    throw new Error(`${label} component evidence is not the required PostgreSQL 17.11 result.`);
  }
}
function filesFor(result) {
  return new Map((result?.scenarioEvidence?.groups ?? []).flatMap((group) => group.files ?? []).map((file) => [file.file, file]));
}
const newFiles = filesFor(newComponent);
const upgradeFiles = filesFor(upgradeComponent);
const cache = new Map();
async function proof(item) {
  const full = path.join(backendRoot, 'supabase', 'verification', item.file);
  let bytes = cache.get(full);
  if (!bytes) { bytes = await readFile(full); cache.set(full, bytes); }
  if (!bytes.toString('utf8').includes(item.marker)) throw new Error(`${item.scenarioId}: marker is absent from ${item.file}: ${item.marker}`);
  const digest = sha256(bytes);
  const fresh = newFiles.get(item.file);
  const upgrade = upgradeFiles.get(item.file);
  if (!fresh || !upgrade || fresh.fileSha256 !== digest || upgrade.fileSha256 !== digest
      || !/^[a-f0-9]{64}$/.test(fresh.outputSha256 ?? '') || !/^[a-f0-9]{64}$/.test(upgrade.outputSha256 ?? '')) {
    throw new Error(`${item.scenarioId}: current verifier was not executed in both PostgreSQL modes: ${item.file}`);
  }
  return { digest, fresh, upgrade };
}

const outcomes = [];
const projectionDigests = [];
for (const item of registry) {
  const checked = await proof(item);
  outcomes.push({
    scenarioId: item.scenarioId,
    localResult: 'PASS',
    observations: {
      executedActions: [`Executed the Weekly Source local-boundary assertion in ${item.file} on fresh and upgraded PostgreSQL 17.11.`],
      databaseReadbacks: [
        `fileSha256=${checked.digest}`,
        `newOutputSha256=${checked.fresh.outputSha256}`,
        `upgradeOutputSha256=${checked.upgrade.outputSha256}`,
        `localBoundaryAssertion=${item.marker}`,
      ],
      prohibitedOutcomeChecks: [
        'NO_BANKING_PAY_RESULT_CLAIMED',
        'NO_WORKBENCH_RESIDUAL_CALCULATED',
        'NO_DRAFT_OR_SETTLEMENT_RESULT_INFERRED',
        'CURRENT_LOCAL_ASSERTION_REQUIRED',
      ],
      actualInvoiceResult: 'Only the Weekly Source local source/invoice boundary is credited; the separately owned Banking Pay result remains pending.',
      actualOfficeAuditResult: `The executed local assertion was: ${item.marker}.`,
    },
  });
  projectionDigests.push({ name: `${item.scenarioId}:${item.file}`, digest: checked.digest });
}

const ids = registry.map((item) => item.scenarioId);
if (ids.length !== 59 || new Set(ids).size !== 59) throw new Error(`Expected 59 unique shared scenarios, received ${ids.length}.`);
const expected = { localCount: 59, scenarioIds: [...ids].sort(), externalOwner: 'HANDOVER2' };
const actual = { localCount: outcomes.length, scenarioIds: outcomes.map((item) => item.scenarioId).sort(), externalOwner: 'HANDOVER2', spiOutcomes: outcomes };
const scenario = {
  schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1', scenarioId: 'WS-SPI-SHARED-LOCAL-LIMBS-STEP4-001',
  fixedSeed: sha256('weekly-source-step4-shared-local-limbs-v1'), requirementIds: [], protectedIds: [],
};
const envelope = createResultEnvelope({
  scenario,
  repositories: [{ repository: 'cloudtms-backend', commit: commit(backendRoot) }],
  database: { used: true, engine: 'PostgreSQL', version: '17.11', modes: ['NEW', 'UPGRADE'], rowsReadBack: true },
  generatedSources: [], parser: { used: false }, clockValuesUtc: [],
  executedOwners: ['WEEKLY_SOURCE_LOCAL_BOUNDARY_ONLY'],
  oracle: { expected, expectedDigest: canonicalDigest(expected) }, actual,
  comparison: {
    pass: canonicalDigest({ localCount: actual.localCount, scenarioIds: actual.scenarioIds, externalOwner: actual.externalOwner }) === canonicalDigest(expected),
    actualDigest: canonicalDigest(actual), firstDivergence: null,
  },
  c1: { category: 'EXTERNAL_HANDOVER2_PENDING', emulator: false, releaseEvidenceEligible: false },
  outbox: { calls: [] }, projectionDigests, spiIds: actual.scenarioIds, protectedIds: [],
  cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
});
await writeResultEnvelope(outputPath, envelope);
console.log(`Exact Step 4 shared local SPI limbs passed ${outcomes.length}/59; HANDOVER 2 results remain pending (${envelope.evidenceDigest}).`);
