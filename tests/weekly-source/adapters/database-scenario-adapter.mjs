import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { canonicalDigest } from '../harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';
import { spiIdsForEvidenceGroup } from '../harness/spi-execution-map.mjs';
import { executeRealWorldDatabaseJourneys } from './real-world-database-execution.mjs';

const TEMPLATE_DATABASE = 'banking_modal_v2_test';

// Every group carries the control-set ids its verification files actually
// prove, and passes them to `createResultEnvelope`, so executed work counts
// toward the coverage gate (contract item G12-3; WP-16b handoff N1). Before
// this, all five groups emitted empty arrays and the 19 Weekly Source verifiers
// contributed zero rows, which is why coverage read 23 of 918.
//
// THE RULE FOR THIS TABLE. An id appears against a group only where a file in
// that group ASSERTS it — not where a comment mentions it. Each list was read
// out of the files' own assertions. If a verifier fails, its whole group fails
// and none of its ids reaches the envelope, because `writeDatabaseEvidence`
// only runs after every file in every group has passed.
//
// Ids must exist in the pinned Plan 6.2 annexes or the gate refuses the
// envelope as an orphan, so a mistyped id fails loudly rather than silently
// inflating coverage.

export const WEEKLY_SOURCE_DATABASE_GROUPS = Object.freeze([
  Object.freeze({
    id: 'source-ingestion',
    files: Object.freeze([
      '15092026_1534_00_weekly_source_private_classifiers_v1.sql',
      '15092026_1534_weekly_source_upload_context_v1.sql',
      '15092026_1534_weekly_source_upload_publication_v1.sql',
      '15092026_1534_weekly_source_projection_build_v1.sql',
    ]),
    acceptanceIds: Object.freeze(['PRC-006', 'PRC-007', 'PRC-020', 'NHSBR-019', 'NHSBR-020']),
    spiIds: Object.freeze(spiIdsForEvidenceGroup('source-ingestion')),
    xsgIds: Object.freeze(['XSG-009']),
  }),
  Object.freeze({
    id: 'queries-and-reads',
    files: Object.freeze([
      '15092026_1534_weekly_source_query_delivery_v1.sql',
      '15092026_1534_weekly_source_read_projections_v1.sql',
      '15092026_2203_weekly_source_candidate_app_contract_v1.sql',
      '15092026_2312_weekly_source_delivery_targets_v1.sql',
      '15092026_1534_weekly_source_acl_contract_v1.sql',
    ]),
    // The read-projection verifier asserts the exact heading, schedule, support
    // pane, status and allowed action of every one of the 22 lifecycle states.
    uiStateIds: Object.freeze([
      'UI-001', 'UI-002', 'UI-003', 'UI-004', 'UI-005', 'UI-006', 'UI-007', 'UI-008',
      'UI-009', 'UI-010', 'UI-011', 'UI-012', 'UI-013', 'UI-014', 'UI-015', 'UI-016',
      'UI-017', 'UI-018', 'UI-019', 'UI-020', 'UI-021', 'UI-022',
    ]),
  }),
  Object.freeze({
    id: 'finalisation-and-pay',
    files: Object.freeze([
      '15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
      '15092026_1534_weekly_source_finalisation_v1.sql',
      '15092026_2336_weekly_source_finalisation_pay_orchestration_v1.sql',
      '15092026_1534_weekly_source_protected_action_orchestration_v1.sql',
      '15092026_1534_weekly_source_protected_pay_publisher_v1.sql',
    ]),
    xsgIds: Object.freeze(['XSG-002']),
    spiIds: Object.freeze(spiIdsForEvidenceGroup('finalisation-and-pay')),
  }),
  Object.freeze({
    id: 'mode-a',
    files: Object.freeze([
      '17092026_0800_weekly_source_mode_a_dispatch_v1.sql',
    ]),
    spiIds: Object.freeze(spiIdsForEvidenceGroup('mode-a')),
  }),
  Object.freeze({
    id: 'invoice-and-correction',
    files: Object.freeze([
      '15092026_1534_weekly_source_correct_final_source_v1.sql',
      '15092026_1534_weekly_source_invoice_admission_v1.sql',
      '15092026_1534_weekly_source_invoice_batch_integration_v1.sql',
      '02092026_1833_weekly_source_invoice_issue_validator_v1.sql',
      '19092026_0100_weekly_source_correction_cancel_v1.sql',
    ]),
    acceptanceIds: Object.freeze(['INV-030']),
    // This group proves the complete local FTI-020 invoice-discounting limb:
    // positive, negative-only and mixed signed invoices; conflicting Draft;
    // stale commit; cancel/rebuild/commit; unissue/reissue; line movement; and
    // injected recompute failure rolling back the move and both ledgers.
    // It deliberately does not claim FTI-024 or XSG-017 because those also
    // require async Worker, document, report and browser evidence.
    xsgIds: Object.freeze(['XSG-030']),
    ftiIds: Object.freeze(['FTI-020']),
    spiIds: Object.freeze(spiIdsForEvidenceGroup('invoice-and-correction')),
  }),
  Object.freeze({
    id: 'settings',
    files: Object.freeze([
      '15092026_1534_weekly_source_settings_expenses_rates_v1.sql',
      '15092026_1534_weekly_source_settings_admin_v1.sql',
    ]),
  }),
  Object.freeze({
    id: 'completed-pack-copy',
    files: Object.freeze([
      '19092026_1645_weekly_source_completed_pack_copy_v1.sql',
    ]),
  }),
  Object.freeze({
    id: 'ordinary-lifecycle-protection',
    files: Object.freeze([
      '20092026_2300_weekly_source_ordinary_unauthorise_family_invoice_census_v1.sql',
    ]),
    acceptanceIds: Object.freeze(['UNA-012']),
    protectedIds: Object.freeze(['PROT-UNAUTH-001', 'PROT-ROTATION-001']),
    ftiIds: Object.freeze(['FTI-027', 'FTI-060']),
  }),
  // THE OWNER GROUP (WP-16b handoff N2). The twelve Plan 6.2 verifiers written
  // by WP-01a, WP-03, WP-02, WP-08a, WP-07, WP-08b, WP-04, WP-10, WP-18, WP-11a,
  // WP-11b and WP-14 were in no group at all, so nothing they prove could reach
  // the coverage gate.
  Object.freeze({
    id: 'plan62-owners',
    files: Object.freeze([
      '17092026_0100_weekly_source_entitlement_schema_v1.sql',
      '17092026_0200_weekly_source_rotation_authority_v1.sql',
      '17092026_0300_weekly_source_entitlement_publication_v1.sql',
      '17092026_0400_weekly_source_freeze_census_v1.sql',
      '17092026_0600_weekly_source_first_authorisation_v1.sql',
      '17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
      '17092026_0110_weekly_source_banking_pay_absence_v1.sql',
      '17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
      '17092026_0800_weekly_source_workbench_seams_v1.sql',
      '17092026_0900_weekly_source_installed_writer_census_v1.sql',
      '17092026_1000_weekly_source_settlement_allocation_v1.sql',
      '17092026_1100_weekly_source_candidate_view_producer_v1.sql',
      '17092026_1200_weekly_source_audit_and_export_v1.sql',
      '17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
      '17092026_1500_weekly_source_row_admission_guards_v1.sql',
    ]),
    acceptanceIds: Object.freeze([
      'ROT-001', 'ROT-004', 'ROT-009', 'ROT-011', 'ROT-012',
      'UNA-001', 'UNA-002', 'UNA-003', 'UNA-004', 'UNA-005', 'UNA-006', 'UNA-007', 'UNA-008',
      'UNA-009', 'UNA-010', 'UNA-011', 'UNA-012', 'UNA-016', 'UNA-017', 'UNA-018',
    ]),
    proofIds: Object.freeze([
      'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'R9', 'R10', 'R11', 'R12', 'R13', 'R14',
      'R16', 'R17', 'R18', 'R19', 'R20', 'R21', 'R22', 'R23', 'R24', 'R25', 'R27', 'R28', 'R29',
      'R30', 'R31', 'R32', 'R33', 'R34', 'R35', 'R36', 'R37', 'R38', 'R39', 'R40', 'R41', 'R42',
      'R43', 'R44',
    ]),
    h2Ids: Object.freeze([
      'H2-004', 'H2-024', 'H2-025', 'H2-031', 'H2-032', 'H2-033', 'H2-035', 'H2-036', 'H2-038',
    ]),
    uiStateIds: Object.freeze(['UI-007', 'UI-009', 'UI-012', 'UI-019', 'UI-020', 'UI-021']),
    xsgIds: Object.freeze(['XSG-009', 'XSG-029']),
  }),
]);

// These are the exact database verifiers blocked by the separately owned
// HANDOVER 2 Workbench/Banking Pay implementation. The component command
// records each as a named dependency; the ordinary NEW/UPGRADE release adapter
// still runs every verifier and therefore remains fail-closed until HANDOVER 2
// is installed. Keeping this as an exact allow-list prevents any unrelated
// Weekly Source failure being silently relabelled as somebody else's work.
// Invoice admission is intentionally executable: the component installs and
// proves the existing invoice-discounting owner as a local, non-Banking-Pay
// assurance limb.
export const WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES = Object.freeze([
  '17092026_0300_weekly_source_entitlement_publication_v1.sql',
  '17092026_0610_weekly_source_withdrawal_supersession_v1.sql',
  '17092026_0700_weekly_source_pending_entitlement_release_v1.sql',
  '17092026_0800_weekly_source_workbench_seams_v1.sql',
  '17092026_0900_weekly_source_installed_writer_census_v1.sql',
  '17092026_1000_weekly_source_settlement_allocation_v1.sql',
  '17092026_1400_weekly_source_ordinary_authorisation_guard_v1.sql',
]);

/** The union of one control-set field across every group, sorted and deduplicated. */
export function databaseGroupIds(field, groups = WEEKLY_SOURCE_DATABASE_GROUPS) {
  const seen = new Set();
  for (const group of groups) {
    for (const id of group[field] ?? []) seen.add(id);
  }
  return [...seen].sort();
}

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.code = code;
  error.details = details;
  throw error;
}

function localDatabaseUrl(connectionUrl, database) {
  let value;
  try {
    value = new URL(connectionUrl);
  } catch {
    fail('WEEKLY_SOURCE_DB_ADAPTER_TARGET_INVALID', 'The database adapter received an invalid local target.');
  }
  if (
    value.protocol !== 'postgresql:'
    || value.hostname !== '127.0.0.1'
    || !value.port
    || decodeURIComponent(value.username) !== 'postgres'
    || value.password
    || decodeURIComponent(value.pathname.slice(1)) !== TEMPLATE_DATABASE
  ) {
    fail('WEEKLY_SOURCE_DB_ADAPTER_TARGET_REFUSED', 'The database adapter accepts only the task-owned local template database.');
  }
  value.pathname = `/${encodeURIComponent(database)}`;
  return value.toString();
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    encoding: 'utf8',
    timeout: options.timeoutMs ?? 600_000,
    maxBuffer: options.maxBuffer ?? 256 * 1024 * 1024,
    windowsHide: true,
  });
  if (result.error || result.status !== 0) {
    const diagnostic = String(result.stderr || result.stdout || result.error?.message || '')
      .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
      .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
      .trim()
      .slice(-12000);
    const message = options.message ?? 'A local database proof command failed.';
    fail(options.code ?? 'WEEKLY_SOURCE_DB_ADAPTER_COMMAND_FAILED', diagnostic ? `${message} ${diagnostic}` : message, {
      exitStatus: Number.isInteger(result.status) ? result.status : -1,
      errorKind: String(result.stderr || result.error?.code || 'UNKNOWN').trim().split(/\s+/)[0] || null,
    });
  }
  return String(result.stdout ?? '');
}

function psqlQuery(psqlBin, url, sql) {
  return run(psqlBin, [url, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', sql], {
    code: 'WEEKLY_SOURCE_DB_ADAPTER_QUERY_FAILED',
    message: 'A local database clone-management query failed.',
  }).trim();
}

function psqlFile(psqlBin, url, file) {
  return run(psqlBin, [url, '-X', '-v', 'ON_ERROR_STOP=1', '-f', file], {
    code: 'WEEKLY_SOURCE_DB_ADAPTER_VERIFICATION_FAILED',
    message: `Weekly Source database verification failed for ${path.basename(file)}.`,
    timeoutMs: 1_200_000,
  });
}

function cloneName(mode, groupIndex, fileIndex) {
  const value = `ws_${mode.toLowerCase()}_${String(groupIndex + 1).padStart(2, '0')}_${String(fileIndex + 1).padStart(2, '0')}`;
  if (!/^[a-z0-9_]{1,63}$/.test(value)) fail('WEEKLY_SOURCE_DB_CLONE_NAME_INVALID', 'A database clone name is invalid.');
  return value;
}

async function fileDigest(file) {
  return createHash('sha256').update(await readFile(file)).digest('hex');
}

function gitCommit(repoRoot) {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {
    cwd: repoRoot,
    encoding: 'utf8',
    timeout: 10_000,
    windowsHide: true,
  });
  const commit = String(result.stdout ?? '').trim();
  if (result.error || result.status !== 0 || !/^[a-f0-9]{40}$/.test(commit)) {
    fail('WEEKLY_SOURCE_DB_COMMIT_UNAVAILABLE', 'The backend commit identity is unavailable.');
  }
  return commit;
}

async function writeDatabaseEvidence({ mode, repoRoot, resultDirectory, evidence, cleanup }) {
  if (!resultDirectory) return null;
  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: `WS-DATABASE-${mode}-EXACT-001`,
    fixedSeed: createHash('sha256').update(`weekly-source-database-${mode}-exact-v1`).digest('hex'),
    requirementIds: [],
    protectedIds: databaseGroupIds('protectedIds'),
  };
  const expected = {
    mode,
    templateDatabase: TEMPLATE_DATABASE,
    groupCount: WEEKLY_SOURCE_DATABASE_GROUPS.length,
    controlSetIds: {
      acceptance: databaseGroupIds('acceptanceIds').length,
      spi: databaseGroupIds('spiIds').length,
      iss: databaseGroupIds('issIds').length,
      ui: databaseGroupIds('uiStateIds').length,
      xsg: databaseGroupIds('xsgIds').length,
      h2: databaseGroupIds('h2Ids').length,
      proofs: databaseGroupIds('proofIds').length,
    },
    groups: evidence.map((group) => ({
      group: group.group,
      verificationCount: group.verificationCount,
      files: group.files.map(({ file, fileSha256 }) => ({ file, fileSha256 })),
    })),
    cleanup,
  };
  const actual = {
    mode,
    templateDatabase: TEMPLATE_DATABASE,
    groupCount: evidence.length,
    controlSetIds: {
      acceptance: databaseGroupIds('acceptanceIds').length,
      spi: databaseGroupIds('spiIds').length,
      iss: databaseGroupIds('issIds').length,
      ui: databaseGroupIds('uiStateIds').length,
      xsg: databaseGroupIds('xsgIds').length,
      h2: databaseGroupIds('h2Ids').length,
      proofs: databaseGroupIds('proofIds').length,
    },
    groups: evidence.map((group) => ({
      group: group.group,
      verificationCount: group.verificationCount,
      files: group.files.map(({ file, fileSha256 }) => ({ file, fileSha256 })),
    })),
    cleanup,
  };
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'cloudtms-backend', commit: gitCommit(repoRoot) }],
    database: { used: true, engine: 'PostgreSQL', mode, templateDatabase: TEMPLATE_DATABASE },
    generatedSources: [],
    parser: { used: false },
    clockValuesUtc: [],
    executedOwners: evidence.flatMap((group) => group.files.map(({ file }) => `supabase/verification/${file}`)),
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison: {
      pass: canonicalDigest(actual) === canonicalDigest(expected),
      actualDigest: canonicalDigest(actual),
      firstDivergence: null,
    },
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
    outbox: { calls: [] },
    projectionDigests: evidence.flatMap((group) => group.files.map(({ file, fileSha256, outputSha256 }) => [
      { name: `supabase/verification/${file}`, digest: fileSha256 },
      { name: `output/${file}`, digest: outputSha256 },
    ])).flat(),
    acceptanceIds: databaseGroupIds('acceptanceIds'),
    protectedIds: databaseGroupIds('protectedIds'),
    modelIds: databaseGroupIds('modelIds'),
    spiIds: databaseGroupIds('spiIds'),
    issIds: databaseGroupIds('issIds'),
    uiStateIds: databaseGroupIds('uiStateIds'),
    ftiIds: databaseGroupIds('ftiIds'),
    xsgIds: databaseGroupIds('xsgIds'),
    h2Ids: databaseGroupIds('h2Ids'),
    proofIds: databaseGroupIds('proofIds'),
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0, ...cleanup },
  });
  if (envelope.status !== 'PASS') fail('WEEKLY_SOURCE_DB_EVIDENCE_MISMATCH', 'The database evidence did not match its exact expected result.');
  await writeResultEnvelope(path.join(resultDirectory, `database-${mode.toLowerCase()}-exact.json`), envelope);
  return envelope.evidenceDigest;
}

async function writeComponentDatabaseEvidence({ mode, repoRoot, resultDirectory, evidence, cleanup, claimGroups, dependencyPending }) {
  if (!resultDirectory) return null;
  const componentProtectedIds = databaseGroupIds('protectedIds', claimGroups);
  const scenario = {
    schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
    scenarioId: `WS-DATABASE-COMPONENT-${mode}-PG17-001`,
    fixedSeed: createHash('sha256').update(`weekly-source-database-component-${mode.toLowerCase()}-pg17-v1`).digest('hex'),
    requirementIds: [],
    protectedIds: componentProtectedIds,
  };
  const groupSummary = evidence.map((group) => ({
    group: group.group,
    verificationCount: group.verificationCount,
    files: group.files.map(({ file, fileSha256 }) => ({ file, fileSha256 })),
  }));
  const expected = {
    mode,
    templateDatabase: TEMPLATE_DATABASE,
    groupCount: evidence.length,
    groups: groupSummary,
    dependencyPending: [...dependencyPending].sort(),
    releaseEvidenceEligible: false,
    cleanup,
  };
  const actual = structuredClone(expected);
  const envelope = createResultEnvelope({
    scenario,
    repositories: [{ repository: 'cloudtms-backend', commit: gitCommit(repoRoot) }],
    database: { used: true, engine: 'PostgreSQL', mode: `COMPONENT_${mode}`, templateDatabase: TEMPLATE_DATABASE },
    generatedSources: [],
    parser: { used: false },
    clockValuesUtc: [],
    executedOwners: evidence.flatMap((group) => group.files.map(({ file }) => `supabase/verification/${file}`)),
    oracle: { expected, expectedDigest: canonicalDigest(expected) },
    actual,
    comparison: { pass: true, actualDigest: canonicalDigest(actual), firstDivergence: null },
    c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: false },
    outbox: { calls: [] },
    projectionDigests: evidence.flatMap((group) => group.files.flatMap(({ file, fileSha256, outputSha256 }) => [
      { name: `supabase/verification/${file}`, digest: fileSha256 },
      { name: `output/${file}`, digest: outputSha256 },
    ])),
    acceptanceIds: databaseGroupIds('acceptanceIds', claimGroups),
    protectedIds: componentProtectedIds,
    spiIds: databaseGroupIds('spiIds', claimGroups),
    issIds: databaseGroupIds('issIds', claimGroups),
    uiStateIds: databaseGroupIds('uiStateIds', claimGroups),
    ftiIds: databaseGroupIds('ftiIds', claimGroups),
    xsgIds: databaseGroupIds('xsgIds', claimGroups),
    h2Ids: [],
    proofIds: [],
    cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0, ...cleanup },
  });
  if (envelope.status !== 'PASS') fail('WEEKLY_SOURCE_COMPONENT_EVIDENCE_MISMATCH', 'The component database evidence did not match its bounded expected result.');
  await writeResultEnvelope(path.join(resultDirectory, `database-component-${mode.toLowerCase()}-pg17-exact.json`), envelope);
  return envelope.evidenceDigest;
}

/**
 * The serial concurrency group (`23A` section 13; WP-16b handoff N2).
 *
 * `R12`, `R15`, `R32`, `R37`, `R42`, `ROT-002`, `ROT-003`, `ROT-012` and
 * `UNA-014` all need two or more sessions holding transactions at the same time,
 * which one `psql -f` per file cannot express. The controller hands this
 * adapter an `openNamedSessionGroup` already bound to the target it has proved,
 * so a scenario can never substitute its own: passing `baseConnectionUrl` or
 * `expectedPort` is refused with `POSTGRES_HARNESS_NAMED_SESSION_TARGET_REFUSED`.
 *
 * The WP-16c suites under `tests/weekly-source/wp16c/` are the cases that use
 * it. They are driven from there rather than from here because each one creates
 * and drops its own clones and commits its own fixture, which this adapter's
 * one-clone-per-file model does not do.
 */
export const WEEKLY_SOURCE_SERIAL_GROUP = Object.freeze({
  id: 'plan62-serial-concurrency',
  requiresNamedSessions: true,
  minimumSessions: 2,
  suites: Object.freeze([
    'tests/weekly-source/wp16c/una-suite.mjs',
    'tests/weekly-source/wp16c/rot-suite.mjs',
    'tests/weekly-source/wp16c/release-suite.mjs',
  ]),
  servesProofIds: Object.freeze([
    'R12', 'R15', 'R32', 'R33', 'R37', 'R42', 'ROT-002', 'ROT-003', 'ROT-012', 'UNA-014',
  ]),
});

const SERIAL_RESULT_FILES = Object.freeze([
  'wp16c-una-suite.json',
  'wp16c-rot-suite.json',
  'wp16c-release-suite.json',
]);

async function runSerialSuites({
  mode,
  connectionUrl,
  repoRoot,
  resultDirectory,
  psqlBin,
  runSerialOnUpgrade = false,
  serialDependencyPending = false,
}) {
  if (serialDependencyPending) {
    return Object.freeze({
      executed: false,
      reason: 'HANDOVER2_IMPLEMENTATION_PENDING',
      suites: WEEKLY_SOURCE_SERIAL_GROUP.suites,
      servesProofIds: WEEKLY_SOURCE_SERIAL_GROUP.servesProofIds,
    });
  }
  if (mode !== 'NEW' && !(mode === 'UPGRADE' && runSerialOnUpgrade === true)) {
    return Object.freeze({
      executed: false,
      reason: 'EXECUTED_ON_NEW_INSTALL_ONLY',
      suites: WEEKLY_SOURCE_SERIAL_GROUP.suites,
    });
  }
  if (!resultDirectory) {
    fail('WEEKLY_SOURCE_SERIAL_RESULT_DIRECTORY_REQUIRED', 'The serial database suites require the harness result directory.');
  }
  const target = new URL(connectionUrl);
  if (target.hostname !== '127.0.0.1' || !target.port || target.password) {
    fail('WEEKLY_SOURCE_SERIAL_TARGET_REFUSED', 'The serial database suites accept only the proved local passwordless target.');
  }
  const suiteEvidence = [];
  for (const [index, relativeSuite] of WEEKLY_SOURCE_SERIAL_GROUP.suites.entries()) {
    const output = run(process.execPath, [path.join(repoRoot, relativeSuite), '--results', resultDirectory], {
      cwd: repoRoot,
      env: {
        ...process.env,
        PSQL_BIN: psqlBin,
        WP16C_PORT: target.port,
        WP16C_TEMPLATE: TEMPLATE_DATABASE,
      },
      timeoutMs: 3_600_000,
      maxBuffer: 256 * 1024 * 1024,
      code: 'WEEKLY_SOURCE_SERIAL_SUITE_FAILED',
      message: `Serial database suite ${relativeSuite} failed.`,
    });
    const resultFile = path.join(resultDirectory, SERIAL_RESULT_FILES[index]);
    const envelope = JSON.parse(await readFile(resultFile, 'utf8'));
    if (envelope?.schemaVersion !== 'WEEKLY_SOURCE_TEST_RESULT_V1' || envelope.status !== 'PASS') {
      fail('WEEKLY_SOURCE_SERIAL_SUITE_EVIDENCE_INVALID', `Serial database suite ${relativeSuite} did not write a passing result envelope.`);
    }
    suiteEvidence.push(Object.freeze({
      suite: relativeSuite,
      resultFile: SERIAL_RESULT_FILES[index],
      evidenceDigest: envelope.evidenceDigest,
      outputSha256: createHash('sha256').update(output).digest('hex'),
    }));
  }
  return Object.freeze({ executed: true, suites: suiteEvidence });
}

async function executeDatabaseScenarios({
  mode,
  connectionUrl,
  repoRoot,
  resultDirectory,
  openNamedSessionGroup,
  runSerialOnUpgrade = false,
  dependencyPendingFiles = [],
  releaseEvidenceEligible = true,
  serialDependencyPending = false,
  scenarioDirectory,
  createRealWorldScenarioDependencies,
}) {
  if (!['NEW', 'UPGRADE'].includes(mode)) fail('WEEKLY_SOURCE_DB_ADAPTER_MODE_INVALID', 'The database adapter requires NEW or UPGRADE mode.');
  const pending = new Set(dependencyPendingFiles);
  const allFiles = WEEKLY_SOURCE_DATABASE_GROUPS.flatMap((group) => group.files);
  for (const file of pending) {
    if (!allFiles.includes(file)) {
      fail('WEEKLY_SOURCE_COMPONENT_PENDING_FILE_UNKNOWN', `The component boundary names unknown verifier ${file}.`);
    }
  }
  if (releaseEvidenceEligible && pending.size > 0) {
    fail('WEEKLY_SOURCE_RELEASE_VERIFIER_EXCLUSION_REFUSED', 'A complete release database proof cannot exclude a verifier.');
  }
  const executionGroups = WEEKLY_SOURCE_DATABASE_GROUPS.map((group) => Object.freeze({
    ...group,
    files: Object.freeze(group.files.filter((file) => !pending.has(file))),
  }));
  const psqlBin = process.env.PSQL_BIN ?? 'psql';
  const adminUrl = localDatabaseUrl(connectionUrl, 'postgres');
  const templateUrl = localDatabaseUrl(connectionUrl, TEMPLATE_DATABASE);
  const verificationRoot = path.join(repoRoot, 'supabase', 'verification');
  const clones = executionGroups.flatMap((group, groupIndex) => group.files.map((file, fileIndex) => ({
    group: group.id,
    file,
    name: cloneName(mode, groupIndex, fileIndex),
  })));
  const evidence = [];
  let realWorldEvidence = null;
  let templateClosed = false;
  // The controller supplies this bound to the target it has already proved. A
  // scenario that tries to supply its own is refused by the controller.
  const namedSessions = typeof openNamedSessionGroup === 'function'
    ? { available: true, group: WEEKLY_SOURCE_SERIAL_GROUP.id, minimumSessions: WEEKLY_SOURCE_SERIAL_GROUP.minimumSessions }
    : { available: false, group: WEEKLY_SOURCE_SERIAL_GROUP.id, reason: 'CONTROLLER_DID_NOT_SUPPLY_A_NAMED_SESSION_FACTORY' };

  try {
    const exactTemplate = psqlQuery(psqlBin, templateUrl, 'select current_database();');
    if (exactTemplate !== TEMPLATE_DATABASE) fail('WEEKLY_SOURCE_DB_TEMPLATE_REFUSED', 'The local template database identity is wrong.');
    psqlQuery(psqlBin, adminUrl, `select pg_terminate_backend(pid) from pg_stat_activity where datname = '${TEMPLATE_DATABASE}' and pid <> pg_backend_pid();`);
    psqlQuery(psqlBin, adminUrl, `alter database ${TEMPLATE_DATABASE} with allow_connections false;`);
    templateClosed = true;

    for (const [groupIndex, group] of executionGroups.entries()) {
      const fileResults = [];
      for (const [fileIndex, fileName] of group.files.entries()) {
        const name = cloneName(mode, groupIndex, fileIndex);
        psqlQuery(psqlBin, adminUrl, `drop database if exists ${name} with (force);`);
        psqlQuery(psqlBin, adminUrl, `create database ${name} with template ${TEMPLATE_DATABASE} owner postgres;`);
        const cloneUrl = localDatabaseUrl(connectionUrl, name);
        const file = path.join(verificationRoot, fileName);
        try {
          const output = psqlFile(psqlBin, cloneUrl, file);
          // Only another client session is test residue. PostgreSQL may start an
          // autovacuum or another internal worker while a long verifier is
          // running; counting that server-owned worker makes isolation fail at
          // random even though no test connection escaped. A leaked verifier
          // session remains a `client backend` and is still rejected here.
          const residue = psqlQuery(psqlBin, cloneUrl, "select count(*) from pg_stat_activity where datname = current_database() and pid <> pg_backend_pid() and backend_type = 'client backend';");
          if (residue !== '0') fail('WEEKLY_SOURCE_DB_GROUP_CONNECTION_RESIDUE', `Database verification ${fileName} retained an unexpected connection.`);
          fileResults.push({
            file: fileName,
            fileSha256: await fileDigest(file),
            outputSha256: createHash('sha256').update(output).digest('hex'),
          });
        } finally {
          try { psqlQuery(psqlBin, adminUrl, `drop database if exists ${name} with (force);`); } catch { /* retain original failure */ }
        }
      }
      evidence.push({
        group: group.id,
        verificationCount: fileResults.length,
        files: fileResults,
      });
    }

    // The populated journeys are deliberately separate from the per-file SQL
    // verifiers. A broad verifier PASS cannot stand in for their declared
    // action sequence. Both journeys run on one fresh clone of the exact NEW or
    // UPGRADE template and the scenario adapter must prove its own row cleanup.
    const realWorldClone = `ws_${mode.toLowerCase()}_real_world`;
    psqlQuery(psqlBin, adminUrl, `drop database if exists ${realWorldClone} with (force);`);
    psqlQuery(psqlBin, adminUrl, `create database ${realWorldClone} with template ${TEMPLATE_DATABASE} owner postgres;`);
    try {
      realWorldEvidence = await executeRealWorldDatabaseJourneys({
        mode,
        scenarioDirectory,
        resultDirectory,
        createScenarioDependencies: createRealWorldScenarioDependencies,
        database: Object.freeze({
          connectionUrl: localDatabaseUrl(connectionUrl, realWorldClone),
          database: realWorldClone,
          psqlBin,
          templateDatabase: TEMPLATE_DATABASE,
          componentBoundary: releaseEvidenceEligible === false,
        }),
      });
    } finally {
      try { psqlQuery(psqlBin, adminUrl, `drop database if exists ${realWorldClone} with (force);`); } catch { /* retain original failure */ }
    }
  } finally {
    for (const clone of clones) {
      try { psqlQuery(psqlBin, adminUrl, `drop database if exists ${clone.name} with (force);`); } catch { /* retain original failure */ }
    }
    if (templateClosed) {
      try { psqlQuery(psqlBin, adminUrl, `alter database ${TEMPLATE_DATABASE} with allow_connections true;`); } catch { /* retain original failure */ }
    }
  }

  const remaining = psqlQuery(psqlBin, adminUrl, `select count(*) from pg_database where datname like 'ws_${mode.toLowerCase()}_%';`);
  const allowConnections = psqlQuery(psqlBin, adminUrl, `select datallowconn::text from pg_database where datname = '${TEMPLATE_DATABASE}';`);
  if (remaining !== '0' || allowConnections !== 'true') {
    fail('WEEKLY_SOURCE_DB_ADAPTER_CLEANUP_INCOMPLETE', 'Database group clones or the template connection fence were not cleaned up.');
  }

  const cleanup = {
    complete: true,
    cloneCountAfter: 0,
    templateConnectionsRestored: true,
  };
  const resultEnvelopeDigest = releaseEvidenceEligible
    ? await writeDatabaseEvidence({ mode, repoRoot, resultDirectory, evidence, cleanup })
    : null;
  const claimGroups = executionGroups.filter((group) => {
    const original = WEEKLY_SOURCE_DATABASE_GROUPS.find((candidate) => candidate.id === group.id);
    return original && original.files.every((file) => !pending.has(file));
  });
  const componentResultEnvelopeDigest = releaseEvidenceEligible
    ? null
    : await writeComponentDatabaseEvidence({
      mode, repoRoot, resultDirectory, evidence, cleanup, claimGroups, dependencyPending: pending,
    });
  const serialSuites = await runSerialSuites({
    mode,
    connectionUrl,
    repoRoot,
    resultDirectory,
    psqlBin,
    runSerialOnUpgrade,
    serialDependencyPending,
  });

  return {
    complete: true,
    mode,
    templateDatabase: TEMPLATE_DATABASE,
    groupCount: evidence.length,
    groups: evidence,
    realWorldEvidence,
    namedSessions,
    serialGroup: WEEKLY_SOURCE_SERIAL_GROUP,
    serialSuites,
    cleanup,
    resultEnvelopeDigest,
    componentResultEnvelopeDigest,
    releaseEvidenceEligible,
    dependencyPending: [...pending].sort(),
    serialDependencyPending: serialSuites.executed === false && serialSuites.reason === 'HANDOVER2_IMPLEMENTATION_PENDING',
  };
}

export async function executeWeeklySourceDatabaseScenarios(options) {
  return executeDatabaseScenarios(options);
}

export async function executeWeeklySourceComponentDatabaseScenarios(options) {
  return executeDatabaseScenarios({
    ...options,
    dependencyPendingFiles: WEEKLY_SOURCE_COMPONENT_HANDOVER2_PENDING_FILES,
    releaseEvidenceEligible: false,
    serialDependencyPending: true,
  });
}
