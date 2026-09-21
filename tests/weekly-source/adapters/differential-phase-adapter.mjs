// Weekly Source Plan 6.2 — the differential phase adapter (WP-16c).
//
// `scripts/run-weekly-source-harness.mjs` requires
// `CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER` for the `differential` phase and
// for `all`, and until now no differential adapter existed anywhere in the
// repository (WP-16b handoff N3). WP-16b shipped the comparison owner and its
// contract in `tests/weekly-source/harness/differential-protection-contract.mjs`
// and originally left it unpopulated. This file is the adapter that now
// measures the 28 database-owned protected areas; the app-owned area is proved
// by the rendered browser/device differential.
//
// WHAT A BASELINE IS HERE, AND WHY IT IS HONEST.
//
// `23A` section 12 asks for an approved BEFORE baseline, re-run after each
// implementation slice and compared surface by surface. A `git checkout` of the
// pre-implementation commit is forbidden by the work-package rules and would
// destroy other packages' uncommitted work, so the BEFORE side is built the way
// WP-09 built its negative control: every database file this project modified is
// read back at its `origin/test` blob with `git show`, which is read-only, and
// applied to a disposable BEFORE clone. The AFTER clone is the same build with
// the worktree's own definitions. Both are then measured with the same query.
//
// WHAT THIS ADAPTER CAN AND CANNOT SEE.
//
// It measures the one `23A` section 12 surface a database phase can measure,
// "database rows and projections", as the exact installed definition, privileges
// and trigger attachments of each protected area's owners. It cannot see the
// Office layout, the Candidate app, the invoice document, the C1 request shape
// or the report/export fields: those are the browser and service phases. A
// protected area whose owners this adapter cannot enumerate is recorded
// `NOT_CAPTURED` with its reason and is **excluded from the passing set**, so
// the coverage gate stays red for it. An area is never counted as covered
// because nothing was found to compare.

import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { canonicalDigest } from '../harness/canonical-json.mjs';
import { createResultEnvelope, writeResultEnvelope } from '../harness/result-envelope.mjs';
import { spiIdsForEvidenceGroup } from '../harness/spi-execution-map.mjs';
import {
  WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT,
  compareDifferentialCaptures,
  createDifferentialCapture,
} from '../harness/differential-protection-contract.mjs';
import {
  loadAndVerifyControllingLedgers,
  parseCsv,
} from '../harness/evidence-coverage-ledger.mjs';

const DATABASE_SURFACE = 'database rows and projections';
const DATABASE_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;

/**
 * The owners of each protected area that a database phase can measure.
 *
 * `routines` are matched as exact `nspname.proname` values; `relations` bring in
 * their trigger attachments and row-level policies. `notCaptured` states, for
 * the record, which of the eight `23A` section 12 surfaces this phase cannot
 * reach for that area, so nobody reads a database-only pass as a full one.
 */
export const WEEKLY_SOURCE_PROTECTED_OWNERS = Object.freeze({
  'PROT-ROTATION-001': {
    routines: ['public.timesheet_route_version_rotate', 'public.timesheet_route_version_confirmed_v1',
      'private._timesheet_route_version_core_v1', 'private._timesheet_route_version_legacy_v1',
      'private._timesheet_route_supersede_candidate_v1', 'public.timesheet_qr_restore_version',
      'public.timesheet_qr_refuse_and_reset', 'private._candidate_timesheet_reject_rotate_v1'],
    relations: ['public.timesheets'],
    requiredDifferential: 'ROT-010',
  },
  'PROT-UNAUTH-001': {
    routines: ['public.timesheet_unauthorise_atomic', 'public.timesheet_unauthorise_bulk_atomic',
      'public.timesheet_authorise_generic_atomic'],
    relations: ['public.timesheets_financials'],
    requiredDifferential: 'UNA-012',
  },
  'PROT-PAY-001': {
    routines: ['public.timesheet_authorise_generic_atomic', 'public.tsfin_prepare_write',
      'public.tsfin_mark_revoked', 'public.tsfin_write_current_snapshot_single_bounded',
      'public.tsfin_write_snapshots_and_complete'],
    relations: ['public.contract_weeks'],
  },
  'PROT-DAILY-001': {
    routines: ['public.timesheet_daily_manual_process_atomic'],
    relations: [],
  },
  'PROT-ORDW-001': {
    routines: ['public.contract_week_manual_upsert_atomic',
      'public.contract_week_manual_upsert_bulk_process_atomic',
      'public.contract_week_manual_unprocess_atomic'],
    relations: [],
  },
  'PROT-INVDOC-001': {
    routines: ['public.invoice_issue_one', 'public.invoice_detail_get', 'public.invoice_reference_rows'],
    relations: ['public.invoice_lines'],
  },
  'PROT-ISSUED-001': {
    routines: ['public.invoice_apply_edits'],
    relations: ['public.invoices'],
  },
  'PROT-WB-001': {
    routines: ['private.pay_workbench_scope_invalidate_v1', 'public.pay_workbench_mark_candidate_dirty',
      'public._pay_workbench_candidate_serial_try_gate', 'public._pay_timesheet_rotation_scope'],
    relations: [],
  },
  'PROT-CANCEL-001': {
    routines: ['public.pay_payment_correction_request_start', 'public.pay_payment_correction_process_chunk',
      'public.pay_pre_bank_cancel_apply_work_item', 'public.pay_batch_abort_failed_draft_create_partial'],
    relations: [],
  },
  'PROT-DRAFT-001': {
    routines: ['public.pay_batch_create_timesheet_snapshots', 'public.pay_settle_rail',
      'public.pay_set_paye_net_manual'],
    relations: ['public.pay_batch_items'],
  },
  'PROT-AUDIT-001': {
    routines: ['public._audit_insert'],
    relations: ['public.audit_events'],
  },
});

/** Areas this phase declares it cannot measure, each with the reason. */
export const WEEKLY_SOURCE_PROTECTED_NOT_CAPTURED = Object.freeze({
  'PROT-BP-001': 'Banking Pay screens and provider/settlement UI: browser phase',
  'PROT-REM-001': 'PAYE payout notice and Umbrella remittance documents: service phase',
  'PROT-BANKALERT-001': 'Banking alert ledger badge and popover: browser phase',
  'PROT-INV-001': 'source-only self-bill invoice authority end to end: service phase',
  'PROT-INVMOVE-001': 'guarded unissue/reissue and segment placement: service phase',
  'PROT-ORDINV-001': 'ordinary evidence-required invoicing and consolidation: service phase',
  'PROT-ADV-001': 'ADVANCE_THIS_PAYMENT timing override: Banking Pay boundary evidence',
  'PROT-EXP-001': 'ordinary receipt and mileage expense route: browser and service phases',
  'PROT-CONTRACT-001': 'Contract creation and valid selection: browser phase',
  'PROT-RATE-001': 'rate engine outputs and stored legacy segment shape: service phase',
  'PROT-APP-001': 'MyTMS layout and navigation: browser phase',
  'PROT-SUMMARY-001': 'Timesheet Summary modal behaviour: browser phase',
  'PROT-EXPORT-001': 'reports and exports: service phase',
  'PROT-SETTINGS-001': 'settings layout and behaviour: browser phase',
  'PROT-NOTIFY-001': 'notification delivery semantics: service phase',
  'PROT-SEC-001': 'tenant and security boundaries: the named security verifiers and the browser phase',
  'PROT-LEGACY-001': 'legacy Timesheets and invoices: service phase',
  'PROT-INFRA-001': 'production timing and resource limits: not reproducible on a disposable clone',
});

function fail(code, message, details = {}) {
  const error = new Error(message);
  error.name = 'WeeklySourceDifferentialAdapterError';
  error.code = code;
  error.details = details;
  throw error;
}

function redact(text) {
  return String(text ?? '')
    .replace(/postgres(?:ql)?:\/\/\S+/gi, '[local database target redacted]')
    .replace(/\b(password|passphrase|secret|api[_-]?key|authorization)\s*[:=]\s*\S+/gi, '$1=[redacted]')
    .trim();
}

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: options.cwd,
    encoding: 'utf8',
    env: options.env ?? process.env,
    timeout: options.timeoutMs ?? 600_000,
    maxBuffer: 256 * 1024 * 1024,
    windowsHide: true,
  });
  return {
    ok: !result.error && result.status === 0,
    stdout: String(result.stdout ?? ''),
    stderr: redact(result.stderr ?? result.error?.message ?? ''),
  };
}

function psqlBin() { return process.env.PSQL_BIN ?? 'psql'; }

function targetFor(connectionUrl, database) {
  if (!DATABASE_PATTERN.test(String(database ?? ''))) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_DATABASE_INVALID', 'A differential database name must be a short lower-case identifier.');
  }
  let url;
  try { url = new URL(connectionUrl); } catch {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_TARGET_INVALID', 'The differential adapter received an invalid local target.');
  }
  if (url.protocol !== 'postgresql:' || url.hostname !== '127.0.0.1' || !url.port) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_TARGET_REFUSED', 'The differential adapter runs against the local disposable cluster only.');
  }
  url.pathname = `/${encodeURIComponent(database)}`;
  return url.toString();
}

function query(url, sql) {
  const result = run(psqlBin(), [url, '-X', '-q', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-c', sql]);
  if (!result.ok) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_QUERY_FAILED', `A differential measurement failed. ${result.stderr}`.slice(0, 600));
  }
  return result.stdout.trim();
}

/**
 * The one measurable surface, as a single digest per protected area.
 *
 * It covers the exact installed definition of each owner, its owner role, its
 * security mode, its `search_path` setting, every grant on it, and every trigger
 * and row-level policy attached to the area's relations. Line endings are
 * normalised first: installed TEST holds CRLF inside routine bodies while a
 * clean local build holds LF, and the environment report records that comparing
 * without normalising reports 38 false differences.
 */
function measureArea(url, area) {
  const routines = (area.routines ?? []).map((name) => `'${name}'`).join(',') || `''`;
  const relations = (area.relations ?? []).map((name) => `'${name}'`).join(',') || `''`;
  const sql = `
    with routine_rows as (
      select n.nspname||'.'||p.proname||'('||pg_catalog.pg_get_function_identity_arguments(p.oid)||')'
             ||'|owner='||pg_catalog.pg_get_userbyid(p.proowner)
             ||'|secdef='||p.prosecdef::text
             ||'|config='||coalesce(pg_catalog.array_to_string(p.proconfig,','),'-')
             ||'|acl='||coalesce(pg_catalog.array_to_string(p.proacl::text[],','),'-')
             ||'|def='||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                 pg_catalog.replace(pg_catalog.pg_get_functiondef(p.oid),chr(13),''),'UTF8')),'hex')
             as row_text
      from pg_catalog.pg_proc p
      join pg_catalog.pg_namespace n on n.oid=p.pronamespace
      where n.nspname||'.'||p.proname in (${routines})
    ), trigger_rows as (
      select 'trigger|'||c.relname||'|'||t.tgname||'|'
             ||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                 pg_catalog.replace(pg_catalog.pg_get_triggerdef(t.oid),chr(13),''),'UTF8')),'hex')
             as row_text
      from pg_catalog.pg_trigger t
      join pg_catalog.pg_class c on c.oid=t.tgrelid
      join pg_catalog.pg_namespace n on n.oid=c.relnamespace
      where not t.tgisinternal and n.nspname||'.'||c.relname in (${relations})
    ), policy_rows as (
      select 'policy|'||c.relname||'|'||pol.polname||'|'
             ||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                 coalesce(pg_catalog.pg_get_expr(pol.polqual,pol.polrelid),'-')
                 ||coalesce(pg_catalog.pg_get_expr(pol.polwithcheck,pol.polrelid),'-'),'UTF8')),'hex')
             as row_text
      from pg_catalog.pg_policy pol
      join pg_catalog.pg_class c on c.oid=pol.polrelid
      join pg_catalog.pg_namespace n on n.oid=c.relnamespace
      where n.nspname||'.'||c.relname in (${relations})
    ), all_rows as (
      select row_text from routine_rows
      union all select row_text from trigger_rows
      union all select row_text from policy_rows
    )
    select coalesce(pg_catalog.count(*),0)::text||'@'
      ||pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
         coalesce(pg_catalog.string_agg(row_text,chr(10) order by row_text),''),'UTF8')),'hex')
    from all_rows;`;
  const [count, digest] = query(url, sql).split('@');
  return { measuredRowCount: Number(count), digest };
}

/** Every database file this project modified, at its `origin/test` blob. */
export function preImplementationFiles(repoRoot, baseRef = 'origin/test') {
  const status = run('git', ['status', '--porcelain', '--', 'supabase/repeatable'], { cwd: repoRoot });
  if (!status.ok) fail('WEEKLY_SOURCE_DIFFERENTIAL_GIT_UNAVAILABLE', 'The repository state is unavailable.');
  const files = [];
  for (const line of status.stdout.split(/\r?\n/)) {
    const match = /^ ?M\s+(supabase\/repeatable\/.+\.sql)$/.exec(line.trimEnd());
    if (match) files.push({ file: match[1], ref: `${baseRef}:${match[1]}` });
  }
  return files;
}

/**
 * Build the BEFORE side: the AFTER clone with every modified repeatable rolled
 * back to its `origin/test` blob. `git show` is read-only; nothing is checked
 * out and no worktree file is touched.
 */
function applyPreImplementationDefinitions(repoRoot, url, files) {
  const scratch = mkdtempSync(path.join(tmpdir(), 'ws-differential-before-'));
  const applied = [];
  const refused = [];
  try {
    for (const entry of files) {
      const blob = run('git', ['show', entry.ref], { cwd: repoRoot });
      if (!blob.ok) { refused.push({ file: entry.file, reason: 'BLOB_UNAVAILABLE' }); continue; }
      const target = path.join(scratch, path.basename(entry.file));
      writeFileSync(target, blob.stdout, 'utf8');
      const outcome = run(psqlBin(), [url, '-X', '-q', '-v', 'ON_ERROR_STOP=1', '-f', target]);
      if (outcome.ok) applied.push(entry.file);
      else refused.push({ file: entry.file, reason: outcome.stderr.slice(-200) });
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }
  return { applied, refused };
}

function gitCommit(repoRoot) {
  const result = run('git', ['rev-parse', 'HEAD'], { cwd: repoRoot });
  const commit = result.stdout.trim();
  if (!result.ok || !/^[a-f0-9]{40}$/.test(commit)) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_COMMIT_UNAVAILABLE', 'The backend commit identity is unavailable.');
  }
  return commit;
}

const FIXED_CLOCK = '2026-09-18T00:00:00Z';

const CANONICAL_POLICY_AUTHORITIES = Object.freeze({
  // UNA-001 authorises the guarded first-authorisation withdrawal and the
  // family-wide ordinary unauthorise census.  That deliberately changes the
  // unauthorise owner measured by both the unauthorise and pre-existing
  // ADVANCE_THIS_PAYMENT protection areas; it does not authorise any change to
  // the marker itself.
  'PROT-ADV-001': Object.freeze(['UNA-001']),
  // QRY-001 is the current no-submission/query policy that adds the concise
  // waiting-for-final-source state to the existing Timesheet Summary owner.
  'PROT-SUMMARY-001': Object.freeze(['QRY-001']),
  'PROT-ROTATION-001': Object.freeze(['ROT-001']),
  'PROT-UNAUTH-001': Object.freeze(['UNA-001']),
});

async function loadIndependentAuthorityIndex(packRoot) {
  if (!packRoot) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_PACK_REQUIRED',
      'The sealed Plan 6.2 pack is required to validate protected-difference authorities.');
  }
  const ledgers = await loadAndVerifyControllingLedgers(packRoot);
  const authorityIndex = new Map();
  for (const row of ledgers.requirements) {
    authorityIndex.set(row.requirement_id, {
      kind: 'ATOMIC_REQUIREMENT',
      protectedIds: String(row.protected_areas ?? '').split(';').map((value) => value.trim()).filter(Boolean),
    });
  }
  const policyPath = path.join(packRoot, 'annexes', 'requirements-traceability.csv');
  const policyRows = parseCsv(readFileSync(policyPath, 'utf8'), 'requirements-traceability.csv');
  if (policyRows.length !== 203) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_POLICY_REGISTRY_INVALID',
      `The canonical policy registry has ${policyRows.length} rows; 203 are required.`);
  }
  const canonicalIds = new Set(policyRows.map((row) => String(row.id ?? '').trim()));
  for (const [protectionId, authorityIds] of Object.entries(CANONICAL_POLICY_AUTHORITIES)) {
    for (const authorityId of authorityIds) {
      if (!canonicalIds.has(authorityId)) {
        fail('WEEKLY_SOURCE_DIFFERENTIAL_POLICY_AUTHORITY_MISSING',
          `${authorityId} is absent from the canonical 203-row policy registry.`);
      }
      const existing = authorityIndex.get(authorityId);
      authorityIndex.set(authorityId, {
        kind: 'CANONICAL_POLICY_REQUIREMENT',
        protectedIds: [...new Set([...(existing?.protectedIds ?? []), protectionId])].sort(),
      });
    }
  }
  return authorityIndex;
}

export async function runWeeklySourceHarnessPhase({
  phase, repoRoot, packRoot, resultDirectory, connectionUrl,
  beforeDatabase = 'ws_differential_before', afterDatabase = 'ws_differential_after',
  templateDatabase = process.env.WP16C_TEMPLATE ?? 'ws62_wp16c_template',
  prebuiltBeforeDatabase = process.env.CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_BEFORE_DB ?? null,
  prebuiltAfterDatabase = process.env.CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_AFTER_DB ?? null,
  prebuiltBeforeProvenance = process.env.CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_BEFORE_PROVENANCE ?? null,
  prebuiltAfterProvenance = process.env.CLOUDTMS_WEEKLY_SOURCE_DIFFERENTIAL_AFTER_PROVENANCE ?? null,
}) {
  if (phase !== 'differential') {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_PHASE_INVALID', 'The differential adapter accepts only the differential phase.');
  }
  if (!connectionUrl) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_TARGET_REQUIRED', 'The differential adapter needs the proved local target.');
  }
  // Validate the target before touching policy inputs, and never accept an
  // expected-difference map from a caller.
  targetFor(connectionUrl, 'postgres');
  // The sealed 23A contract permits only a canonical Requirement ID. The authority file states the expected
  // change, but never authenticates itself: every ID is independently resolved
  // from the sealed/pinned ledgers before the comparison can run.
  const authorityPath = path.join(repoRoot, 'tests', 'weekly-source', 'fixtures',
    'protected-differential-authorities.json');
  const authority = JSON.parse(readFileSync(authorityPath, 'utf8'));
  if (authority?.schemaVersion !== 'WEEKLY_SOURCE_PROTECTED_DIFFERENTIAL_AUTHORITIES_V1'
    || authority?.surface !== DATABASE_SURFACE
    || !Array.isArray(authority?.authorities)) {
    fail('WEEKLY_SOURCE_DIFFERENTIAL_AUTHORITY_INVALID',
      'The checked-in protected-difference authority file is invalid.');
  }
  const expectedDifferences = authority.authorities.map((entry) => ({
    ...entry,
    surface: DATABASE_SURFACE,
  }));
  const requirementIndex = await loadIndependentAuthorityIndex(packRoot);
  for (const entry of expectedDifferences) {
    const resolved = requirementIndex.get(entry.requirementId);
    if (!resolved || !resolved.protectedIds.includes(entry.protectionId)) {
      fail('WEEKLY_SOURCE_DIFFERENTIAL_AUTHORITY_ORPHAN',
        `${entry.requirementId} is not an independent authority for ${entry.protectionId}.`);
    }
  }
  // WP-16d. When the caller has already built a BEFORE database from the committed baseline
  // tree and an AFTER database from the working tree, run the full 29-area rule set over
  // those two builds instead of rolling modified repeatables back onto one clone. The clone
  // route below leaves every file the project ADDED present on both sides, which is why it
  // could only reach 11 of the 29.
  if (prebuiltBeforeDatabase && prebuiltAfterDatabase) {
    const { runProtectedAreaDifferential } = await import('./differential-protected-area-runner.mjs');
    const differential = await runProtectedAreaDifferential({
      query: (database, sql) => query(targetFor(connectionUrl, database), sql),
      beforeDatabase: prebuiltBeforeDatabase,
      afterDatabase: prebuiltAfterDatabase,
      repositoryCommit: gitCommit(repoRoot),
      capturedAtUtc: FIXED_CLOCK,
      expectedDifferences,
      requirementIndex,
      resultDirectory,
      buildProvenance: {
        // A caller may provide a more exact bounded-build description.  Do not
        // label every prebuilt database as a full NEW release: component proofs
        // intentionally stop at the separately owned HANDOVER 2 boundary.
        before: prebuiltBeforeProvenance
          ?? `separately built committed baseline database ${prebuiltBeforeDatabase}`,
        after: prebuiltAfterProvenance
          ?? `separately built working-tree database ${prebuiltAfterDatabase}`,
      },
    });
    return {
      ...differential,
      evidence: [{
        surface: DATABASE_SURFACE,
        summary: differential.summary,
        buildProvenance: differential.buildProvenance,
        measured: differential.measured,
        unmeasurable: differential.unmeasurable,
      }],
    };
  }
  const adminUrl = targetFor(connectionUrl, 'postgres');
  const commit = gitCommit(repoRoot);

  const createdDatabases = [];
  const results = [];
  const notCaptured = [];
  let baselineApplication = null;

  try {
    for (const database of [afterDatabase, beforeDatabase]) {
      query(adminUrl, `drop database if exists ${database} with (force);`);
      query(adminUrl, `create database ${database} template ${templateDatabase};`);
      query(adminUrl, `alter database ${database} set jit = off;`);
      createdDatabases.push(database);
    }
    const afterUrl = targetFor(connectionUrl, afterDatabase);
    const beforeUrl = targetFor(connectionUrl, beforeDatabase);

    baselineApplication = applyPreImplementationDefinitions(
      repoRoot, beforeUrl, preImplementationFiles(repoRoot));

    for (const [protectionId, area] of Object.entries(WEEKLY_SOURCE_PROTECTED_OWNERS)) {
      const beforeMeasure = measureArea(beforeUrl, area);
      const afterMeasure = measureArea(afterUrl, area);
      if (beforeMeasure.measuredRowCount === 0 || afterMeasure.measuredRowCount === 0) {
        notCaptured.push({
          protectionId,
          reason: 'no owner of this protected area could be measured in the database, so nothing '
            + 'was compared and coverage is NOT claimed',
        });
        continue;
      }
      const before = createDifferentialCapture({
        protectionId,
        phase: 'BEFORE',
        surfaces: { [DATABASE_SURFACE]: beforeMeasure },
        capturedAtUtc: FIXED_CLOCK,
        repositoryCommit: commit,
      });
      const after = createDifferentialCapture({
        protectionId,
        phase: 'AFTER',
        surfaces: { [DATABASE_SURFACE]: afterMeasure },
        capturedAtUtc: FIXED_CLOCK,
        repositoryCommit: commit,
      });
      results.push(compareDifferentialCaptures(before, after, {
        expectedDifferences: expectedDifferences.filter((entry) => entry.protectionId === protectionId),
      }));
    }

    for (const [protectionId, reason] of Object.entries(WEEKLY_SOURCE_PROTECTED_NOT_CAPTURED)) {
      notCaptured.push({ protectionId, reason });
    }
  } finally {
    for (const database of createdDatabases) {
      try { query(adminUrl, `drop database if exists ${database} with (force);`); } catch { /* best effort */ }
    }
  }

  const passing = results.filter((result) => result.pass).map((result) => result.protectionId).sort();
  const adverse = results.filter((result) => !result.pass);
  const complete = notCaptured.length === 0 && adverse.length === 0;

  if (resultDirectory) {
    const expected = {
      phase: 'differential',
      surface: DATABASE_SURFACE,
      protectedAreaCount: WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.protectedAreaCount,
      comparedCount: results.length,
      adverseCount: 0,
      notCapturedCount: 0,
    };
    const actual = {
      phase: 'differential',
      surface: DATABASE_SURFACE,
      protectedAreaCount: WEEKLY_SOURCE_DIFFERENTIAL_ADAPTER_CONTRACT.protectedAreaCount,
      comparedCount: results.length,
      adverseCount: adverse.length,
      notCapturedCount: notCaptured.length,
      protectionResults: results.map((result) => ({
        protectedId: result.protectionId,
        surface: 'DATABASE',
        result: result.pass ? 'PASS' : 'FAIL',
        executedChecks: ['BEFORE_CAPTURE', 'AFTER_CAPTURE', 'AUTHORISED_DIFFERENCE_COMPARISON'],
        observedResults: [
          `before=${result.beforeDigest}`,
          `after=${result.afterDigest}`,
          `adverseSurfaces=${(result.adverseSurfaces ?? []).join('|') || 'NONE'}`,
        ],
        prohibitedOutcomeChecks: [result.pass ? 'NO_UNAUTHORISED_DIFFERENCE' : 'UNAUTHORISED_DIFFERENCE_FOUND'],
      })),
    };
    const envelope = createResultEnvelope({
      scenario: {
        schemaVersion: 'WEEKLY_SOURCE_TEST_SCENARIO_V1',
        scenarioId: 'WS-DIFFERENTIAL-PROTECTED-001',
        fixedSeed: createHash('sha256').update('weekly-source-differential-protected-v1').digest('hex'),
        requirementIds: [],
        protectedIds: passing,
      },
      repositories: [{ repository: 'cloudtms-backend', commit }],
      database: { used: true, engine: 'PostgreSQL', mode: 'DIFFERENTIAL', templateDatabase },
      generatedSources: [],
      parser: { used: false },
      clockValuesUtc: [FIXED_CLOCK],
      executedOwners: Object.values(WEEKLY_SOURCE_PROTECTED_OWNERS)
        .flatMap((area) => area.routines ?? []),
      oracle: { expected, expectedDigest: canonicalDigest(expected) },
      actual,
      comparison: {
        pass: canonicalDigest(actual) === canonicalDigest(expected),
        actualDigest: canonicalDigest(actual),
        firstDivergence: adverse.length
          ? { protectionId: adverse[0].protectionId, adverseSurfaces: adverse[0].adverseSurfaces }
          : (notCaptured[0] ?? null),
      },
      c1: { category: 'NONE', emulator: false, releaseEvidenceEligible: true },
      outbox: { calls: [] },
      projectionDigests: [],
      // Only areas with a genuine passing before/after pair. An area nobody
      // measured never reaches this list.
      protectedIds: passing,
      spiIds: spiIdsForEvidenceGroup('protected-differential'),
      cleanup: { complete: true, databaseRowsCreated: 0, externalEffects: 0 },
    });
    await writeResultEnvelope(path.join(resultDirectory, 'differential-protected.json'), envelope);
  }

  return {
    executed: true,
    pass: complete,
    complete,
    evidence: [{
      surface: DATABASE_SURFACE,
      baselineSource: 'git show origin/test:<file> applied to a disposable BEFORE clone; no checkout',
      baselineFilesApplied: baselineApplication?.applied.length ?? 0,
      baselineFilesRefused: baselineApplication?.refused ?? [],
      comparedCount: results.length,
      passingCount: passing.length,
      passing,
      adverse: adverse.map((result) => ({
        protectionId: result.protectionId,
        adverseSurfaces: result.adverseSurfaces,
        beforeDigest: result.beforeDigest,
        afterDigest: result.afterDigest,
      })),
      notCaptured,
      results,
    }],
  };
}
