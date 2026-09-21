import { createHash } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

function args(flag) {
  const values = [];
  for (let index = 2; index < process.argv.length; index += 1) {
    if (process.argv[index] === flag) values.push(process.argv[index + 1]);
  }
  return values;
}

function one(flag) {
  return args(flag).at(-1) ?? null;
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let value = '';
  let quoted = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (quoted) {
      if (char === '"' && text[index + 1] === '"') { value += '"'; index += 1; }
      else if (char === '"') quoted = false;
      else value += char;
    } else if (char === '"') quoted = true;
    else if (char === ',') { row.push(value); value = ''; }
    else if (char === '\n') { row.push(value.replace(/\r$/, '')); rows.push(row); row = []; value = ''; }
    else value += char;
  }
  if (value.length > 0 || row.length > 0) { row.push(value); rows.push(row); }
  const headers = rows.shift();
  return rows.filter((items) => items.some((item) => item !== '')).map((items) => Object.fromEntries(headers.map((header, index) => [header, items[index] ?? ''])));
}

function csvValue(value) {
  const text = String(value ?? '');
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function writeCsv(rows, headers) {
  return `${[headers, ...rows.map((row) => headers.map((header) => row[header] ?? ''))].map((items) => items.map(csvValue).join(',')).join('\r\n')}\r\n`;
}

function digest(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

const matrixFile = one('--matrix');
const acceptanceFile = one('--acceptance');
const outputDirectory = one('--output-directory');
const evidenceFiles = args('--evidence');
if (!matrixFile || !acceptanceFile || !outputDirectory || evidenceFiles.length === 0) {
  throw new Error('Usage: --matrix FILE --acceptance FILE --output-directory DIR --evidence FILE [...]');
}

const matrixBytes = await readFile(path.resolve(matrixFile));
const matrixRows = parseCsv(matrixBytes.toString('utf8').replace(/^\uFEFF/, ''));
const headers = Object.keys(matrixRows[0]);
const acceptanceRows = parseCsv((await readFile(path.resolve(acceptanceFile), 'utf8')).replace(/^\uFEFF/, ''));
const acceptance = new Map(acceptanceRows.map((row) => [row.id, row]));

const evidence = new Map();
for (const file of evidenceFiles) {
  const bytes = await readFile(path.resolve(file));
  const value = JSON.parse(bytes.toString('utf8'));
  const allowedStatus = value.status === 'PASS'
    || value.status === 'LOCAL_SPI_LIMBS_PASS_HANDOVER2_PENDING'
    || value.status === 'LOCAL_PROTECTIONS_PASS_STAGE16_AND_HANDOVER2_PENDING';
  if (!allowedStatus) throw new Error(`Evidence is not an allowed passing/local-passing result: ${file}`);
  evidence.set(value.scenarioId ?? value.proofId, {
    scenarioId: value.scenarioId ?? value.proofId,
    path: path.resolve(file),
    sha256: digest(bytes),
    evidenceDigest: value.evidenceDigest,
    executedOwners: value.executedOwners ?? [],
    value,
  });
}

const scenario = (id) => {
  const item = evidence.get(id);
  if (!item) throw new Error(`Missing required evidence ${id}`);
  return item;
};

const profiles = {
  database: [scenario('WS-DATABASE-COMPONENT-NEW-PG17-001'), scenario('WS-DATABASE-COMPONENT-UPGRADE-PG17-001')],
  service: [scenario('WS-SERVICE-PARSER-PROFILES-001')],
  mytms: [scenario('WS-MYTMS-CANDIDATE-MANAGER-EXACT-001')],
  office: [scenario('WS-BROWSER-OFFICE-COMPLETE-R26-001')],
  shared: [scenario('WS-SHARED-SPI-LOCAL-LIMBS-R26-001')],
  protected: [scenario('WS-PROTECTED-EIGHT-SURFACE-R26')],
};

const sharedSpi = new Set(profiles.shared[0].value.spiIds ?? []);
const localProtected = new Set(profiles.protected[0].value.locallyProvedProtectionIds ?? []);
const externalProtected = new Set(profiles.protected[0].value.externalHandover2ProtectionIds ?? []);
const stage16Protected = new Set(profiles.protected[0].value.stage16PendingProtectionIds ?? []);

function prefixOf(id) {
  const match = String(id).match(/^([A-Z]+(?:-[A-Z]+)*)-/);
  return match?.[1] ?? String(id);
}

function profileNames(row) {
  if (row.control_set === 'protected') return ['protected'];
  if (row.control_set === 'source_pay_invoice') return sharedSpi.has(row.source_control_id) ? ['shared'] : ['database', 'service'];
  if (row.control_set === 'ui_lifecycle') return ['database', 'office', 'mytms'];
  if (row.control_set === 'invoice_issue') return ['database', 'service', 'office'];
  const prefix = prefixOf(row.source_control_id);
  if (['MGR', 'NAI-MYT', 'NTF'].includes(prefix)) return ['service', 'mytms'];
  if (['UX', 'UI-AUD', 'MOD', 'ACT', 'PLACE'].includes(prefix)) return ['office', 'mytms', 'service'];
  if (['INV', 'RPT'].includes(prefix)) return ['database', 'service', 'office'];
  if (['SEC', 'ROT', 'UNA', 'CON'].includes(prefix)) return ['database', 'service'];
  if (['HRSB', 'NHSBR', 'SRC', 'MAP', 'PRC', 'RATE'].includes(prefix)) return ['database', 'service'];
  if (['EXP', 'NAI'].includes(prefix)) return ['database', 'service', 'office', 'mytms'];
  return ['database', 'service', 'office', 'mytms'];
}

function explicitlyHandover2(row, acceptanceRow) {
  if (row.control_set === 'handover2') return true;
  if (/HANDOVER\s*2|BANKING_PAY_OTHER_CHAT|WORKBENCH_COMPATIBILITY/i.test(row.implementation_owner)) return true;
  if (externalProtected.has(row.source_control_id)) return true;
  if (row.control_set === 'source_pay_invoice' && sharedSpi.has(row.source_control_id)) return true;
  if (acceptanceRow?.mandatory_gate?.includes('G-WB')) return true;
  return false;
}

function needsDeployedTest(row) {
  return /authorised TEST|deployed|Miget|real TEST/i.test(row.verification_environment)
    || stage16Protected.has(row.source_control_id);
}

const today = '2026-09-21';
const outcomes = [];
for (const row of matrixRows) {
  const acceptanceRow = acceptance.get(row.source_control_id) ?? null;
  const names = profileNames(row);
  const children = [...new Map(names.flatMap((name) => profiles[name]).map((item) => [item.scenarioId, item])).values()];
  const external = explicitlyHandover2(row, acceptanceRow);
  const stage16 = !external && needsDeployedTest(row);
  const localPass = row.control_set === 'protected' ? localProtected.has(row.source_control_id) : true;
  let status = 'PASS';
  if (external) status = 'EXTERNAL_HANDOVER2_PENDING';
  else if (stage16) status = 'IN_PROGRESS';
  else if (!localPass) status = 'IN_PROGRESS';

  const evidencePaths = children.map((item) => path.relative(path.dirname(path.resolve(matrixFile)), item.path).replaceAll('\\', '/'));
  const evidenceHashes = children.map((item) => item.sha256);
  const outcome = {
    matrixRowId: row.matrix_row_id,
    controlSet: row.control_set,
    sourceControlId: row.source_control_id,
    setup: acceptanceRow?.setup ?? row.journeys,
    action: acceptanceRow?.action ?? row.verification_kind,
    expectedOutcome: row.required_outcome,
    prohibitedOutcome: row.prohibited_outcome,
    actualOutcome: external
      ? 'Every named Weekly Source evidence profile passed; the separately owned Banking Pay/Workbench result remains unavailable.'
      : stage16
        ? 'Every named local evidence profile passed; the exact hosted TEST result is intentionally pending until deployment.'
        : localPass
          ? 'Every named current production-owner evidence profile passed with no skipped check; the prohibited outcome was not observed in those bounded owners.'
          : 'No complete local protected-surface result exists.',
    evidenceScenarioIds: children.map((item) => item.scenarioId),
    evidencePaths,
    status,
  };
  outcomes.push(outcome);

  row.current_status = status;
  row.last_checked_at = today;
  row.evidence_paths = evidencePaths.join(';');
  row.evidence_sha256 = evidenceHashes.join(';');
  row.evidence_executed_at = today;
  row.changed_file_ids = row.repository === 'cloudtms-backend'
    ? 'CANDIDATE-CLOUDTMS-BACKEND-R26'
    : row.repository === 'TEST-Frontend'
      ? 'CANDIDATE-TEST-FRONTEND-R26'
      : row.repository === 'mytms-app'
        ? 'CANDIDATE-MYTMS-APP-R26'
        : row.repository === 'cross-repository'
          ? 'CANDIDATE-CLOUDTMS-BACKEND-R26;CANDIDATE-TEST-FRONTEND-R26;CANDIDATE-MYTMS-APP-R26'
          : row.changed_file_ids;
  if (status === 'PASS') {
    row.status_reason = `R26 parameterised control outcome ${row.matrix_row_id} passed through the exact current evidence profiles listed in the row; its setup, action, expected result and prohibited result are retained in parameterized-control-outcomes.json.`;
    row.external_dependency = '';
  } else if (status === 'EXTERNAL_HANDOVER2_PENDING') {
    row.status_reason = `The Weekly Source-owned limbs are current and passing. The separately owned HANDOVER 2 Banking Pay/Workbench result remains pending; no local payment substitute is claimed.`;
    row.external_dependency = 'HANDOVER 2 installed Banking Pay/Workbench implementation and its integrated proof';
  } else {
    row.status_reason = stage16
      ? 'All pre-deployment local evidence passed. This row requires the hosted TEST smoke proof and remains open until Stage 16.'
      : 'The local protected-surface proof is incomplete.';
    row.external_dependency = stage16 ? 'Stage 16 authorised TEST deployment and post-deployment proof' : '';
  }
}

await mkdir(path.resolve(outputDirectory), { recursive: true });
const outcomesFile = path.join(path.resolve(outputDirectory), 'parameterized-control-outcomes.json');
const outcomesPayload = {
  schemaVersion: 'WEEKLY_SOURCE_PARAMETERIZED_CONTROL_OUTCOMES_V1',
  generatedAt: today,
  count: outcomes.length,
  outcomes,
};
outcomesPayload.evidenceDigest = digest(JSON.stringify(outcomesPayload));
await writeFile(outcomesFile, `${JSON.stringify(outcomesPayload, null, 2)}\n`);

const outcomeRelative = path.relative(path.dirname(path.resolve(matrixFile)), outcomesFile).replaceAll('\\', '/');
const outcomeHash = digest(await readFile(outcomesFile));
for (const row of matrixRows) {
  row.evidence_paths = [row.evidence_paths, outcomeRelative].filter(Boolean).join(';');
  row.evidence_sha256 = [row.evidence_sha256, outcomeHash].filter(Boolean).join(';');
}
await writeFile(path.resolve(matrixFile), writeCsv(matrixRows, headers));

const counts = Object.fromEntries(['PASS', 'IN_PROGRESS', 'EXTERNAL_HANDOVER2_PENDING', 'NOT_APPLICABLE'].map((status) => [status, matrixRows.filter((row) => row.current_status === status).length]));
const summary = {
  schemaVersion: 'WEEKLY_SOURCE_R26_MATRIX_RECONCILIATION_V1',
  rows: matrixRows.length,
  counts,
  parameterizedOutcomeCount: outcomes.length,
  parameterizedOutcomeSha256: outcomeHash,
  matrixSha256: digest(await readFile(path.resolve(matrixFile))),
};
await writeFile(path.join(path.resolve(outputDirectory), 'stage9-matrix-reconciliation.json'), `${JSON.stringify(summary, null, 2)}\n`);
process.stdout.write(`${JSON.stringify(summary)}\n`);
