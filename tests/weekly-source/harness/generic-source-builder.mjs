import { DeterministicIdentityRegistry } from './deterministic-identities.mjs';
import { writeCsvArtifact, poundsTextFromPence } from './csv-fixture-utils.mjs';
import { requireScenarioUpload, SourceFixtureError } from './workbook-fixture-utils.mjs';

const REQUIRED_FIELDS = Object.freeze([
  'identity', 'candidateReference', 'client', 'workDate', 'actualStart', 'actualEnd',
  'actualBreakMinutes', 'actualWorkedMinutes'
]);

function validateDefinition(definition) {
  if (!definition || definition.profileId !== 'GENERIC_WEEKLY_COMPLETE_TEST_V1' || definition.fileType !== 'CSV') {
    throw new SourceFixtureError('GENERIC_PROFILE_NOT_CONFIGURED', 'Generic fixtures require one separately approved, versioned profile definition');
  }
  if (!Array.isArray(definition.headers) || !definition.headers.length || new Set(definition.headers).size !== definition.headers.length) {
    throw new SourceFixtureError('GENERIC_HEADER_INVALID', 'Generic profile headers must be a non-empty unique list');
  }
  if (!definition.columns || REQUIRED_FIELDS.some((field) => !definition.headers.includes(definition.columns[field]))) {
    throw new SourceFixtureError('GENERIC_COLUMN_MAP_INVALID', 'Generic profile column map is incomplete or does not match its exact headers');
  }
  const extraMappings = Object.keys(definition.columns).filter((key) => ![
    ...REQUIRED_FIELDS, 'reference', 'contract', 'role', 'band', 'sourceExpense'
  ].includes(key));
  if (extraMappings.length) throw new SourceFixtureError('GENERIC_COLUMN_MAP_UNKNOWN', `Unknown generic column mapping ${extraMappings[0]}`);
}

export function buildGenericSource(scenario, uploadOrKey, definition) {
  const context = requireScenarioUpload(scenario, uploadOrKey);
  if (context.upload.profile !== 'GENERIC_WEEKLY_COMPLETE_V1') {
    throw new SourceFixtureError('GENERIC_PROFILE_REQUIRED', 'This writer accepts only the generic complete source profile');
  }
  validateDefinition(definition);
  const mutations = new Set(context.upload.mutations || []);
  const headers = [...definition.headers];
  if (mutations.has('WRONG_HEADER')) headers[0] = `Unexpected ${headers[0]}`;
  const registry = new DeterministicIdentityRegistry(scenario.scenarioId);
  const rows = context.upload.physicalRows.map((row, ordinal) => {
    const candidate = context.candidates.get(row.candidateKey);
    const values = Object.fromEntries(definition.headers.map((header) => [header, '']));
    values[definition.columns.identity] = row.requestId || registry.externalKey('generic-source-row', ordinal, { prefix: 'GEN' });
    values[definition.columns.candidateReference] = candidate.tmsRef;
    values[definition.columns.client] = mutations.has('CROSS_CLIENT') ? `${context.client.name} other` : context.client.name;
    values[definition.columns.workDate] = row.workDate;
    values[definition.columns.actualStart] = row.actualStart ?? '';
    values[definition.columns.actualEnd] = row.actualEnd ?? '';
    values[definition.columns.actualBreakMinutes] = row.actualBreakMinutes ?? '';
    values[definition.columns.actualWorkedMinutes] = row.actualWorkedMinutes ?? '';
    if (definition.columns.reference) values[definition.columns.reference] = row.requestId ?? '';
    if (definition.columns.contract) values[definition.columns.contract] = row.contractKey ?? '';
    if (definition.columns.role) values[definition.columns.role] = row.role ?? '';
    if (definition.columns.band) values[definition.columns.band] = row.band ?? '';
    if (definition.columns.sourceExpense) {
      values[definition.columns.sourceExpense] = row.sourceExpensePence === null ? '' : poundsTextFromPence(row.sourceExpensePence);
    }
    return definition.headers.map((header) => values[header]);
  });
  const identityIndex = definition.headers.indexOf(definition.columns.identity);
  if (mutations.has('BLANK_REQUEST_ID') && rows[0]) rows[0][identityIndex] = '';
  if (mutations.has('DUPLICATE_REQUEST_ID') && rows[0]) {
    if (rows[1]) rows[1][identityIndex] = rows[0][identityIndex];
    else rows.push([...rows[0]]);
  }
  return writeCsvArtifact({
    profile: context.upload.profile,
    fileName: `${context.upload.key}.csv`,
    headers,
    rows
  });
}

export { REQUIRED_FIELDS as GENERIC_REQUIRED_FIELDS };

