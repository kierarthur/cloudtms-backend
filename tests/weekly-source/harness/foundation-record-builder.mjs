import { createHash } from 'node:crypto';
import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { DeterministicIdentityRegistry } from './deterministic-identities.mjs';

export class FoundationBuilderError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'FoundationBuilderError';
    this.code = code;
  }
}

function uniqueByKey(items, group) {
  const result = new Map();
  for (const item of items) {
    if (result.has(item.key)) throw new FoundationBuilderError('FOUNDATION_KEY_DUPLICATE', `${group} contains duplicate key ${item.key}`);
    result.set(item.key, item);
  }
  return result;
}

function sorted(items) {
  return [...items].sort((left, right) => left.key.localeCompare(right.key, 'en'));
}

function makeId(registry, role, key) {
  const safeRole = String(role).toLowerCase().replace(/[^a-z0-9_.:-]+/g, '-').slice(0, 48).replace(/[-.:]+$/, '');
  const keyDigest = createHash('sha256').update(String(key), 'utf8').digest('hex').slice(0, 24);
  return registry.uuid(`${safeRole}:${keyDigest}`, 0);
}

function requireFoundationScenario(scenario) {
  if (!scenario?.scenarioId || !scenario.foundation) {
    throw new FoundationBuilderError('FOUNDATION_SCENARIO_REQUIRED', 'A validated Weekly Source scenario is required');
  }
  const foundation = scenario.foundation;
  for (const group of ['users', 'candidates', 'clients', 'contracts', 'weeks', 'timesheets']) {
    if (!Array.isArray(foundation[group])) {
      throw new FoundationBuilderError('FOUNDATION_GROUP_REQUIRED', `Foundation group ${group} is required`);
    }
  }
  return foundation;
}

export function buildFoundationRecordPlan(scenario) {
  const foundation = requireFoundationScenario(scenario);
  const registry = new DeterministicIdentityRegistry(scenario.scenarioId);
  const users = uniqueByKey(foundation.users, 'users');
  const candidates = uniqueByKey(foundation.candidates, 'candidates');
  const clients = uniqueByKey(foundation.clients, 'clients');
  const contracts = uniqueByKey(foundation.contracts, 'contracts');
  const weeks = uniqueByKey(foundation.weeks, 'weeks');
  const timesheets = uniqueByKey(foundation.timesheets, 'timesheets');
  const agencyId = makeId(registry, 'agency', foundation.agency.key);
  const globalSettingsId = makeId(registry, 'agency-settings', foundation.agency.key);

  for (const candidate of candidates.values()) {
    if (candidate.payMethod === 'UMBRELLA' && !candidate.umbrellaKey) {
      throw new FoundationBuilderError('FOUNDATION_UMBRELLA_REQUIRED', `Umbrella Candidate ${candidate.key} has no provider prerequisite`);
    }
    if (candidate.payMethod === 'PAYE' && candidate.umbrellaKey) {
      throw new FoundationBuilderError('FOUNDATION_UMBRELLA_UNEXPECTED', `PAYE Candidate ${candidate.key} cannot declare an umbrella provider`);
    }
  }
  for (const contract of contracts.values()) {
    if (!candidates.has(contract.candidateKey)) {
      throw new FoundationBuilderError('FOUNDATION_CANDIDATE_DEPENDENCY_MISSING', `Contract ${contract.key} references missing Candidate ${contract.candidateKey}`);
    }
    if (!clients.has(contract.clientKey)) {
      throw new FoundationBuilderError('FOUNDATION_CLIENT_DEPENDENCY_MISSING', `Contract ${contract.key} references missing Client ${contract.clientKey}`);
    }
  }
  for (const week of weeks.values()) {
    if (!contracts.has(week.contractKey)) {
      throw new FoundationBuilderError('FOUNDATION_CONTRACT_DEPENDENCY_MISSING', `Week ${week.key} references missing Contract ${week.contractKey}`);
    }
  }
  for (const timesheet of timesheets.values()) {
    if (!weeks.has(timesheet.weekKey)) {
      throw new FoundationBuilderError('FOUNDATION_WEEK_DEPENDENCY_MISSING', `Timesheet ${timesheet.key} references missing Week ${timesheet.weekKey}`);
    }
  }

  const providerKeys = [...new Set([...candidates.values()].map((item) => item.umbrellaKey).filter(Boolean))].sort();
  const umbrellaProviders = providerKeys.map((key) => ({
    recordKind: 'UMBRELLA_PROVIDER_PREREQUISITE',
    key,
    id: makeId(registry, 'umbrella-provider', key),
    agencyId
  }));
  const agencyRecords = [{
    recordKind: 'AGENCY_AND_GLOBAL_SETTINGS',
    key: foundation.agency.key,
    id: agencyId,
    settingsId: globalSettingsId,
    globalSettings: cloneJson(foundation.agency.globalSettings)
  }];
  const userRecords = sorted(users.values()).map((item) => ({
    recordKind: 'PERMITTED_TEST_USER',
    key: item.key,
    id: makeId(registry, 'user', item.key),
    agencyId,
    role: item.role,
    active: item.active
  }));
  const candidateRecords = sorted(candidates.values()).map((item) => ({
    recordKind: 'CANDIDATE_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'candidate', item.key),
    agencyId,
    displayName: item.displayName,
    tmsRef: item.tmsRef,
    payMethod: item.payMethod,
    umbrellaProviderId: item.umbrellaKey ? makeId(registry, 'umbrella-provider', item.umbrellaKey) : null,
    active: item.active
  }));
  const clientRecords = sorted(clients.values()).map((item) => ({
    recordKind: 'CLIENT_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'client', item.key),
    agencyId,
    name: item.name,
    sourceAuthority: item.sourceAuthority,
    managerEmail: item.managerEmail ?? null
  }));
  const clientSettingRecords = sorted(clients.values()).map((item) => ({
    recordKind: 'CLIENT_EFFECTIVE_SETTINGS_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'client-settings', item.key),
    clientId: makeId(registry, 'client', item.key),
    settings: cloneJson(item.settings)
  }));

  const sourceAliasRecords = [];
  const aliasOwners = new Set();
  for (const upload of sorted(scenario.sourceUploads || [])) {
    if (!clients.has(upload.clientKey)) {
      throw new FoundationBuilderError('FOUNDATION_UPLOAD_CLIENT_MISSING', `Upload ${upload.key} references missing Client ${upload.clientKey}`);
    }
    if (upload.trustName) {
      const key = `${upload.clientKey}:${upload.trustName}`;
      if (!aliasOwners.has(`client:${key}`)) {
        aliasOwners.add(`client:${key}`);
        sourceAliasRecords.push({
          recordKind: 'CLIENT_SOURCE_ALIAS_PREREQUISITE',
          key,
          id: makeId(registry, 'client-source-alias', key),
          clientId: makeId(registry, 'client', upload.clientKey),
          sourceValue: upload.trustName
        });
      }
    }
    for (const row of upload.physicalRows) {
      const candidate = candidates.get(row.candidateKey);
      if (!candidate) {
        throw new FoundationBuilderError('FOUNDATION_UPLOAD_CANDIDATE_MISSING', `Upload row ${row.key} references missing Candidate ${row.candidateKey}`);
      }
      const key = `${upload.profile}:${candidate.key}`;
      if (!aliasOwners.has(`candidate:${key}`)) {
        aliasOwners.add(`candidate:${key}`);
        sourceAliasRecords.push({
          recordKind: 'CANDIDATE_SOURCE_ALIAS_PREREQUISITE',
          key,
          id: makeId(registry, 'candidate-source-alias', key),
          candidateId: makeId(registry, 'candidate', candidate.key),
          sourceValue: candidate.tmsRef,
          sourceProfile: upload.profile
        });
      }
      if (row.contractKey) {
        const contract = contracts.get(row.contractKey);
        if (!contract || contract.candidateKey !== row.candidateKey || contract.clientKey !== upload.clientKey) {
          throw new FoundationBuilderError('FOUNDATION_CROSS_AGENCY_OR_OWNER_LINK', `Source row ${row.key} does not belong to its declared Candidate and Client Contract`);
        }
      }
    }
  }
  sourceAliasRecords.sort((left, right) => left.key.localeCompare(right.key, 'en'));

  const contractRecords = sorted(contracts.values()).map((item) => ({
    recordKind: 'CONTRACT_AND_RATE_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'contract', item.key),
    candidateId: makeId(registry, 'candidate', item.candidateKey),
    clientId: makeId(registry, 'client', item.clientKey),
    startDate: item.startDate,
    endDate: item.endDate,
    role: item.role ?? null,
    band: item.band,
    payMethod: item.payMethod,
    rates: cloneJson(item.rates),
    additionalUnits: cloneJson(item.additionalUnits || [])
  }));
  const weekRecords = sorted(weeks.values()).map((item) => ({
    recordKind: 'CONTRACT_WEEK_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'contract-week', item.key),
    contractId: makeId(registry, 'contract', item.contractKey),
    weekEndingDate: item.weekEndingDate,
    additionalSequence: item.additionalSequence,
    status: item.status
  }));
  const timesheetRecords = sorted(timesheets.values()).map((item) => ({
    recordKind: 'CANDIDATE_TIMESHEET_EVIDENCE_PREREQUISITE',
    key: item.key,
    id: makeId(registry, 'timesheet', item.key),
    weekId: makeId(registry, 'contract-week', item.weekKey),
    submissionState: item.submissionState,
    candidateSigned: item.candidateSigned,
    managerSigned: item.managerSigned,
    shifts: item.shifts.map((shift) => ({
      ...cloneJson(shift),
      id: makeId(registry, `timesheet-shift:${item.key}`, shift.key)
    })),
    expenses: cloneJson(item.expenses),
    evidence: Array.from({ length: item.expenses.evidenceCount }, (_, ordinal) => ({
      id: registry.uuid(`expense-evidence:${item.key}`, ordinal),
      ordinal
    }))
  }));
  const preExistingStateRecords = [];
  const deviceRecords = sorted(candidates.values()).flatMap((candidate) =>
    Array.from({ length: candidate.activeDeviceCount || 0 }, (_, ordinal) => ({
      recordKind: 'CANDIDATE_DEVICE_PREREQUISITE',
      key: `${candidate.key}:${ordinal}`,
      id: registry.uuid(`candidate-device:${candidate.key}`, ordinal),
      candidateId: makeId(registry, 'candidate', candidate.key),
      active: true
    }))
  );
  const communicationPrerequisiteRecords = sorted(clients.values())
    .filter((client) => client.managerEmail)
    .map((client) => ({
      recordKind: 'MANAGER_COMMUNICATION_PREREQUISITE',
      key: client.key,
      id: makeId(registry, 'manager-communication', client.key),
      clientId: makeId(registry, 'client', client.key),
      managerEmail: client.managerEmail
    }));

  const stages = [
    { sequence: 1, name: 'AGENCY_AND_USER', records: [...agencyRecords, ...userRecords] },
    { sequence: 2, name: 'UMBRELLA_PROVIDER', records: umbrellaProviders },
    { sequence: 3, name: 'CANDIDATE', records: candidateRecords },
    { sequence: 4, name: 'CLIENT', records: clientRecords },
    { sequence: 5, name: 'CLIENT_SETTINGS', records: clientSettingRecords },
    { sequence: 6, name: 'SOURCE_ALIASES', records: sourceAliasRecords },
    { sequence: 7, name: 'CONTRACT_AND_RATES', records: contractRecords },
    { sequence: 8, name: 'CONTRACT_WEEK', records: weekRecords },
    { sequence: 9, name: 'TIMESHEET_AND_EVIDENCE', records: timesheetRecords },
    { sequence: 10, name: 'DECLARED_PRE_EXISTING_STATE', records: preExistingStateRecords },
    { sequence: 11, name: 'DEVICES_AND_COMMUNICATION', records: [...deviceRecords, ...communicationPrerequisiteRecords] }
  ];
  const planWithoutDigest = {
    planVersion: 'WEEKLY_SOURCE_FOUNDATION_RECORD_PLAN_V1',
    mode: 'NON_DATABASE_INPUT_PLAN',
    scenarioId: scenario.scenarioId,
    stages
  };
  return deepFreeze({ ...planWithoutDigest, planDigest: canonicalDigest(planWithoutDigest) });
}
