import { lstat, readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { canonicalDigest, cloneJson, deepFreeze } from './canonical-json.mjs';
import { validateJsonSchema } from './json-schema-validator.mjs';

const DEFAULT_SCHEMA_PATH = fileURLToPath(new URL('./scenario-schema.json', import.meta.url));
const SCENARIO_ID_PATTERN = /^WS-[A-Z0-9][A-Z0-9_-]{2,79}$/;

export class ScenarioContractError extends Error {
  constructor(code, message, details = []) {
    super(message);
    this.name = 'ScenarioContractError';
    this.code = code;
    this.details = deepFreeze(cloneJson(details));
  }
}

function assertNoProhibitedContent(value, location = '$') {
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertNoProhibitedContent(item, `${location}[${index}]`));
    return;
  }
  if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (/password|passphrase|secret|bearer|api[_-]?key|connection[_-]?string|database[_-]?url/i.test(key)) {
        throw new ScenarioContractError('SCENARIO_PROHIBITED_FIELD', `${location}.${key} is a prohibited credential or connection field`);
      }
      assertNoProhibitedContent(item, `${location}.${key}`);
    }
    return;
  }
  if (typeof value !== 'string') return;
  if (/\b(?:postgres|postgresql):\/\//i.test(value)) {
    throw new ScenarioContractError('SCENARIO_DATABASE_URL_FORBIDDEN', `${location} contains a database connection URL`);
  }
  if (/\bBearer\s+[A-Za-z0-9._~+\/-]{12,}/i.test(value)) {
    throw new ScenarioContractError('SCENARIO_CREDENTIAL_FORBIDDEN', `${location} contains bearer credential material`);
  }
  if (/https?:\/\//i.test(value)) {
    throw new ScenarioContractError('SCENARIO_ENDPOINT_FORBIDDEN', `${location} contains an HTTP endpoint`);
  }
  if (/^\s*(?:select\b[\s\S]*\bfrom\b|insert\s+into\b|update\b[\s\S]*\bset\b|delete\s+from\b|create\s+(?:table|function|schema|view)\b|alter\s+(?:table|function|schema|view)\b|drop\s+(?:table|function|schema|view)\b)/i.test(value)) {
    throw new ScenarioContractError('SCENARIO_SQL_FORBIDDEN', `${location} contains SQL rather than scenario facts`);
  }
}

async function readJson(filePath, code) {
  let text;
  try {
    text = await readFile(filePath, 'utf8');
  } catch (error) {
    throw new ScenarioContractError(code, `Unable to read ${path.basename(filePath)}`, [{ message: error.code || error.name }]);
  }
  try {
    return JSON.parse(text);
  } catch (error) {
    throw new ScenarioContractError(code, `${path.basename(filePath)} is not valid JSON`, [{ message: error.message }]);
  }
}

export async function loadScenarioSchema(schemaPath = DEFAULT_SCHEMA_PATH) {
  const schema = await readJson(schemaPath, 'SCENARIO_SCHEMA_READ_FAILED');
  if (schema.$schema !== 'https://json-schema.org/draft/2020-12/schema') {
    throw new ScenarioContractError('SCENARIO_SCHEMA_VERSION_UNSUPPORTED', 'The checked-in scenario schema is not JSON Schema 2020-12');
  }
  return deepFreeze(schema);
}

export function validateScenarioObject(scenario, schema) {
  assertNoProhibitedContent(scenario);
  const validation = validateJsonSchema(scenario, schema);
  if (!validation.valid) {
    throw new ScenarioContractError(
      'SCENARIO_SCHEMA_INVALID',
      `Scenario ${String(scenario?.scenarioId || '<unknown>')} does not satisfy the Weekly Source scenario contract`,
      validation.errors
    );
  }
  return scenario;
}

export async function loadScenarioFile(filePath, { schema } = {}) {
  const absolutePath = path.resolve(filePath);
  if (path.extname(absolutePath).toLowerCase() !== '.json') {
    throw new ScenarioContractError('SCENARIO_FILE_TYPE_INVALID', 'Scenario files must use the .json extension');
  }
  const stat = await lstat(absolutePath).catch(() => null);
  if (!stat?.isFile() || stat.isSymbolicLink()) {
    throw new ScenarioContractError('SCENARIO_FILE_INVALID', `${path.basename(absolutePath)} must be a regular, non-symbolic-link file`);
  }
  const activeSchema = schema || await loadScenarioSchema();
  const parsed = await readJson(absolutePath, 'SCENARIO_READ_FAILED');
  validateScenarioObject(parsed, activeSchema);
  return deepFreeze(parsed);
}

export async function loadScenarioFiles(filePaths, { schemaPath = DEFAULT_SCHEMA_PATH } = {}) {
  if (!Array.isArray(filePaths)) throw new TypeError('filePaths must be an array');
  if (filePaths.length === 0) throw new ScenarioContractError('SCENARIO_SET_EMPTY', 'At least one Weekly Source scenario file is required');
  const schema = await loadScenarioSchema(schemaPath);
  const absolutePaths = [...filePaths].map((item) => path.resolve(item)).sort((a, b) => a.localeCompare(b, 'en'));
  const scenarios = [];
  const owners = new Map();
  for (const filePath of absolutePaths) {
    const scenario = await loadScenarioFile(filePath, { schema });
    if (!SCENARIO_ID_PATTERN.test(scenario.scenarioId)) {
      throw new ScenarioContractError('SCENARIO_ID_INVALID', `${scenario.scenarioId} is not a valid immutable scenario ID`);
    }
    if (owners.has(scenario.scenarioId)) {
      throw new ScenarioContractError(
        'SCENARIO_ID_DUPLICATE',
        `Scenario ID ${scenario.scenarioId} is declared by more than one file`,
        [{ firstFile: path.basename(owners.get(scenario.scenarioId)), secondFile: path.basename(filePath) }]
      );
    }
    owners.set(scenario.scenarioId, filePath);
    scenarios.push(scenario);
  }
  scenarios.sort((left, right) => left.scenarioId.localeCompare(right.scenarioId, 'en'));
  const digest = canonicalDigest(scenarios);
  return deepFreeze({ scenarios, digest });
}

export async function loadScenariosFromDirectory(directoryPath, options = {}) {
  const absoluteDirectory = path.resolve(directoryPath);
  const stat = await lstat(absoluteDirectory).catch(() => null);
  if (!stat?.isDirectory() || stat.isSymbolicLink()) {
    throw new ScenarioContractError('SCENARIO_DIRECTORY_INVALID', 'Scenario directory must be a regular, non-symbolic-link directory');
  }
  const entries = await readdir(absoluteDirectory, { withFileTypes: true });
  const unexpected = entries.filter((entry) => !entry.isFile() || path.extname(entry.name).toLowerCase() !== '.json');
  if (unexpected.length) {
    throw new ScenarioContractError(
      'SCENARIO_DIRECTORY_CONTENT_INVALID',
      'Scenario directory may contain JSON scenario files only',
      unexpected.map((entry) => ({ name: entry.name }))
    );
  }
  return loadScenarioFiles(entries.map((entry) => path.join(absoluteDirectory, entry.name)), options);
}

export { DEFAULT_SCHEMA_PATH };
