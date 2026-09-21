import { canonicalJson } from './canonical-json.mjs';

function valueType(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (Number.isInteger(value)) return 'integer';
  return typeof value;
}

function typeMatches(value, expected) {
  if (expected === 'number') return typeof value === 'number' && Number.isFinite(value);
  if (expected === 'integer') return Number.isInteger(value);
  if (expected === 'object') {
    return value !== null && !Array.isArray(value) && typeof value === 'object';
  }
  return valueType(value) === expected;
}

function resolveRef(root, ref) {
  if (typeof ref !== 'string' || !ref.startsWith('#/')) {
    throw new Error(`Only local JSON Schema references are supported: ${String(ref)}`);
  }
  return ref.slice(2).split('/').reduce((cursor, rawPart) => {
    const part = rawPart.replace(/~1/g, '/').replace(/~0/g, '~');
    if (!cursor || !Object.hasOwn(cursor, part)) throw new Error(`Unresolved JSON Schema reference: ${ref}`);
    return cursor[part];
  }, root);
}

function validDate(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return false;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year
    && date.getUTCMonth() === month - 1
    && date.getUTCDate() === day;
}

function validDateTime(value) {
  const match = /^(\d{4}-\d{2}-\d{2})T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(value);
  if (!match || !validDate(match[1])) return false;
  return Number.isFinite(Date.parse(value));
}

function validEmail(value) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value);
}

function validateNode(value, schema, root, path, errors) {
  if (schema.$ref) {
    validateNode(value, resolveRef(root, schema.$ref), root, path, errors);
    return;
  }

  if (schema.oneOf) {
    const branches = schema.oneOf.map((branch) => {
      const branchErrors = [];
      validateNode(value, branch, root, path, branchErrors);
      return branchErrors;
    });
    const matches = branches.filter((branchErrors) => branchErrors.length === 0).length;
    if (matches !== 1) errors.push({ path, keyword: 'oneOf', message: `must match exactly one branch; matched ${matches}` });
    return;
  }

  if (Object.hasOwn(schema, 'const') && canonicalJson(value) !== canonicalJson(schema.const)) {
    errors.push({ path, keyword: 'const', message: `must equal ${JSON.stringify(schema.const)}` });
    return;
  }

  if (schema.enum && !schema.enum.some((candidate) => canonicalJson(candidate) === canonicalJson(value))) {
    errors.push({ path, keyword: 'enum', message: 'contains an unapproved value' });
    return;
  }

  if (schema.type) {
    const expectedTypes = Array.isArray(schema.type) ? schema.type : [schema.type];
    if (!expectedTypes.some((expected) => typeMatches(value, expected))) {
      errors.push({ path, keyword: 'type', message: `must be ${expectedTypes.join(' or ')}` });
      return;
    }
  }

  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) {
      errors.push({ path, keyword: 'minLength', message: `must contain at least ${schema.minLength} characters` });
    }
    if (schema.maxLength !== undefined && value.length > schema.maxLength) {
      errors.push({ path, keyword: 'maxLength', message: `must contain no more than ${schema.maxLength} characters` });
    }
    if (schema.pattern && !new RegExp(schema.pattern, 'u').test(value)) {
      errors.push({ path, keyword: 'pattern', message: `must match ${schema.pattern}` });
    }
    if (schema.format === 'date' && !validDate(value)) {
      errors.push({ path, keyword: 'format', message: 'must be a valid calendar date' });
    }
    if (schema.format === 'date-time' && !validDateTime(value)) {
      errors.push({ path, keyword: 'format', message: 'must be an RFC 3339 date-time with a timezone' });
    }
    if (schema.format === 'email' && !validEmail(value)) {
      errors.push({ path, keyword: 'format', message: 'must be an email address' });
    }
  }

  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) {
      errors.push({ path, keyword: 'minimum', message: `must be at least ${schema.minimum}` });
    }
    if (schema.maximum !== undefined && value > schema.maximum) {
      errors.push({ path, keyword: 'maximum', message: `must be no more than ${schema.maximum}` });
    }
  }

  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) {
      errors.push({ path, keyword: 'minItems', message: `must contain at least ${schema.minItems} items` });
    }
    if (schema.uniqueItems) {
      const seen = new Set();
      value.forEach((item, index) => {
        const key = canonicalJson(item);
        if (seen.has(key)) errors.push({ path: `${path}[${index}]`, keyword: 'uniqueItems', message: 'must be unique' });
        seen.add(key);
      });
    }
    if (schema.items) value.forEach((item, index) => validateNode(item, schema.items, root, `${path}[${index}]`, errors));
  }

  if (value !== null && !Array.isArray(value) && typeof value === 'object') {
    const properties = schema.properties || {};
    for (const required of schema.required || []) {
      if (!Object.hasOwn(value, required)) {
        errors.push({ path: `${path}.${required}`, keyword: 'required', message: 'is required' });
      }
    }
    for (const [key, item] of Object.entries(value)) {
      if (Object.hasOwn(properties, key)) validateNode(item, properties[key], root, `${path}.${key}`, errors);
      else if (schema.additionalProperties === false) {
        errors.push({ path: `${path}.${key}`, keyword: 'additionalProperties', message: 'is not allowed' });
      }
    }
  }
}

export function validateJsonSchema(value, schema) {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) {
    throw new TypeError('A JSON Schema object is required');
  }
  const errors = [];
  validateNode(value, schema, schema, '$', errors);
  return { valid: errors.length === 0, errors };
}
