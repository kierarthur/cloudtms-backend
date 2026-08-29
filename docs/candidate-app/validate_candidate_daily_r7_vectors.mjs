#!/usr/bin/env node
import fs from 'node:fs';
import crypto from 'node:crypto';

const data = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const key = Buffer.from(data.key_ascii, 'ascii');
const unreserved = /^[A-Za-z0-9._~-]$/u;
const headerName = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/u;

function decodeComponent(raw, encodingError, rejectSeparators = false) {
  const bytes = [];
  for (let index = 0; index < raw.length;) {
    if (raw[index] === '%') {
      if (index + 2 >= raw.length || !/^[0-9A-Fa-f]{2}$/u.test(raw.slice(index + 1, index + 3))) throw new Error(encodingError);
      const byte = Number.parseInt(raw.slice(index + 1, index + 3), 16);
      if (rejectSeparators && [0x2f, 0x5c, 0x3f, 0x23].includes(byte)) throw new Error('PATH_SEPARATOR_ENCODED');
      bytes.push(byte); index += 3; continue;
    }
    bytes.push(...Buffer.from(raw[index], 'utf8')); index += 1;
  }
  const buffer = Buffer.from(bytes);
  let text;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(buffer); } catch { throw new Error(encodingError); }
  return { buffer, text };
}

function rejectControls(text, error) {
  for (const char of text) {
    const code = char.codePointAt(0);
    if (code < 0x20 || code === 0x7f) throw new Error(error);
  }
}

function percentEncode(buffer) {
  let output = '';
  for (const byte of buffer) {
    const char = String.fromCharCode(byte);
    output += unreserved.test(char) ? char : `%${byte.toString(16).toUpperCase().padStart(2, '0')}`;
  }
  return output;
}

function asciiCompare(left, right) {
  const maximum = Math.min(left.length, right.length);
  for (let index = 0; index < maximum; index += 1) {
    const difference = left.charCodeAt(index) - right.charCodeAt(index);
    if (difference) return difference;
  }
  return left.length - right.length;
}

function normalizeQuery(raw) {
  if (raw === '') return '';
  const pairs = [];
  for (const item of raw.split('&')) {
    if (!item.includes('=') || item.includes('+') || item.includes(';')) throw new Error('QUERY_INVALID');
    const at = item.indexOf('='); const nameRaw = item.slice(0, at), valueRaw = item.slice(at + 1);
    if (!nameRaw) throw new Error('QUERY_INVALID');
    const name = decodeComponent(nameRaw, 'QUERY_ENCODING_INVALID');
    const value = decodeComponent(valueRaw, 'QUERY_ENCODING_INVALID');
    rejectControls(name.text, 'QUERY_CONTROL_INVALID'); rejectControls(value.text, 'QUERY_CONTROL_INVALID');
    pairs.push([percentEncode(name.buffer), percentEncode(value.buffer)]);
  }
  pairs.sort((a, b) => asciiCompare(a[0], b[0]) || asciiCompare(a[1], b[1]));
  return pairs.map(([name, value]) => `${name}=${value}`).join('&');
}

function parseRawTarget(rawTarget) {
  if (!rawTarget.startsWith('/') || rawTarget.includes('#') || rawTarget.includes('\\')) throw new Error('PATH_NORMALIZATION_INVALID');
  if ((rawTarget.match(/\?/gu) ?? []).length > 1) throw new Error('PATH_NORMALIZATION_INVALID');
  const at = rawTarget.indexOf('?'); const rawPath = at < 0 ? rawTarget : rawTarget.slice(0, at); const rawQuery = at < 0 ? '' : rawTarget.slice(at + 1);
  if (rawPath.includes('//')) throw new Error('PATH_NORMALIZATION_INVALID');
  const segments = rawPath.split('/').map((segment) => {
    const decoded = decodeComponent(segment, 'PATH_ENCODING_INVALID', true);
    rejectControls(decoded.text, 'PATH_CONTROL_INVALID');
    if (decoded.text === '.' || decoded.text === '..') throw new Error('PATH_NORMALIZATION_INVALID');
    return percentEncode(decoded.buffer);
  });
  const normalizedPath = segments.join('/');
  if (!normalizedPath.startsWith('/')) throw new Error('PATH_NORMALIZATION_INVALID');
  return [normalizedPath, at < 0 ? '' : normalizeQuery(rawQuery)];
}

function validateHeaders(headers) {
  const seen = new Map();
  for (const pair of headers) {
    if (!Array.isArray(pair) || pair.length !== 2) throw new Error('HEADER_FORMAT_INVALID');
    const [name, value] = pair;
    if (!headerName.test(name) || value !== value.trim()) throw new Error('HEADER_FORMAT_INVALID');
    rejectControls(value, 'HEADER_FORMAT_INVALID');
    const lower = name.toLowerCase();
    if (seen.has(lower)) throw new Error('AMBIGUOUS_HEADER');
    seen.set(lower, value);
  }
  if (seen.has('transfer-encoding')) throw new Error('TRANSFER_AMBIGUOUS');
  if (!seen.has('content-length') || !/^(?:0|[1-9][0-9]*)$/u.test(seen.get('content-length'))) throw new Error('HEADER_FORMAT_INVALID');
}

function prefix(vector, bodyHash) {
  return Buffer.from(
    `CLOUDTMS-HMAC-V1\n${vector.method}\n${vector.normalized_path}\n${vector.normalized_query}\n` +
    `${vector.timestamp}\n${vector.nonce}\n${bodyHash}\n${vector.idempotency_key}\n${vector.correlation_id}\n\n`,
    'ascii',
  );
}

function sign(vector) {
  const body = Buffer.from(vector.body, 'utf8'); const bodyHash = crypto.createHash('sha256').update(body).digest('hex');
  const canonicalPrefix = prefix(vector, bodyHash); const message = Buffer.concat([canonicalPrefix, body]);
  return [
    bodyHash, canonicalPrefix.toString('base64'), crypto.createHash('sha256').update(message).digest('hex'),
    crypto.createHmac('sha256', key).update(message).digest('hex'),
  ];
}

function applyMutation(base, mutation) {
  const vector = structuredClone(base); let presentedHash = base.body_sha256, signature = base.signature_hex;
  if ('body' in mutation) vector.body = mutation.body;
  if ('body_append' in mutation) vector.body += mutation.body_append;
  if ('body_replace' in mutation) vector.body = vector.body.replace(...mutation.body_replace);
  const immediate = new Map([
    ['body_prefix_bom', 'BODY_ENCODING_INVALID'], ['transfer_ambiguity', 'TRANSFER_AMBIGUOUS'],
    ['duplicate_header', 'AMBIGUOUS_HEADER'], ['ambiguous_header_casing', 'AMBIGUOUS_HEADER'],
    ['path_invalid', 'PATH_NORMALIZATION_INVALID'], ['header_outer_whitespace', 'HEADER_FORMAT_INVALID'],
  ]);
  for (const [field, error] of immediate) if (mutation[field]) return [vector, signature, error];
  for (const field of ['timestamp', 'correlation_id', 'idempotency_key', 'method', 'normalized_path', 'key_id']) if (field in mutation) vector[field] = mutation[field];
  if ('raw_query' in mutation) vector.normalized_query = normalizeQuery(mutation.raw_query);
  if (mutation.resign) [presentedHash,,, signature] = sign(vector);
  if ('content_hash' in mutation) presentedHash = mutation.content_hash;
  if ('signature_hex' in mutation) signature = mutation.signature_hex;
  vector.presented_hash = presentedHash; return [vector, signature, null];
}

function verify(vector, signature, seen, rejectQuery = false) {
  if (vector.key_id !== data.active_key_id) return 'KEY_VERSION_MISMATCH';
  if (Math.abs(Number(vector.timestamp) - data.verification_epoch) > 300) return 'TIMESTAMP_OUTSIDE_WINDOW';
  const body = Buffer.from(vector.body, 'utf8'); const actualHash = crypto.createHash('sha256').update(body).digest('hex');
  if (actualHash !== (vector.presented_hash ?? vector.body_sha256)) return 'CONTENT_HASH_MISMATCH';
  const expected = crypto.createHmac('sha256', key).update(Buffer.concat([prefix(vector, actualHash), body])).digest('hex');
  if (expected !== signature) return 'SIGNATURE_MISMATCH';
  if (seen.has(vector.nonce)) return 'NONCE_REPLAY'; seen.add(vector.nonce);
  if (rejectQuery && vector.normalized_query) return 'UNEXPECTED_QUERY';
  return 'ACCEPTED';
}

const positives = new Map(data.positive_vectors.map((vector) => [vector.id, vector]));
for (const vector of positives.values()) {
  const actual = sign(vector); const expected = [vector.body_sha256, vector.canonical_prefix_base64, vector.signed_message_sha256, vector.signature_hex];
  if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error(vector.id);
}
for (const test of data.query_canonicalization_cases) if (normalizeQuery(test.raw_query) !== test.normalized_query) throw new Error(test.id);
for (const test of data.raw_parser_cases) {
  let actualError = null, parsed = null;
  try { validateHeaders(test.headers); parsed = parseRawTarget(test.raw_target); } catch (error) { actualError = error.message; }
  if (actualError !== (test.expected_error ?? null)) throw new Error(`${test.id}: ${actualError}`);
  if (!actualError && (parsed[0] !== test.expected_path || parsed[1] !== test.expected_query)) throw new Error(test.id);
}
for (const test of data.negative_vectors) {
  const [vector, signature, immediate] = applyMutation(positives.get(test.base_id), test.mutation); let actual = immediate;
  if (!actual) {
    const seen = new Set(); actual = verify(vector, signature, seen, 'raw_query' in test.mutation);
    if (test.mutation.verify_twice && actual === 'ACCEPTED') actual = verify(vector, signature, seen);
  }
  if (actual !== test.expected) throw new Error(`${test.id}: ${actual}`);
}
const routeValid = [...positives.values()].filter((vector) => vector.route_schema_valid).length;
console.log(`HMAC_R7_NODE_VECTOR_PASS|positive=${positives.size}|route_valid=${routeValid}|negative=${data.negative_vectors.length}|query=${data.query_canonicalization_cases.length}|raw_parser=${data.raw_parser_cases.length}|version=${data.version}`);
