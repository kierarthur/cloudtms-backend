import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { sqlDateKey } from '../scripts/cloudtms-db-release-lib.mjs';

const POSTGRES_FUNCTION_ARGUMENT_LIMIT = 100;
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const SKIPPED_DIRECTORIES = new Set([
  '.codex-tmp',
  '.git',
  '.wrangler',
  'node_modules',
]);

const HISTORICAL_OVERSIZED_EXCEPTION = Object.freeze({
  path: 'supabase/baseline/22082026_1503_cloudtms_test_routines_10.sql',
  line: 6793,
  argumentCount: 132,
  callSha256: '67f3320c7bce19353ce3699eedd00e5ae7b6409c5af95a4550f36decd05d8291',
  owner: 'public.pay_no_money_unwind_apply_work_item',
  supersededBy:
    'supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql',
});

const EXPECTED_CEILING_OCCURRENCES = Object.freeze([
  'supabase/baseline/22082026_1503_cloudtms_test_routines_04.sql:7841',
  'supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql:597',
  'supabase/repeatable/07082026_2224_candidate_app_weekly_office_replacements_v1.sql:3051',
  'supabase/repeatable/13062026_1544_process_authorise_unprocess_unauthorise.sql:3046',
  'supabase/repeatable/29082026_0326_banking_pay_release_authority_repair_v1.sql:1550',
].sort());

const FINAL_CEILING_OWNERS = Object.freeze([
  Object.freeze({
    owner: 'private.pay_workbench_settled_certificate_constituent_seed_v8',
    path:
      'supabase/repeatable/02092026_2301_banking_pay_workbench_settled_certificate_build_v8.sql',
    line: 597,
    callSha256: 'ebc188732afa6694ab65c53b58853f2f3adb26e50bedc08af0ca00a0e273df8c',
    warning: 'Workbench settled-certificate seed (Draft-readiness path) has zero argument headroom',
  }),
  Object.freeze({
    owner: 'public.bulk_process_dataset_v1',
    path: 'supabase/repeatable/29082026_0326_banking_pay_release_authority_repair_v1.sql',
    line: 1550,
    callSha256: 'dcc62647fa73a9b940cedae8654900ba3420cac20386bc4eba28e3b5d8bbb783',
    warning: 'bulk-process result projection has zero argument headroom',
  }),
]);

const FINAL_NO_MONEY_OWNER = Object.freeze({
  owner: 'public.pay_no_money_unwind_apply_work_item',
  path:
    'supabase/repeatable/07092026_1932_banking_pay_unpaid_cancellation_sourceless_apply_v1.sql',
});

function canonicalSql(source) {
  return source.replace(/\r\n?/g, '\n');
}

function sha256(source) {
  return crypto.createHash('sha256').update(source).digest('hex');
}

function relativeSqlPath(absolutePath) {
  return path.relative(REPO_ROOT, absolutePath).replaceAll('\\', '/');
}

function sqlFilesUnder(directory, result = []) {
  const entries = fs.readdirSync(directory, { withFileTypes: true })
    .sort((left, right) => left.name.localeCompare(right.name));

  for (const entry of entries) {
    if (entry.isDirectory() && SKIPPED_DIRECTORIES.has(entry.name)) continue;
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) sqlFilesUnder(absolutePath, result);
    else if (entry.isFile() && entry.name.toLowerCase().endsWith('.sql')) result.push(absolutePath);
  }
  return result;
}

function dollarTagAt(source, index, end) {
  if (source[index] !== '$') return null;
  const match = /^(?:\$\$|\$[A-Za-z_][A-Za-z_0-9]*\$)/u.exec(
    source.slice(index, Math.min(end, index + 132)),
  );
  return match?.[0] ?? null;
}

function usesBackslashEscapes(source, quoteIndex) {
  const escapeString = quoteIndex >= 1
    && /[eE]/u.test(source[quoteIndex - 1])
    && (quoteIndex < 2 || !/[A-Za-z_0-9$]/u.test(source[quoteIndex - 2]));
  const unicodeString = quoteIndex >= 2
    && /[uU]/u.test(source[quoteIndex - 2])
    && source[quoteIndex - 1] === '&'
    && (quoteIndex < 3 || !/[A-Za-z_0-9$]/u.test(source[quoteIndex - 3]));
  return escapeString || unicodeString;
}

function readBackslashEscape(source, index, end) {
  const escaped = source[index + 1];
  if (escaped === undefined || index + 1 >= end) return { end, value: '' };
  const simpleEscapes = {
    b: '\b',
    f: '\f',
    n: '\n',
    r: '\r',
    t: '\t',
  };
  if (Object.hasOwn(simpleEscapes, escaped)) {
    return { end: index + 2, value: simpleEscapes[escaped] };
  }
  if (/[0-7]/u.test(escaped)) {
    let escapeEnd = index + 2;
    while (escapeEnd < end && escapeEnd < index + 4 && /[0-7]/u.test(source[escapeEnd])) {
      escapeEnd += 1;
    }
    return {
      end: escapeEnd,
      value: String.fromCodePoint(Number.parseInt(source.slice(index + 1, escapeEnd), 8)),
    };
  }
  if (escaped === 'x') {
    const match = /^[0-9A-Fa-f]{1,2}/u.exec(source.slice(index + 2, end));
    if (match) {
      return { end: index + 2 + match[0].length, value: String.fromCodePoint(Number.parseInt(match[0], 16)) };
    }
  }
  if (escaped === 'u' || escaped === 'U') {
    const width = escaped === 'u' ? 4 : 8;
    const digits = source.slice(index + 2, index + 2 + width);
    if (digits.length === width && /^[0-9A-Fa-f]+$/u.test(digits)) {
      const codePoint = Number.parseInt(digits, 16);
      if (codePoint <= 0x10ffff) {
        return { end: index + 2 + width, value: String.fromCodePoint(codePoint) };
      }
    }
  }
  return { end: index + 2, value: escaped };
}

function readSingleQuoted(source, index, end) {
  const backslashEscapes = usesBackslashEscapes(source, index);
  const sourceOffsets = [];
  let value = '';
  index += 1;
  while (index < end) {
    if (source[index] === "'") {
      if (source[index + 1] === "'") {
        value += "'";
        sourceOffsets.push(index);
        index += 2;
        continue;
      }
      return {
        contentEnd: index,
        end: index + 1,
        sourceOffsets,
        terminated: true,
        value,
      };
    }
    if (backslashEscapes && source[index] === '\\' && index + 1 < end) {
      const escapeStart = index;
      const escape = readBackslashEscape(source, index, end);
      value += escape.value;
      sourceOffsets.push(...Array.from({ length: escape.value.length }, () => escapeStart));
      index = escape.end;
      continue;
    }
    value += source[index];
    sourceOffsets.push(index);
    index += 1;
  }
  return { contentEnd: end, end, sourceOffsets, terminated: false, value };
}

function skipSingleQuoted(source, index, end) {
  return readSingleQuoted(source, index, end).end;
}

function readDoubleQuotedIdentifier(source, index, end) {
  let value = '';
  index += 1;
  while (index < end) {
    if (source[index] === '"') {
      if (source[index + 1] === '"') {
        value += '"';
        index += 2;
        continue;
      }
      return { end: index + 1, value };
    }
    value += source[index];
    index += 1;
  }
  return { end, value };
}

function skipLineComment(source, index, end) {
  const newline = source.indexOf('\n', index + 2);
  return newline < 0 || newline >= end ? end : newline + 1;
}

function skipBlockComment(source, index, end) {
  let depth = 1;
  index += 2;
  while (index < end && depth > 0) {
    if (source[index] === '/' && source[index + 1] === '*') {
      depth += 1;
      index += 2;
    } else if (source[index] === '*' && source[index + 1] === '/') {
      depth -= 1;
      index += 2;
    } else {
      index += 1;
    }
  }
  return index;
}

function skipTrivia(source, index, end) {
  for (;;) {
    const before = index;
    while (index < end && /\s/u.test(source[index])) index += 1;
    if (source[index] === '-' && source[index + 1] === '-') {
      index = skipLineComment(source, index, end);
    } else if (source[index] === '/' && source[index + 1] === '*') {
      index = skipBlockComment(source, index, end);
    }
    if (index === before) return index;
  }
}

function bareIdentifierAt(source, index, end) {
  if (index >= end || !/[A-Za-z_]/u.test(source[index])) return null;
  let identifierEnd = index + 1;
  while (identifierEnd < end && /[A-Za-z_0-9$]/u.test(source[identifierEnd])) {
    identifierEnd += 1;
  }
  return { end: identifierEnd, value: source.slice(index, identifierEnd) };
}

function dollarQuotedAt(source, index, end) {
  const tag = dollarTagAt(source, index, end);
  if (!tag) return null;
  const bodyStart = index + tag.length;
  const bodyEnd = source.indexOf(tag, bodyStart);
  const terminated = bodyEnd >= 0 && bodyEnd + tag.length <= end;
  return {
    bodyEnd: terminated ? bodyEnd : end,
    bodyStart,
    end: terminated ? bodyEnd + tag.length : end,
    quoteIndex: index,
    tag,
    terminated,
  };
}

function keywordAfterTrivia(source, index, end) {
  const start = skipTrivia(source, index, end);
  const identifier = bareIdentifierAt(source, start, end);
  return identifier ? { ...identifier, start } : null;
}

function stringLiteralAfter(source, index, end) {
  let quoteIndex = skipTrivia(source, index, end);
  const prefix = bareIdentifierAt(source, quoteIndex, end);
  if (prefix?.value.toLowerCase() === 'e' && source[prefix.end] === "'") {
    quoteIndex = prefix.end;
  }
  if (source[quoteIndex] === "'") {
    return { ...readSingleQuoted(source, quoteIndex, end), kind: 'single', quoteIndex };
  }
  const dollarQuoted = dollarQuotedAt(source, quoteIndex, end);
  return dollarQuoted ? { ...dollarQuoted, kind: 'dollar' } : null;
}

function executableBodyAt(source, declarationStart, declarationKeywordEnd, end) {
  const declarationKeyword = source.slice(declarationStart, declarationKeywordEnd).toLowerCase();
  const isDo = declarationKeyword === 'do';
  let index = declarationKeywordEnd;

  if (!isDo) {
    let keyword = keywordAfterTrivia(source, index, end);
    if (keyword?.value.toLowerCase() === 'or') {
      keyword = keywordAfterTrivia(source, keyword.end, end);
      if (keyword?.value.toLowerCase() !== 'replace') return null;
      keyword = keywordAfterTrivia(source, keyword.end, end);
    }
    if (!['function', 'procedure'].includes(keyword?.value.toLowerCase())) return null;
    index = keyword.end;
  }

  let body = null;
  let expectBody = false;
  let expectLanguage = false;
  let language = null;
  let parameterDepth = 0;
  let bracketDepth = 0;
  let sawParameters = isDo;
  let parametersClosed = isDo;

  while (index < end) {
    const current = source[index];
    if (/\s/u.test(current)) {
      index += 1;
      continue;
    }
    if (current === '-' && source[index + 1] === '-') {
      index = skipLineComment(source, index, end);
      continue;
    }
    if (current === '/' && source[index + 1] === '*') {
      index = skipBlockComment(source, index, end);
      continue;
    }

    const atClauseLevel = parametersClosed && parameterDepth === 0 && bracketDepth === 0;
    if (current === "'") {
      const literal = readSingleQuoted(source, index, end);
      if (atClauseLevel && expectLanguage) {
        language = literal.value.trim().toLowerCase();
        expectLanguage = false;
      } else if (atClauseLevel && ((isDo && !body) || expectBody)) {
        body = { ...literal, kind: 'single', quoteIndex: index };
        expectBody = false;
      }
      index = literal.end;
      continue;
    }
    if (current === '$') {
      const quoted = dollarQuotedAt(source, index, end);
      if (quoted) {
        if (atClauseLevel && ((isDo && !body) || expectBody)) {
          body = { ...quoted, kind: 'dollar' };
          expectBody = false;
        }
        index = quoted.end;
        continue;
      }
    }
    if (current === '"') {
      const quoted = readDoubleQuotedIdentifier(source, index, end);
      if (atClauseLevel && expectLanguage) {
        language = quoted.value.toLowerCase();
        expectLanguage = false;
      } else if (atClauseLevel && expectBody) {
        expectBody = false;
      }
      index = quoted.end;
      continue;
    }
    if (current === '(') {
      if (!isDo && parameterDepth === 0 && !sawParameters) sawParameters = true;
      parameterDepth += 1;
      index += 1;
      continue;
    }
    if (current === ')') {
      if (parameterDepth > 0) parameterDepth -= 1;
      if (!isDo && sawParameters && parameterDepth === 0) parametersClosed = true;
      index += 1;
      continue;
    }
    if (current === '[') {
      bracketDepth += 1;
      index += 1;
      continue;
    }
    if (current === ']') {
      if (bracketDepth > 0) bracketDepth -= 1;
      index += 1;
      continue;
    }
    if (current === ';' && atClauseLevel) break;

    const identifier = bareIdentifierAt(source, index, end);
    if (identifier) {
      if (atClauseLevel) {
        const keyword = identifier.value.toLowerCase();
        const escapeBodyPrefix = keyword === 'e'
          && source[identifier.end] === "'"
          && ((isDo && !body) || expectBody);
        if (expectLanguage) {
          if (!isDo && keyword === 'as') {
            expectLanguage = false;
            expectBody = true;
          } else {
            language = keyword;
            expectLanguage = false;
          }
        } else if (keyword === 'language') {
          expectLanguage = true;
        } else if (!isDo && keyword === 'as') {
          expectBody = true;
        } else if (expectBody && !escapeBodyPrefix) {
          expectBody = false;
        }
      }
      index = identifier.end;
      continue;
    }

    if (atClauseLevel && expectBody) expectBody = false;
    index += 1;
  }

  const executableLanguage = isDo
    ? language === null || language === 'plpgsql'
    : language === 'plpgsql' || language === 'sql';
  return executableLanguage ? body : null;
}

function readBalancedCall(source, openParenIndex, end) {
  let parenDepth = 1;
  let bracketDepth = 0;
  let topLevelCommas = 0;
  let sawArgumentToken = false;
  let index = openParenIndex + 1;

  while (index < end) {
    const current = source[index];
    if (/\s/u.test(current)) {
      index += 1;
      continue;
    }
    if (current === '-' && source[index + 1] === '-') {
      index = skipLineComment(source, index, end);
      continue;
    }
    if (current === '/' && source[index + 1] === '*') {
      index = skipBlockComment(source, index, end);
      continue;
    }
    if (current === "'") {
      sawArgumentToken = true;
      index = skipSingleQuoted(source, index, end);
      continue;
    }
    if (current === '"') {
      sawArgumentToken = true;
      index = readDoubleQuotedIdentifier(source, index, end).end;
      continue;
    }
    if (current === '$') {
      const tag = dollarTagAt(source, index, end);
      if (tag) {
        sawArgumentToken = true;
        const closingTag = source.indexOf(tag, index + tag.length);
        index = closingTag < 0 || closingTag >= end ? end : closingTag + tag.length;
        continue;
      }
    }
    if (current === '(') {
      parenDepth += 1;
      sawArgumentToken = true;
      index += 1;
      continue;
    }
    if (current === ')') {
      parenDepth -= 1;
      if (parenDepth === 0) {
        return {
          argumentCount: sawArgumentToken ? topLevelCommas + 1 : 0,
          end: index + 1,
          terminated: true,
        };
      }
      sawArgumentToken = true;
      index += 1;
      continue;
    }
    if (current === '[') {
      bracketDepth += 1;
      sawArgumentToken = true;
      index += 1;
      continue;
    }
    if (current === ']') {
      if (bracketDepth > 0) bracketDepth -= 1;
      sawArgumentToken = true;
      index += 1;
      continue;
    }
    if (current === ',' && parenDepth === 1 && bracketDepth === 0) {
      topLevelCommas += 1;
      index += 1;
      continue;
    }
    sawArgumentToken = true;
    index += 1;
  }

  return { argumentCount: null, end, terminated: false };
}

function lineNumberAt(lineBreaks, offset) {
  let low = 0;
  let high = lineBreaks.length;
  while (low < high) {
    const middle = (low + high) >> 1;
    if (lineBreaks[middle] < offset) low = middle + 1;
    else high = middle;
  }
  return low + 1;
}

function scanSql(source) {
  const hits = [];
  const errors = [];
  const lineBreaks = [];
  for (let index = 0; index < source.length; index += 1) {
    if (source[index] === '\n') lineBreaks.push(index);
  }

  function recordCall(
    segmentSource,
    identifierStart,
    identifierEnd,
    segmentEnd,
    dollarDepth,
    rootOffsetAt,
  ) {
    const openParenIndex = skipTrivia(segmentSource, identifierEnd, segmentEnd);
    if (segmentSource[openParenIndex] !== '(') return;
    const call = readBalancedCall(segmentSource, openParenIndex, segmentEnd);
    const rootStart = rootOffsetAt(identifierStart);
    const rootEnd = call.end > identifierStart
      ? rootOffsetAt(call.end - 1) + 1
      : rootOffsetAt(call.end);
    const hit = {
      start: rootStart,
      end: rootEnd,
      line: lineNumberAt(lineBreaks, rootStart),
      argumentCount: call.argumentCount,
      callSha256: call.terminated
        ? sha256(segmentSource.slice(identifierStart, call.end))
        : null,
      dollarDepth,
      terminated: call.terminated,
    };
    hits.push(hit);
    if (!call.terminated) errors.push(`unterminated jsonb_build_object at line ${hit.line}`);
  }

  function scanSegment(
    segmentSource,
    start,
    end,
    dollarDepth,
    rootOffsetAt = (offset) => offset,
  ) {
    const executableBodies = new Map();
    let index = start;
    while (index < end) {
      const current = segmentSource[index];
      if (current === '-' && segmentSource[index + 1] === '-') {
        index = skipLineComment(segmentSource, index, end);
        continue;
      }
      if (current === '/' && segmentSource[index + 1] === '*') {
        index = skipBlockComment(segmentSource, index, end);
        continue;
      }
      if (current === "'") {
        const literal = readSingleQuoted(segmentSource, index, end);
        const executableBody = executableBodies.get(index);
        if (executableBody) {
          if (!literal.terminated) {
            errors.push(
              `unterminated quoted routine body at line ${lineNumberAt(lineBreaks, rootOffsetAt(index))}`,
            );
          } else {
            const decodedRootOffsetAt = (offset) => rootOffsetAt(
              executableBody.sourceOffsets[offset] ?? executableBody.contentEnd,
            );
            scanSegment(
              executableBody.value,
              0,
              executableBody.value.length,
              dollarDepth + 1,
              decodedRootOffsetAt,
            );
          }
        }
        index = literal.end;
        continue;
      }
      if (current === '"') {
        const quoted = readDoubleQuotedIdentifier(segmentSource, index, end);
        if (quoted.value === 'jsonb_build_object') {
          recordCall(
            segmentSource,
            index,
            quoted.end,
            end,
            dollarDepth,
            rootOffsetAt,
          );
        }
        index = quoted.end;
        continue;
      }
      if (current === '$') {
        const quoted = dollarQuotedAt(segmentSource, index, end);
        if (quoted) {
          if (!quoted.terminated) {
            errors.push(
              `unterminated dollar quote ${quoted.tag} at line ${lineNumberAt(lineBreaks, rootOffsetAt(index))}`,
            );
            return;
          }
          if (executableBodies.has(index)) {
            scanSegment(
              segmentSource,
              quoted.bodyStart,
              quoted.bodyEnd,
              dollarDepth + 1,
              rootOffsetAt,
            );
          }
          index = quoted.end;
          continue;
        }
      }
      const identifier = bareIdentifierAt(segmentSource, index, end);
      if (identifier) {
        const identifierValue = identifier.value.toLowerCase();
        if (identifierValue === 'create' || identifierValue === 'do') {
          const executableBody = executableBodyAt(
            segmentSource,
            index,
            identifier.end,
            end,
          );
          if (executableBody) executableBodies.set(executableBody.quoteIndex, executableBody);
        }
        if (identifierValue === 'execute' && dollarDepth > 0) {
          const dynamicSql = stringLiteralAfter(segmentSource, identifier.end, end);
          if (dynamicSql) executableBodies.set(dynamicSql.quoteIndex, dynamicSql);
        }
        if (identifierValue === 'jsonb_build_object') {
          recordCall(
            segmentSource,
            index,
            identifier.end,
            end,
            dollarDepth,
            rootOffsetAt,
          );
        }
        index = identifier.end;
        continue;
      }
      index += 1;
    }
  }

  scanSegment(source, 0, source.length, 0);
  return { errors, hits };
}

function routineOwnerAt(source, offset) {
  const declaration = /(?:^|\n)[\t ]*CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([A-Za-z_][A-Za-z_0-9$]*\s*\.\s*[A-Za-z_][A-Za-z_0-9$]*)\s*\(/giu;
  let owner = null;
  for (const match of source.matchAll(declaration)) {
    if (match.index >= offset) break;
    owner = match[1].replace(/\s+/gu, '');
  }
  return owner;
}

function definitionPaths(records, owner) {
  const escapedOwner = owner.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace('\\.', '\\s*\\.\\s*');
  const pattern = new RegExp(
    `(?:^|\\n)[\\t ]*CREATE\\s+(?:OR\\s+REPLACE\\s+)?FUNCTION\\s+${escapedOwner}\\s*\\(`,
    'iu',
  );
  return records
    .filter((record) => record.path.startsWith('supabase/') && pattern.test(record.source))
    .map((record) => record.path);
}

function latestRepeatableDefinitionPath(records, owner) {
  const paths = definitionPaths(records, owner)
    .filter((sqlPath) => sqlPath.startsWith('supabase/repeatable/'))
    .sort((left, right) => sqlDateKey(left).localeCompare(sqlDateKey(right)) || left.localeCompare(right));
  return paths.at(-1) ?? null;
}

let cachedRepositoryScan;
function repositoryScan() {
  if (cachedRepositoryScan) return cachedRepositoryScan;
  const records = [];
  const hits = [];
  const errors = [];

  for (const absolutePath of sqlFilesUnder(REPO_ROOT)) {
    const sqlPath = relativeSqlPath(absolutePath);
    const source = canonicalSql(fs.readFileSync(absolutePath, 'utf8'));
    const scanned = scanSql(source);
    const record = { path: sqlPath, source };
    records.push(record);
    hits.push(...scanned.hits.map((hit) => ({ ...hit, path: sqlPath })));
    errors.push(...scanned.errors.map((error) => `${sqlPath}: ${error}`));
  }

  cachedRepositoryScan = { errors, hits, records };
  return cachedRepositoryScan;
}

function conciseHit(hit) {
  return {
    path: hit.path,
    line: hit.line,
    argumentCount: hit.argumentCount,
    callSha256: hit.callSha256,
  };
}

function assertOversizedAllowlist(hits) {
  const oversized = hits
    .filter((hit) => hit.argumentCount > POSTGRES_FUNCTION_ARGUMENT_LIMIT)
    .sort((left, right) => left.path.localeCompare(right.path) || left.line - right.line);
  assert.deepEqual(
    oversized.map(conciseHit),
    [{
      path: HISTORICAL_OVERSIZED_EXCEPTION.path,
      line: HISTORICAL_OVERSIZED_EXCEPTION.line,
      argumentCount: HISTORICAL_OVERSIZED_EXCEPTION.argumentCount,
      callSha256: HISTORICAL_OVERSIZED_EXCEPTION.callSha256,
    }],
    'a new or changed jsonb_build_object call exceeds PostgreSQL FUNC_MAX_ARGS',
  );
  return oversized;
}

function ceilingOccurrences(hits) {
  return hits
    .filter((hit) => hit.argumentCount === POSTGRES_FUNCTION_ARGUMENT_LIMIT)
    .map((hit) => `${hit.path}:${hit.line}`)
    .sort();
}

function assertCeilingAllowlist(hits) {
  assert.deepEqual(
    ceilingOccurrences(hits),
    EXPECTED_CEILING_OCCURRENCES,
    'a jsonb_build_object call reached the exact ceiling without explicit review',
  );
}

test('balanced SQL scanner handles comments, quoted text, dollar bodies, arrays, and nested calls', () => {
  const fixture = canonicalSql(String.raw`
-- jsonb_build_object('line_comment', 1)
/* outer /* jsonb_build_object('nested_comment', 2) */ comment */
SELECT 'jsonb_build_object(''ordinary_string'', 3)',
       E'jsonb_build_object(\'escape_string\', 4)';
CREATE FUNCTION fixture() RETURNS jsonb LANGUAGE plpgsql AS $function$
BEGIN
  RETURN pg_catalog."jsonb_build_object"/* trivia */(
    'nested_function', some_call(1, 2),
    'array', ARRAY[1, 2],
    'quoted_comma', 'value,inside',
    'nested_json', jsonb_build_object('first', 1, 'second', 2)
  );
END
$function$;
`);
  const result = scanSql(fixture);
  assert.deepEqual(result.errors, []);
  assert.deepEqual(result.hits.map((hit) => hit.argumentCount), [8, 4]);
  assert.deepEqual(result.hits.map((hit) => hit.dollarDepth), [1, 1]);
});

test('scanner enters only executable quoted routine and dynamic SQL bodies', () => {
  const ignoredOversizedArguments = Array.from(
    { length: POSTGRES_FUNCTION_ARGUMENT_LIMIT + 2 },
    (_, index) => index + 1,
  ).join(', ');
  const fixture = canonicalSql(String.raw`
SELECT $data$jsonb_build_object(${ignoredOversizedArguments})$data$,
       'jsonb_build_object(${ignoredOversizedArguments})',
       E'escaped quote before inert text: \' jsonb_build_object(${ignoredOversizedArguments})';

CREATE FUNCTION dollar_body_fixture() RETURNS jsonb
LANGUAGE plpgsql
AS $routine$
BEGIN
  RETURN jsonb_build_object(
    'outer', some_call(1, 2),
    'nested', jsonb_build_object('first', 1)
  );
END
$routine$;

CREATE PROCEDURE ordinary_body_fixture()
LANGUAGE plpgsql
AS 'BEGIN
  PERFORM pg_catalog."jsonb_build_object"(
    ''outer'', ARRAY[1, 2],
    ''nested'', jsonb_build_object(''first'', 1)
  );
END';

CREATE FUNCTION escape_body_fixture() RETURNS jsonb
AS E'BEGIN\n  RETURN jsonb_build_object(\'outer\', ARRAY[1, 2], \'nested\', jsonb_build_object(\'first\', 1));\nEND'
LANGUAGE plpgsql;

DO $do_body$
BEGIN
  EXECUTE $dynamic$SELECT jsonb_build_object('dynamic', ARRAY[1, 2])$dynamic$;
END
$do_body$;
`);
  const result = scanSql(fixture);
  assert.deepEqual(result.errors, []);
  assert.deepEqual(result.hits.map((hit) => hit.argumentCount), [4, 2, 4, 2, 4, 2, 2]);
  assert.deepEqual(result.hits.map((hit) => hit.dollarDepth), [1, 1, 1, 1, 1, 1, 2]);
});

test('repository allowlists reject deterministic new and changed arity mutations', () => {
  const scan = repositoryScan();
  const argumentsAtCeiling = Array.from(
    { length: POSTGRES_FUNCTION_ARGUMENT_LIMIT },
    (_, index) => index + 1,
  ).join(', ');
  const argumentsOverCeiling = `${argumentsAtCeiling}, 101, 102`;
  const safeRoutine = canonicalSql(String.raw`
CREATE FUNCTION mutation_fixture() RETURNS jsonb
AS E'BEGIN\n  RETURN jsonb_build_object(${argumentsAtCeiling});\nEND'
LANGUAGE plpgsql;
`);
  const safeScan = scanSql(safeRoutine);
  assert.deepEqual(safeScan.errors, []);
  assert.deepEqual(safeScan.hits.map((hit) => hit.argumentCount), [100]);
  const changedRoutine = safeRoutine.replace(argumentsAtCeiling, argumentsOverCeiling);
  const changedScan = scanSql(changedRoutine);
  assert.deepEqual(changedScan.errors, []);
  assert.deepEqual(changedScan.hits.map((hit) => hit.argumentCount), [102]);
  const virtualViolation = {
    ...changedScan.hits[0],
    path: 'supabase/repeatable/09092026_0000_virtual_arity_violation.sql',
  };
  assert.throws(
    () => assertOversizedAllowlist([...scan.hits, virtualViolation]),
    /a new or changed jsonb_build_object call exceeds PostgreSQL FUNC_MAX_ARGS/u,
  );

  const changedHistoricalFingerprint = scan.hits.map((hit) => (
    hit.path === HISTORICAL_OVERSIZED_EXCEPTION.path
      && hit.line === HISTORICAL_OVERSIZED_EXCEPTION.line
      ? { ...hit, callSha256: '0'.repeat(64) }
      : hit
  ));
  assert.throws(
    () => assertOversizedAllowlist(changedHistoricalFingerprint),
    /a new or changed jsonb_build_object call exceeds PostgreSQL FUNC_MAX_ARGS/u,
  );

  assert.throws(
    () => assertCeilingAllowlist([
      ...scan.hits,
      { ...safeScan.hits[0], path: 'tests/virtual_new_ceiling.sql' },
    ]),
    /a jsonb_build_object call reached the exact ceiling without explicit review/u,
  );
});

test('repository SQL has only its fingerprinted immutable oversized baseline call', (context) => {
  const scan = repositoryScan();
  assert.deepEqual(scan.errors, [], 'the scanner must finish every discovered call and dollar body');

  const oversized = assertOversizedAllowlist(scan.hits);

  const exceptionRecord = scan.records.find(
    (record) => record.path === HISTORICAL_OVERSIZED_EXCEPTION.path,
  );
  assert.ok(exceptionRecord, 'the immutable baseline exception file is missing');
  assert.equal(
    routineOwnerAt(exceptionRecord.source, oversized[0].start),
    HISTORICAL_OVERSIZED_EXCEPTION.owner,
    'the historical exception moved to another routine owner',
  );

  const release = JSON.parse(fs.readFileSync(
    path.join(REPO_ROOT, 'supabase/release/current-release.json'),
    'utf8',
  ));
  assert.ok(
    release.baselineFiles.includes(HISTORICAL_OVERSIZED_EXCEPTION.path),
    'the exception is allowed only while it remains in the immutable NEW baseline',
  );
  assert.equal(
    latestRepeatableDefinitionPath(scan.records, HISTORICAL_OVERSIZED_EXCEPTION.owner),
    HISTORICAL_OVERSIZED_EXCEPTION.supersededBy,
    'the immutable baseline exception must remain superseded by the current safe owner',
  );

  context.diagnostic(
    `scanned ${scan.records.length} SQL files and ${scan.hits.length} jsonb_build_object calls; `
      + 'one exact immutable superseded baseline exception is fingerprinted',
  );
});

test('the two final owners at exactly 100 arguments remain explicit zero-headroom warnings', (context) => {
  const scan = repositoryScan();
  assertCeilingAllowlist(scan.hits);

  for (const binding of FINAL_CEILING_OWNERS) {
    const hit = scan.hits.find(
      (candidate) => candidate.path === binding.path && candidate.line === binding.line,
    );
    assert.ok(hit, `missing ceiling call ${binding.path}:${binding.line}`);
    assert.equal(hit.argumentCount, POSTGRES_FUNCTION_ARGUMENT_LIMIT);
    assert.equal(hit.callSha256, binding.callSha256, `${binding.owner} ceiling call changed`);
    const record = scan.records.find((candidate) => candidate.path === binding.path);
    assert.equal(routineOwnerAt(record.source, hit.start), binding.owner);
    assert.equal(
      latestRepeatableDefinitionPath(scan.records, binding.owner),
      binding.path,
      `${binding.owner} is no longer the final repeatable owner`,
    );
    context.diagnostic(`WARNING: ${binding.owner}: ${binding.warning}`);
  }

  assert.equal(
    latestRepeatableDefinitionPath(scan.records, FINAL_NO_MONEY_OWNER.owner),
    FINAL_NO_MONEY_OWNER.path,
    'the bounded source-less cancellation routine must remain the final no-money owner',
  );
});
