const WORD_START = /[A-Za-z_]/;
const WORD_CONTINUE = /[A-Za-z0-9_$]/;

const normaliseMetaCommand = (value) => String(value || '').trim().replace(/\s+/g, ' ').toUpperCase();

const fail = (message) => {
  throw new Error(`Catalog-owned repeatable transaction envelope is not exact: ${message}`);
};

const hasEscapeStringPrefix = (source, start) => {
  if (start < 1 || !/[Ee]/.test(source[start - 1])) return false;
  const beforePrefix = source[start - 2];
  return beforePrefix === undefined || !WORD_CONTINUE.test(beforePrefix);
};

const readQuoted = (source, start, quote, backslashEscapes = false) => {
  let cursor = start + 1;
  while (cursor < source.length) {
    if (backslashEscapes && source[cursor] === '\\') {
      cursor += 2;
      continue;
    }
    if (source[cursor] === quote) {
      if (source[cursor + 1] === quote) {
        cursor += 2;
        continue;
      }
      return cursor + 1;
    }
    cursor += 1;
  }
  fail(`unterminated ${quote === "'" ? 'string' : 'quoted identifier'}`);
};

const readDollarQuoted = (source, start) => {
  const tagMatch = source.slice(start).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/);
  if (!tagMatch) return null;
  const tag = tagMatch[0];
  const bodyStart = start + tag.length;
  const end = source.indexOf(tag, bodyStart);
  if (end < 0) fail(`unterminated dollar quote ${tag}`);
  return end + tag.length;
};

const scanTopLevel = (source) => {
  const statements = [];
  const metaCommands = [];
  let cursor = 0;
  let lineWhitespaceOnly = true;
  let statementStart = null;
  let tokens = [];

  const addToken = (token, start) => {
    if (statementStart === null) statementStart = start;
    tokens.push(token);
    lineWhitespaceOnly = false;
  };

  while (cursor < source.length) {
    const current = source[cursor];
    const next = source[cursor + 1];

    if (/\s/.test(current)) {
      if (current === '\n') lineWhitespaceOnly = true;
      cursor += 1;
      continue;
    }

    if (current === '-' && next === '-') {
      cursor += 2;
      while (cursor < source.length && source[cursor] !== '\n') cursor += 1;
      continue;
    }

    if (current === '/' && next === '*') {
      let depth = 1;
      cursor += 2;
      while (cursor < source.length && depth > 0) {
        if (source[cursor] === '/' && source[cursor + 1] === '*') {
          depth += 1;
          cursor += 2;
        } else if (source[cursor] === '*' && source[cursor + 1] === '/') {
          depth -= 1;
          cursor += 2;
        } else {
          if (source[cursor] === '\n') lineWhitespaceOnly = true;
          cursor += 1;
        }
      }
      if (depth !== 0) fail('unterminated block comment');
      continue;
    }

    if (current === '\\' && lineWhitespaceOnly) {
      const start = cursor;
      while (cursor < source.length && source[cursor] !== '\n') cursor += 1;
      metaCommands.push({ start, end: cursor, text: source.slice(start, cursor) });
      lineWhitespaceOnly = false;
      continue;
    }

    if (current === "'") {
      addToken('<STRING>', cursor);
      cursor = readQuoted(source, cursor, "'", hasEscapeStringPrefix(source, cursor));
      continue;
    }

    if (current === '"') {
      addToken('<QUOTED_IDENTIFIER>', cursor);
      cursor = readQuoted(source, cursor, '"');
      continue;
    }

    if (current === '$') {
      const quotedEnd = readDollarQuoted(source, cursor);
      if (quotedEnd !== null) {
        addToken('<DOLLAR_QUOTED>', cursor);
        cursor = quotedEnd;
        continue;
      }
    }

    if (WORD_START.test(current)) {
      const start = cursor;
      cursor += 1;
      while (cursor < source.length && WORD_CONTINUE.test(source[cursor])) cursor += 1;
      addToken(source.slice(start, cursor).toUpperCase(), start);
      continue;
    }

    if (current === ';') {
      if (statementStart !== null) {
        statements.push({ start: statementStart, end: cursor + 1, tokens });
      }
      statementStart = null;
      tokens = [];
      lineWhitespaceOnly = false;
      cursor += 1;
      continue;
    }

    addToken(current, cursor);
    cursor += 1;
  }

  if (statementStart !== null) {
    statements.push({ start: statementStart, end: source.length, tokens, unterminated: true });
  }

  return { statements, metaCommands };
};

const transactionKind = (statement) => {
  const [first, second] = statement.tokens;
  if (first === 'BEGIN') return 'BEGIN';
  if (first === 'COMMIT') return 'COMMIT';
  if (first === 'ROLLBACK') return 'ROLLBACK';
  if (first === 'START' && second === 'TRANSACTION') return 'START_TRANSACTION';
  return null;
};

export function prepareCatalogOwnedSourceForRehearsal(sourceSql) {
  const source = String(sourceSql);
  const { statements, metaCommands } = scanTopLevel(source);
  const controls = statements
    .map((statement) => ({ statement, kind: transactionKind(statement) }))
    .filter((entry) => entry.kind !== null);

  if (controls.length === 0) {
    return { mode: 'PLAIN', sourceSql: source };
  }

  const includes = metaCommands.filter(({ text }) => /^\\I(?:R)?(?:\s|$)/i.test(String(text).trim()));
  if (includes.length > 0) fail('include commands are not permitted inside an outer transaction envelope');

  const startTransactions = controls.filter(({ kind }) => kind === 'START_TRANSACTION');
  if (startTransactions.length > 0) fail('START TRANSACTION is not permitted');

  const rollbacks = controls.filter(({ kind }) => kind === 'ROLLBACK');
  if (rollbacks.length > 0) fail('ROLLBACK is not permitted');

  const begins = controls.filter(({ kind }) => kind === 'BEGIN');
  const commits = controls.filter(({ kind }) => kind === 'COMMIT');
  if (begins.length !== 1) fail(`expected one top-level BEGIN, found ${begins.length}`);
  if (commits.length !== 1) fail(`expected one top-level COMMIT, found ${commits.length}`);
  if (statements.length < 2) fail('outer BEGIN/COMMIT do not enclose executable SQL');

  const first = statements[0];
  const last = statements[statements.length - 1];
  if (first !== begins[0].statement || first.tokens.length !== 1 || first.tokens[0] !== 'BEGIN' || first.unterminated) {
    fail('BEGIN must be the first executable SQL statement and must be exactly BEGIN;');
  }
  if (last !== commits[0].statement || last.tokens.length !== 1 || last.tokens[0] !== 'COMMIT' || last.unterminated) {
    fail('COMMIT must be the final executable SQL statement and must be exactly COMMIT;');
  }

  const permittedPreamble = metaCommands.filter(({ end }) => end <= first.start);
  const nonPreamble = metaCommands.filter(({ start }) => start >= first.end);
  if (permittedPreamble.length > 1
      || permittedPreamble.some(({ text }) => normaliseMetaCommand(text) !== '\\SET ON_ERROR_STOP ON')) {
    fail('only one optional \\set ON_ERROR_STOP on preamble is permitted');
  }
  if (nonPreamble.length > 0) fail('psql meta-commands are not permitted inside or after the envelope');

  const innerSql = source.slice(first.end, last.start);
  if (!innerSql.trim()) fail('outer transaction envelope is empty');

  return {
    mode: 'EXACT_OUTER_TRANSACTION_ENVELOPE',
    sourceSql: source,
    innerSql,
    outerBegin: source.slice(first.start, first.end),
    outerCommit: source.slice(last.start, last.end),
  };
}
