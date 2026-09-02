const WORD_START = /[A-Za-z_]/;
const WORD_CONTINUE = /[A-Za-z0-9_$]/;

const fail = (message) => {
  throw new Error(`Catalog-owned repeatable rehearsal is not exact: ${message}`);
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

const hasEscapeStringPrefix = (source, start) => {
  if (start < 1 || !/[Ee]/.test(source[start - 1])) return false;
  const beforePrefix = source[start - 2];
  return beforePrefix === undefined || !WORD_CONTINUE.test(beforePrefix);
};

const scanTopLevel = (source) => {
  const statements = [];
  const metaCommands = [];
  let cursor = 0;
  let statementStart = null;
  let lineWhitespaceOnly = true;

  const startStatement = (at) => {
    if (statementStart === null) statementStart = at;
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
      startStatement(cursor);
      cursor = readQuoted(source, cursor, "'", hasEscapeStringPrefix(source, cursor));
      continue;
    }

    if (current === '"') {
      startStatement(cursor);
      cursor = readQuoted(source, cursor, '"');
      continue;
    }

    if (current === '$') {
      const quotedEnd = readDollarQuoted(source, cursor);
      if (quotedEnd !== null) {
        startStatement(cursor);
        cursor = quotedEnd;
        continue;
      }
    }

    if (current === ';') {
      if (statementStart !== null) statements.push({ start: statementStart, end: cursor + 1 });
      statementStart = null;
      lineWhitespaceOnly = false;
      cursor += 1;
      continue;
    }

    startStatement(cursor);
    cursor += 1;
  }

  if (statementStart !== null) fail('unterminated top-level SQL statement');
  return { statements, metaCommands };
};

const ownerStatement = /^ALTER\s+(FUNCTION|PROCEDURE|ROUTINE)\s+([\s\S]+?)\s+OWNER\s+TO\s+([A-Za-z_][A-Za-z0-9_$]*|"[^"]+")\s*;$/i;
const relativeIncludeCommand = /^\\ir[ \t]+(?:(['"])([^'"\r\n]+)\1|([A-Za-z0-9_./-]+))[ \t]*$/i;

export function expandCatalogRepeatableIncludesForRehearsal({ sourceSql, sourcePath, resolveInclude }) {
  if (typeof resolveInclude !== 'function') fail('include resolver is required');

  const expand = (source, currentPath, stack) => {
    if (stack.includes(currentPath)) {
      fail(`cyclic relative include: ${[...stack, currentPath].join(' -> ')}`);
    }

    const { metaCommands } = scanTopLevel(source);
    let expandedSource = source;
    const includedPaths = [];
    for (const command of metaCommands.toReversed()) {
      const match = relativeIncludeCommand.exec(command.text.trim());
      if (!match) fail(`unsupported psql meta-command in ${currentPath}: ${command.text.trim()}`);
      const includeReference = match[2] ?? match[3];
      const resolved = resolveInclude(currentPath, includeReference);
      if (!resolved || typeof resolved.sourcePath !== 'string' || typeof resolved.sourceSql !== 'string') {
        fail(`include resolver returned an invalid result for ${includeReference}`);
      }
      const child = expand(resolved.sourceSql, resolved.sourcePath, [...stack, currentPath]);
      const replacement = [
        `-- BEGIN EXACT EXPANDED RELATIVE INCLUDE ${resolved.sourcePath}`,
        child.sourceSql,
        `-- END EXACT EXPANDED RELATIVE INCLUDE ${resolved.sourcePath}`,
      ].join('\n');
      expandedSource = expandedSource.slice(0, command.start)
        + replacement
        + expandedSource.slice(command.end);
      includedPaths.unshift(resolved.sourcePath, ...child.includedPaths);
    }
    return { sourceSql: expandedSource, includedPaths };
  };

  return expand(String(sourceSql), String(sourcePath), []);
}

export function adaptCatalogLogicalOwnerForRehearsal(sourceSql) {
  const source = String(sourceSql);
  const { statements, metaCommands } = scanTopLevel(source);
  const replacements = [];

  for (const statement of statements) {
    const raw = source.slice(statement.start, statement.end);
    const match = ownerStatement.exec(raw);
    if (!match) {
      if (/^ALTER\s+[A-Za-z_]+[\s\S]+\bOWNER\s+TO\s+postgres\b/i.test(raw.trim())) {
        fail('only exact ALTER FUNCTION/PROCEDURE/ROUTINE ... OWNER TO postgres; is portable');
      }
      if (/^(?:SET\s+(?:LOCAL\s+|SESSION\s+)?ROLE|SET\s+SESSION\s+AUTHORIZATION|REASSIGN\s+OWNED)\b/i.test(raw.trim())) {
        fail('role-changing SQL is not permitted');
      }
      continue;
    }

    const role = match[3];
    if (role.toLowerCase() !== 'postgres') {
      fail(`unexpected logical owner ${role}`);
    }
    const adapted = raw.replace(/(\s+OWNER\s+TO\s+)postgres(\s*;)$/i, '$1CURRENT_USER$2');
    if (adapted === raw) fail('logical owner clause could not be mapped');
    replacements.push({ ...statement, adapted, identity: `${match[1].toUpperCase()} ${match[2].trim()}` });
  }

  if (replacements.length === 0) return { mode: 'UNCHANGED', sourceSql: source, mappedIdentities: [] };

  if (metaCommands.length > 0) fail('psql meta-commands are not permitted when logical-owner mapping is required');

  let adaptedSource = source;
  for (const replacement of replacements.toReversed()) {
    adaptedSource = adaptedSource.slice(0, replacement.start)
      + replacement.adapted
      + adaptedSource.slice(replacement.end);
  }

  return {
    mode: 'MAPPED_LOGICAL_POSTGRES_TO_CURRENT_USER',
    sourceSql: adaptedSource,
    mappedIdentities: replacements.map(({ identity }) => identity),
  };
}
