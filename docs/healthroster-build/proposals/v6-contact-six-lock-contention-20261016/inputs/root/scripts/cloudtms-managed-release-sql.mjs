import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';

export const MANAGED_RELEASE_VERSION = 'CLOUDTMS_MANAGED_RELEASE_V1';
export const MANAGED_RELEASE_LOCK = 'cloudtms_database_release_admission_v1';
export const literal = value => `'${String(value).replaceAll("'", "''")}'`;
export const canonical = source => String(source).replaceAll('\r\n', '\n');
export const digest = source => crypto.createHash('sha256').update(source).digest('hex');

// This is a packaging parser, not a SQL rewriter. Dollar-quoted procedure and
// function bodies (including the retained backfill's internal COMMIT) are opaque.
export function scanManagedSql(sourceSql) {
  const source = canonical(sourceSql);
  const statements = [], meta = [];
  let i = 0, start = null, tokens = [], whiteLine = true;
  const token = (value, at) => { start ??= at; tokens.push(value); whiteLine = false; };
  while (i < source.length) {
    const c = source[i], next = source[i + 1];
    if (/\s/.test(c)) { if (c === '\n') whiteLine = true; i++; continue; }
    if (c === '-' && next === '-') { while (i < source.length && source[i] !== '\n') i++; continue; }
    if (c === '/' && next === '*') {
      let depth = 1; i += 2;
      while (i < source.length && depth) {
        if (source[i] === '/' && source[i + 1] === '*') { depth++; i += 2; }
        else if (source[i] === '*' && source[i + 1] === '/') { depth--; i += 2; }
        else { if (source[i] === '\n') whiteLine = true; i++; }
      }
      if (depth) throw Error('MANAGED_SQL_UNTERMINATED_COMMENT');
      continue;
    }
    if (c === '\\' && whiteLine) {
      const at = i; while (i < source.length && source[i] !== '\n') i++;
      meta.push({ start: at, end: i, text: source.slice(at, i) }); whiteLine = false; continue;
    }
    if (c === "'" || c === '"') {
      const at = i, quote = c;
      const escape = quote === "'" && /[eE]/.test(source[i - 1] || '') && !/[A-Za-z0-9_$]/.test(source[i - 2] || '');
      i++; let closed = false;
      while (i < source.length) {
        if (escape && source[i] === '\\') { i += 2; continue; }
        if (source[i] === quote) {
          if (source[i + 1] === quote) { i += 2; continue; }
          i++; closed = true; break;
        }
        i++;
      }
      if (!closed) throw Error('MANAGED_SQL_UNTERMINATED_QUOTE');
      token('<QUOTED>', at); continue;
    }
    if (c === '$') {
      const match = source.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/);
      if (match) {
        const at = i, tag = match[0], end = source.indexOf(tag, i + tag.length);
        if (end < 0) throw Error('MANAGED_SQL_UNTERMINATED_BODY');
        i = end + tag.length; token('<BODY>', at); continue;
      }
    }
    if (/[A-Za-z_]/.test(c)) {
      const at = i; while (i < source.length && /[A-Za-z0-9_$]/.test(source[i])) i++;
      token(source.slice(at, i).toUpperCase(), at); continue;
    }
    if (c === ';') {
      if (start !== null) statements.push({ start, end: i + 1, tokens });
      start = null; tokens = []; whiteLine = false; i++; continue;
    }
    token(c, i++);
  }
  if (start !== null) statements.push({ start, end: source.length, tokens, unterminated: true });
  return { source, statements, meta };
}

const TRANSACTIONS = new Set(['BEGIN', 'COMMIT', 'ROLLBACK', 'START', 'END', 'ABORT', 'SAVEPOINT', 'RELEASE', 'PREPARE']);
const HEADS = new Set(['CREATE', 'ALTER', 'DROP', 'COMMENT', 'GRANT', 'REVOKE', 'DO', 'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'SET', 'RESET', 'NOTIFY', 'TRUNCATE', 'LOCK', 'ANALYZE', 'REFRESH', 'VALUES']);

export function exactAtomicBody(sourceSql) {
  const parsed = scanManagedSql(sourceSql);
  const { source, statements, meta } = parsed;
  for (const entry of meta) {
    if (!/^\\set\s+ON_ERROR_STOP\s+on\s*$/i.test(entry.text)) {
      throw Error(`MANAGED_SQL_UNCLASSIFIED_META: ${entry.text.split(/\s/)[0]}`);
    }
  }
  const controls = statements.filter(row => TRANSACTIONS.has(row.tokens[0]));
  let first = null, last = null;
  if (controls.length) {
    [first] = statements; last = statements.at(-1);
    if (controls.length !== 2 || controls[0] !== first || controls[1] !== last
      || first.tokens.join(' ') !== 'BEGIN' || last.tokens.join(' ') !== 'COMMIT'
      || first.unterminated || last.unterminated) throw Error('MANAGED_SQL_UNCLASSIFIED_TRANSACTION');
  }
  for (const row of statements.filter(row => row !== first && row !== last)) {
    if (row.unterminated || !HEADS.has(row.tokens[0])
      || row.tokens.includes('CONCURRENTLY')
      || row.tokens.some(word => word.startsWith('PG_ADVISORY_UNLOCK'))
      || (row.tokens[0] === 'SET' && row.tokens.includes('TRANSACTION'))) {
      throw Error(`MANAGED_SQL_UNCLASSIFIED_STATEMENT: ${row.tokens[0]}`);
    }
  }
  // Remove packaging only; never change quoted bodies or other SQL bytes.
  const remove = [...meta, ...(first ? [first, last] : [])].sort((a, b) => b.start - a.start);
  let body = source;
  for (const row of remove) body = body.slice(0, row.start) + body.slice(row.end);
  return body;
}

// Includes remain source-owned and relative to their original files. Expand
// canonically for classification; the executable mapped tree remains separate.
export function readManagedClosure(relative, root, stack = [], mapSource = value => value) {
  const normal = relative.replaceAll('\\', '/');
  if (!normal.startsWith('supabase/') || path.isAbsolute(normal) || normal.split('/').includes('..')) throw Error('MANAGED_SQL_PATH_INVALID');
  if (stack.includes(normal)) throw Error('MANAGED_SQL_INCLUDE_CYCLE');
  const original = canonical(fs.readFileSync(path.join(root, normal), 'utf8'));
  const source = mapSource(original, normal);
  const parsed = scanManagedSql(source);
  const includes = parsed.meta.filter(row => /^\\ir\s/i.test(row.text));
  let expanded = source;
  const files = new Map([[normal, digest(original)]]), ordered = [{ path: normal, source: original }], replacements = [];
  for (const row of includes) {
    const match = row.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);
    if (!match) throw Error('MANAGED_SQL_INCLUDE_INVALID');
    const childPath = path.posix.normalize(path.posix.join(path.posix.dirname(normal), match[1] ?? match[2] ?? match[3]));
    const child = readManagedClosure(childPath, root, [...stack, normal], mapSource);
    for (const [key, hash] of child.files) files.set(key, hash);
    ordered.push(...child.ordered); replacements.push({ ...row, value: child.expanded });
  }
  for (const row of replacements.reverse()) expanded = expanded.slice(0, row.start) + row.value + expanded.slice(row.end);
  return { path: normal, source, expanded, files, ordered,
    closureHash: digest(Buffer.concat(ordered.flatMap(row => [Buffer.from(`${row.path}\0`), Buffer.from(row.source), Buffer.from('\0')])))};
}

export function managedManifestHash({ current, release, root, executionPolicy }) {
  const files = [...release.baselineFiles, release.controlPlaneMigration, release.bootstrapFile];
  const bootstrap = files.map(file => [file, digest(canonical(fs.readFileSync(path.join(root, file), 'utf8')))]);
  return digest(JSON.stringify({ version: MANAGED_RELEASE_VERSION,
    migrations: current.migrations.map(({ path: file, sha256 }) => [file, sha256]),
    repeatables: current.repeatables.map(({ path: file, sha256 }) => [file, sha256]),
    bootstrap, baselineRepeatableLock: release.baselineRepeatableLock,
    baselineRepeatables: JSON.parse(fs.readFileSync(path.join(root, release.baselineRepeatableLock), 'utf8')),
    executionPolicy,
    verificationFiles: release.verificationFiles.map(file=>[file,digest(canonical(fs.readFileSync(path.join(root,file),'utf8')))]),
    newVerificationFiles: release.newVerificationFiles.map(file=>[file,digest(canonical(fs.readFileSync(path.join(root,file),'utf8')))]) }));
}
