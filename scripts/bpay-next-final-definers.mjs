// Source-only signature and recursive release-root ownership. No DB connection.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { inventory, repoRoot } from './cloudtms-db-release-lib.mjs';
import { readManagedClosure, scanManagedSql } from './cloudtms-managed-release-sql.mjs';

const aliases = new Map([['int', 'integer'], ['int4', 'integer'], ['int8', 'bigint'],
  ['int2', 'smallint'], ['bool', 'boolean'], ['decimal', 'numeric'],
  ['varchar', 'character varying'], ['timestamptz', 'timestamp with time zone'],
  ['timestamp', 'timestamp without time zone'], ['float8', 'double precision'],
  ['float4', 'real'], ['char', 'character'], ['bpchar', 'character'],
  ['time', 'time without time zone'], ['timetz', 'time with time zone'],
  ['varbit', 'bit varying']]);
const builtins = new Set(['integer', 'bigint', 'smallint', 'int', 'int2', 'int4', 'int8',
  'boolean', 'bool', 'numeric', 'decimal', 'real', 'double', 'float8', 'float4', 'text',
  'character', 'varchar', 'char', 'uuid', 'json', 'jsonb', 'date', 'time', 'timestamp',
  'timestamptz', 'interval', 'bytea', 'record', 'anyelement', 'anyarray', 'anycompatible',
  'anycompatiblearray', 'regclass', 'regprocedure', 'oid', 'citext', 'void', 'bit', 'varbit', 'bpchar', 'timetz']);
const baselinePublicTypes = new Set([...fs.readFileSync(path.join(repoRoot,
  'supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql'), 'utf8')
  .matchAll(/^\s*create\s+(?:type|table)\s+public\.([a-z_][a-z0-9_]*)\s/gim)].map(row => row[1].toLowerCase()));
function parameterIdentity(tokens, file) {
  const t = tokens.map(token => token.toLowerCase());
  let mode = 'in';
  if (['in', 'out', 'inout', 'variadic'].includes(t[0])) mode = t.shift();
  if (mode === 'out') return null;
  const defaultAt = t.findIndex(token => token === 'default' || token === '=');
  if (defaultAt >= 0) t.splice(defaultAt);
  // Named arguments have a leading identifier; qualified composite types do not.
  if (t.length > 1 && !['.', '[', '('].includes(t[1]) && !builtins.has(t[0])) t.shift();
  if (!t.length || t.includes('<quoted>') || t.includes('<body>')) {
    throw Error(`BPAY_NEXT_SIGNATURE_UNCLASSIFIED_PARAMETER: ${file}`);
  }
  if (t.includes('array') || t.some((token, index) => token === '[' && t[index + 1] !== ']')) {
    throw Error(`BPAY_NEXT_SIGNATURE_ARRAY_SYNTAX_UNCLASSIFIED: ${file}`);
  }
  const array = t.includes('[') ? '[]' : '';
  const clean = t.filter(token => !['[', ']'].includes(token));
  // PostgreSQL routine identity excludes numeric type modifiers, including
  // bit(128) in the existing selector. Retain the underlying type identity.
  let type = clean.join(' ').replace(/\(\s*[\d\s,]+\)/g, '').replace(/\s+/g, ' ').trim().replaceAll(' . ', '.');
  if (type.includes('(') || type.includes(')')) throw Error(`BPAY_NEXT_SIGNATURE_TYPEMOD_UNCLASSIFIED: ${file}`);
  if (type.startsWith('pg_catalog.')) {
    const name = type.slice('pg_catalog.'.length);
    // pg_catalog.char is PostgreSQL's internal one-byte type, distinct from SQL char.
    if (name === 'char' || !builtins.has(name)) throw Error(`BPAY_NEXT_SIGNATURE_QUALIFIED_TYPE_UNCLASSIFIED: ${file}`);
    type = name;
  }
  type = aliases.get(type) ?? type;
  if (baselinePublicTypes.has(type)) type = `public.${type}`;
  const canonicalBuiltins = new Set([...builtins].map(name => aliases.get(name) ?? name));
  canonicalBuiltins.add('timestamp with time zone');
  canonicalBuiltins.add('time with time zone');
  if (!type.includes('.') && !canonicalBuiltins.has(type)) {
    throw Error(`BPAY_NEXT_SIGNATURE_UNQUALIFIED_TYPE_UNCLASSIFIED: ${file} (${type})`);
  }
  return type + array;
}

export function routineDefinitions(source, file) {
  const result = [];
  for (const row of scanManagedSql(source).statements) {
    const t = row.tokens;
    const at = t[0] === 'CREATE' ? t.findIndex(token => token === 'FUNCTION' || token === 'PROCEDURE') : -1;
    if (at !== 1 && !(at === 3 && t[1] === 'OR' && t[2] === 'REPLACE')) continue;
    const open = t.indexOf('(', at + 1);
    const identity = t.slice(at + 1, open).join('').toLowerCase();
    if (open < 0 || !/^[a-z_][a-z0-9_$]*\.[a-z_][a-z0-9_$]*$/.test(identity)) {
      throw Error(`BPAY_NEXT_SIGNATURE_UNCLASSIFIED_NAME: ${file}`);
    }
    let depth = 1, brackets = 0, segment = [], parameters = [], closed = false;
    for (let i = open + 1; i < t.length; i++) {
      const token = t[i];
      if (token === '(') depth++;
      if (token === ')') depth--;
      if (token === '[') brackets++;
      if (token === ']') brackets--;
      if (depth === 0) {
        if (segment.length) parameters.push(segment);
        closed = true; break;
      }
      if (token === ',' && depth === 1 && brackets === 0) { parameters.push(segment); segment = []; }
      else segment.push(token);
    }
    if (!closed) throw Error(`BPAY_NEXT_SIGNATURE_UNCLOSED: ${file}`);
    result.push(`${identity}(${parameters.map(p => parameterIdentity(p, file)).filter(p => p !== null).join(',')})`);
  }
  return result;
}

function executionDefinitions(relative, stack = []) {
  if (stack.includes(relative)) throw Error('BPAY_NEXT_SIGNATURE_INCLUDE_CYCLE');
  const source = fs.readFileSync(path.join(repoRoot, relative), 'utf8').replaceAll('\r\n', '\n');
  const parsed = scanManagedSql(source), result = [];
  const events = [...parsed.statements.map(row => ({ ...row, kind: 'statement' })),
    ...parsed.meta.filter(row => /^\\ir\s/i.test(row.text)).map(row => ({ ...row, kind: 'include' }))]
    .sort((a, b) => a.start - b.start);
  for (const row of events) {
    if (row.kind === 'statement') {
      for (const signature of routineDefinitions(source.slice(row.start, row.end), relative)) {
        result.push({ signature, physical: relative });
      }
    } else {
      const match = row.text.match(/^\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/i);
      if (!match) throw Error('BPAY_NEXT_SIGNATURE_INCLUDE_INVALID');
      const child = path.posix.normalize(path.posix.join(path.posix.dirname(relative), match[1] ?? match[2] ?? match[3]));
      result.push(...executionDefinitions(child, [...stack, relative]));
    }
  }
  return result;
}

export function definerInventory(current = inventory()) {
  const roots = [], routines = new Map();
  for (const item of current.repeatables) {
    const closure = readManagedClosure(item.path, repoRoot);
    const definitions = [];
    for (const definition of executionDefinitions(item.path)) {
        const row = { ...definition, root: item.path, closureSha256: item.sha256 };
        definitions.push(row);
        if (!routines.has(row.signature)) routines.set(row.signature, []);
        routines.get(row.signature).push(row);
    }
    roots.push({ ...item, includes: [...closure.files.keys()], definitions });
  }
  const migrationRoots = current.migrations.map(item => {
    const closure = readManagedClosure(item.path, repoRoot);
    return { ...item, includes: [...closure.files.keys()], definitions: executionDefinitions(item.path) };
  });
  return { roots, routines, migrationRoots };
}

// Both NEW and changed-only loads reapply the controlling root whenever an
// admitted predecessor or migration could otherwise leave an older body last.
export function closeFinalDefinerSelection(pending, current = inventory()) {
  const registry = JSON.parse(fs.readFileSync(path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json'), 'utf8'));
  const { routines, migrationRoots } = definerInventory(current);
  const selected = new Set(pending.repeatables.map(row => row.path));
  const migrations = new Set(pending.migrations.map(row => row.path));
  for (const row of migrationRoots.filter(item => migrations.has(item.path))) {
    for (const definition of row.definitions) {
      const final = routines.get(definition.signature)?.at(-1);
      if (final) selected.add(final.root);
    }
  }
  let added;
  do {
    added = false;
    for (const chain of registry.chains) {
      if (!chain.predecessors?.some(file => selected.has(file))) continue;
      for (const file of chain.finalClosures) {
        if (!current.repeatables.some(row => row.path === file)) throw Error(`BPAY_NEXT_FINAL_CLOSURE_MISSING: ${file}`);
        if (!selected.has(file)) { selected.add(file); added = true; }
      }
    }
  } while (added);
  const repeatables = current.repeatables.filter(row => selected.has(row.path));
  verifyFinalDefiners({ changed: new Set(), selectedRoots: repeatables.map(row => row.path), selectedMigrations: [...migrations] });
  return { ...pending, repeatables };
}

const git = args => {
  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_GIT_FAILED');
  return result.stdout.split(/\r?\n/).filter(Boolean).map(file => file.replaceAll('\\', '/'));
};
export function changedSourcePaths() {
  const changes = new Set([
    ...git(['diff-tree', '--root', '-m', '--first-parent', '--no-commit-id', '--name-only', '-r', 'HEAD']),
    ...git(['diff', '--name-only', 'HEAD', '--']),
    ...git(['ls-files', '--others', '--exclude-standard', '--', 'supabase']),
  ]);
  const registryPath = path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json');
  const base = process.env.CLOUDTMS_FINAL_DEFINER_BASE ?? JSON.parse(fs.readFileSync(registryPath, 'utf8')).releaseBaseCommit;
  if (base) {
    if (!/^[0-9a-f]{40}$/.test(base)) throw Error('BPAY_NEXT_FINAL_DEFINER_BASE_INVALID');
    const ancestor = spawnSync('git', ['merge-base', '--is-ancestor', base, 'HEAD'], { cwd: repoRoot, windowsHide: true });
    if (ancestor.status !== 0) throw Error('BPAY_NEXT_FINAL_DEFINER_BASE_NOT_ANCESTOR');
    for (const file of git(['diff', '--name-only', `${base}..HEAD`, '--'])) changes.add(file);
  }
  return changes;
}

export function verifyFinalDefiners({ changed = changedSourcePaths(), selectedRoots = null, selectedMigrations = null } = {}) {
  const registry = JSON.parse(fs.readFileSync(path.join(repoRoot, 'docs/bpay-next/final-definer-closures.json'), 'utf8'));
  if (registry.formatVersion !== 1 || !Array.isArray(registry.chains)) throw Error('BPAY_NEXT_FINAL_DEFINER_REGISTRY_INVALID');
  const { roots, routines, migrationRoots } = definerInventory();
  const entries = new Map();
  for (const entry of registry.chains) {
    if (entries.has(entry.signature)) throw Error(`BPAY_NEXT_FINAL_DEFINER_DUPLICATE_REGISTRY: ${entry.signature}`);
    entries.set(entry.signature, entry);
    const definitions = routines.get(entry.signature) ?? [];
    const final = definitions.at(-1);
    if (!final || final.root !== entry.finalRoot || final.physical !== entry.finalPhysical) {
      throw Error(`BPAY_NEXT_FINAL_DEFINER_ORDER: ${entry.signature}`);
    }
    for (const definition of definitions) {
      if (!entry.allowedPhysical.includes(definition.physical)) throw Error(`BPAY_NEXT_COMPETING_DEFINER: ${entry.signature} ${definition.physical}`);
    }
  }
  const pendingRoots = selectedRoots ? new Set(selectedRoots) : new Set(roots.filter(root => root.includes.some(file => changed.has(file))).map(root => root.path));
  const pendingMigrations = selectedMigrations ? new Set(selectedMigrations) : new Set(migrationRoots.filter(root => root.includes.some(file => changed.has(file))).map(root => root.path));
  for (const migration of migrationRoots.filter(row => pendingMigrations.has(row.path))) {
    for (const definition of migration.definitions) {
      const final = routines.get(definition.signature)?.at(-1);
      if (final && !pendingRoots.has(final.root)) {
        throw Error(`BPAY_NEXT_MIGRATION_FINAL_REQUIRED: ${definition.signature} ${final.root}`);
      }
    }
  }
  for (const [signature, definitions] of routines) {
    const entry = entries.get(signature);
    const distinctRoots = new Set(definitions.map(row => row.root));
    if (distinctRoots.size > 1 && !entry) throw Error(`BPAY_NEXT_FINAL_DEFINER_UNREGISTERED: ${signature}`);
    if (!entry || !definitions.some(row => row.root !== entry.finalRoot && pendingRoots.has(row.root))) continue;
    if (!pendingRoots.has(entry.finalRoot)) throw Error(`BPAY_NEXT_PENDING_FINAL_REQUIRED: ${signature} ${entry.finalRoot}`);
  }
  return { signatures: routines.size, registered: entries.size, roots: roots.length, pendingRoots: [...pendingRoots] };
}
