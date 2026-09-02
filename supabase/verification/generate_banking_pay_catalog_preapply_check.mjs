#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { adaptCatalogLogicalOwnerForRehearsal } from './catalog_logical_owner_adapter.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const manifestNames = [
  'banking_pay_revision5_catalog_manifest.json',
  'banking_pay_workbench_certified_source_preview_catalog_manifest.json',
  'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json',
  'banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json',
];

const [outputArg, ...pendingArgs] = process.argv.slice(2);
if (!outputArg) {
  console.error('Usage: generate_banking_pay_catalog_preapply_check.mjs <output.sql> [pending-repeatable.sql ...]');
  process.exit(1);
}

const normalizeRepoPath = (value) => path.relative(repoRoot, path.resolve(repoRoot, value))
  .split(path.sep)
  .join('/');
const sqlLiteral = (value) => `'${String(value).replaceAll("'", "''")}'`;
const psqlPath = (value) => String(value).replaceAll('\\', '/').replaceAll("'", "''");
const pending = pendingArgs.map(normalizeRepoPath);
for (const pendingPath of pending) {
  if (!pendingPath.startsWith('supabase/repeatable/') || pendingPath.includes('../')) {
    throw new Error(`Pending repeatable is outside the repository repeatable directory: ${pendingPath}`);
  }
}
const pendingSet = new Set(pending);

const manifests = manifestNames.map((name) => ({
  name,
  value: JSON.parse(fs.readFileSync(path.join(here, name), 'utf8')),
}));

const functionsByIdentity = new Map();
const ownedPending = new Set();
for (const { name: manifestName, value: manifest } of manifests) {
  for (const fn of manifest.functions || []) {
    const sourceFiles = (fn.source_files || []).map(normalizeRepoPath);
    const matchingSources = sourceFiles.filter((sourceFile) => pendingSet.has(sourceFile));
    if (matchingSources.length === 0) continue;

    for (const sourceFile of matchingSources) ownedPending.add(sourceFile);
    const key = `${fn.schema}\u0000${fn.name}\u0000${fn.identity_arguments}`;
    const existing = functionsByIdentity.get(key);
    if (existing && existing.definition_sha256 !== fn.definition_sha256) {
      throw new Error(`Conflicting catalog hashes for ${fn.schema}.${fn.name}`);
    }
    functionsByIdentity.set(key, { ...fn, manifestName });
  }
}

const statements = [
  '\\set ON_ERROR_STOP on',
  'BEGIN;',
];
for (const sourceFile of pending) {
  if (!ownedPending.has(sourceFile)) continue;
  const absoluteSource = path.resolve(repoRoot, sourceFile);
  if (!fs.existsSync(absoluteSource)) {
    throw new Error(`Catalog-owned repeatable is missing: ${sourceFile}`);
  }
  const sourceSql = fs.readFileSync(absoluteSource, 'utf8');
  if (/^\s*(?:BEGIN|COMMIT|ROLLBACK)\s*;/im.test(sourceSql)) {
    throw new Error(`Catalog-owned repeatable contains transaction control and cannot be rehearsed safely: ${sourceFile}`);
  }
  const adaptedOwner = adaptCatalogLogicalOwnerForRehearsal(sourceSql);
  if (adaptedOwner.mode !== 'UNCHANGED') {
    statements.push(`-- Catalog-owned repeatable inlined after exact logical-owner validation: ${sourceFile}`);
    statements.push(adaptedOwner.sourceSql);
  } else {
    statements.push(`\\i '${psqlPath(absoluteSource)}'`);
  }
}

for (const fn of functionsByIdentity.values()) {
  const label = `${fn.schema}.${fn.name}`;
  statements.push(`
DO $catalog_preapply$
DECLARE
  v_oid oid;
  v_count integer;
  v_actual_sha256 text;
BEGIN
  SELECT count(*), pg_catalog.min(p.oid::text)::oid
  INTO v_count, v_oid
  FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = ${sqlLiteral(fn.schema)}
    AND p.proname = ${sqlLiteral(fn.name)}
    AND pg_catalog.pg_get_function_identity_arguments(p.oid) = ${sqlLiteral(fn.identity_arguments)};

  IF v_count <> 1 OR v_oid IS NULL THEN
    RAISE EXCEPTION 'BANKING_PAY_CATALOG_PREAPPLY_IDENTITY_MISMATCH: ${label}';
  END IF;

  SELECT pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(pg_catalog.pg_get_functiondef(v_oid), 'UTF8'),
      'sha256'
    ),
    'hex'
  )
  INTO v_actual_sha256;

  IF v_actual_sha256 IS DISTINCT FROM ${sqlLiteral(fn.definition_sha256)} THEN
    RAISE EXCEPTION 'BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: ${label} (${fn.manifestName})';
  END IF;
END
$catalog_preapply$;`);
}

statements.push('ROLLBACK;', '');
fs.writeFileSync(path.resolve(outputArg), statements.join('\n'), 'utf8');
console.log(`Catalog pre-apply rehearsal covers ${ownedPending.size} pending repeatable(s) and ${functionsByIdentity.size} function identity/identities.`);
