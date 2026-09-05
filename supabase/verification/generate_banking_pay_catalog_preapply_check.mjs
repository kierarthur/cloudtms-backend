#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  adaptCatalogLogicalOwnerForRehearsal,
  expandCatalogRepeatableIncludesForRehearsal,
} from './catalog_logical_owner_adapter.mjs';
import { prepareCatalogOwnedSourceForRehearsal } from './catalog_outer_transaction_envelope.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '..', '..');
const repeatableRoot = path.resolve(repoRoot, 'supabase', 'repeatable');
const manifestNames = [
  'banking_pay_revision5_catalog_manifest.json',
  'banking_pay_workbench_certified_source_preview_catalog_manifest.json',
  'banking_pay_targeted_fast_route_certified_reuse_catalog_manifest.json',
  'banking_pay_semantic_ready_cancellation_reversion_catalog_manifest.json',
  'banking_pay_workbench_settled_certificate_v8_catalog_manifest.json',
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
const requireRepeatablePath = (absolutePath, label) => {
  const relative = path.relative(repeatableRoot, absolutePath);
  if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) {
    throw new Error(`Catalog rehearsal ${label} is outside the repository repeatable directory: ${absolutePath}`);
  }
  return absolutePath;
};
const readRepeatableSource = (absolutePath, label) => {
  requireRepeatablePath(absolutePath, label);
  if (!fs.existsSync(absolutePath)) throw new Error(`Catalog-owned repeatable is missing: ${absolutePath}`);
  return fs.readFileSync(absolutePath, 'utf8');
};
const prepareRepeatableSource = (sourceSql) => {
  const prepared = prepareCatalogOwnedSourceForRehearsal(sourceSql);
  return {
    mode: prepared.mode,
    sourceSql: prepared.mode === 'EXACT_OUTER_TRANSACTION_ENVELOPE'
      ? prepared.innerSql
      : prepared.sourceSql,
  };
};
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
const dependenciesByOwnedSource = new Map();
const dependencyOwnersByPendingSource = new Map();
for (const { name: manifestName, value: manifest } of manifests) {
  for (const fn of manifest.functions || []) {
    const sourceFiles = (fn.source_files || []).map(normalizeRepoPath);
    const matchingSources = sourceFiles.filter((sourceFile) => pendingSet.has(sourceFile));
    if (matchingSources.length === 0) continue;

    const dependencyFiles = (fn.preapply_dependency_files || []).map(normalizeRepoPath);
    for (const dependencyFile of dependencyFiles) {
      const absoluteDependency = path.resolve(repoRoot, dependencyFile);
      requireRepeatablePath(absoluteDependency, `dependency for ${fn.schema}.${fn.name}`);
      if (!fs.existsSync(absoluteDependency)) {
        throw new Error(`Catalog-owned pre-apply dependency is missing: ${dependencyFile}`);
      }
    }
    for (const sourceFile of matchingSources) {
      ownedPending.add(sourceFile);
      const existingDependencies = dependenciesByOwnedSource.get(sourceFile) || [];
      dependenciesByOwnedSource.set(
        sourceFile,
        [...new Set([...existingDependencies, ...dependencyFiles])],
      );
      for (const dependencyFile of dependencyFiles) {
        if (!pendingSet.has(dependencyFile)) continue;
        const dependencyOwners = dependencyOwnersByPendingSource.get(dependencyFile) || [];
        dependencyOwnersByPendingSource.set(
          dependencyFile,
          [...new Set([...dependencyOwners, sourceFile])],
        );
      }
    }
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
const emittedRehearsalFiles = new Set();
const appendRehearsalFile = (sourceFile, dependencyOf = null) => {
  if (emittedRehearsalFiles.has(sourceFile)) return;
  const absoluteSource = path.resolve(repoRoot, sourceFile);
  const preparedSource = prepareRepeatableSource(readRepeatableSource(absoluteSource, sourceFile));
  const expanded = expandCatalogRepeatableIncludesForRehearsal({
    sourceSql: preparedSource.sourceSql,
    sourcePath: sourceFile,
    resolveInclude(currentSourcePath, includeReference) {
      const currentAbsolute = path.resolve(repoRoot, currentSourcePath);
      const includedAbsolute = requireRepeatablePath(
        path.resolve(path.dirname(currentAbsolute), includeReference),
        `relative include from ${currentSourcePath}`,
      );
      const includedPath = normalizeRepoPath(includedAbsolute);
      const includedSource = prepareRepeatableSource(readRepeatableSource(includedAbsolute, includedPath));
      return {
        sourcePath: includedPath,
        sourceSql: includedSource.sourceSql,
      };
    },
  });
  const adaptedOwner = adaptCatalogLogicalOwnerForRehearsal(expanded.sourceSql);
  const reason = dependencyOf
    ? `declared dependency of ${dependencyOf}`
    : 'catalog-owned pending repeatable';
  statements.push(`-- ${reason} inlined after exact transaction-envelope, relative-include, and logical-owner validation (${preparedSource.mode}): ${sourceFile}`);
  statements.push(adaptedOwner.sourceSql);
  emittedRehearsalFiles.add(sourceFile);
};
for (const sourceFile of pending) {
  if (dependencyOwnersByPendingSource.has(sourceFile)) {
    appendRehearsalFile(
      sourceFile,
      dependencyOwnersByPendingSource.get(sourceFile).join(', '),
    );
  }
  if (!ownedPending.has(sourceFile)) continue;
  for (const dependencyFile of dependenciesByOwnedSource.get(sourceFile) || []) {
    if (dependencyFile === sourceFile) {
      throw new Error(`Catalog-owned pre-apply dependency cannot reference its owner: ${sourceFile}`);
    }
  }
  appendRehearsalFile(sourceFile);
}

for (const fn of functionsByIdentity.values()) {
  const label = `${fn.schema}.${fn.name}`;
  const expectedPreapplyDefinition = fn.preapply_definition_sha256 || fn.definition_sha256;
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

  IF v_actual_sha256 IS DISTINCT FROM ${sqlLiteral(expectedPreapplyDefinition)} THEN
    RAISE EXCEPTION 'BANKING_PAY_CATALOG_PREAPPLY_DEFINITION_MISMATCH: ${label} (${fn.manifestName})';
  END IF;
END
$catalog_preapply$;`);
}

statements.push('ROLLBACK;', '');
fs.writeFileSync(path.resolve(outputArg), statements.join('\n'), 'utf8');
console.log(`Catalog pre-apply rehearsal covers ${ownedPending.size} pending repeatable(s), ${emittedRehearsalFiles.size - ownedPending.size} declared dependency file(s), and ${functionsByIdentity.size} function identity/identities.`);
