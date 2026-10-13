import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';

const root = resolve(import.meta.dirname, '..');
const directory = 'supabase/repeatable/13102026_1438_weekly_source_protection_queries';
const hash = (value) => createHash('sha256').update(value).digest('hex');
const read = (path) => readFileSync(resolve(root, path));
const source = (path) => read(path).toString('utf8').replaceAll('\r\n', '\n');
const files = [
  'supabase/migrations/13102026_1456_weekly_source_protection_query_support_v1.sql',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries_v1.sql',
  ...readdirSync(resolve(root, directory)).sort().map((name) => `${directory}/${name}`),
  'supabase/verification/13102026_1438_weekly_source_protection_queries_v1.sql',
  'broker/src/weekly-source/protected-action-orchestrator-stage-only.mjs',
  'broker/src/weekly-source/protected-bulk-action.mjs',
  'broker/src/weekly-source/protected-automatic-reconcile.mjs',
  'tests/fixtures/healthroster-protected-orchestrator-fixture.mjs',
  'tests/fixtures/healthroster-automatic-reconcile-produced-stage.json',
  'tests/healthroster-protection-source.test.js',
  'tests/healthroster-protected-stage-only.test.js',
  'tests/healthroster-protected-bulk.test.js',
  'tests/healthroster-protected-automatic-reconcile.test.js',
  'docs/healthroster-build/PROTECTED_WAIT_WITNESS_CONTRACT.md',
  'docs/healthroster-build/PROTECTION_INTEGRATION_HUNKS.md',
  'docs/healthroster-build/WP_PROTECTION_IMPLEMENTATION.md',
  'docs/healthroster-build/AUTOMATIC_RECONCILE_COMPONENT_CENSUS.json',
  'docs/healthroster-build/NATIVE_PROTECTION_LOCK_CENSUS.json',
  'docs/healthroster-build/PROTECTION_CALLER_BOUNDARY_CENSUS.json',
  'scripts/healthroster-protection-source-manifest.mjs',
  'scripts/healthroster-automatic-reconcile-fixture.mjs',
  'scripts/healthroster-protection-caller-census.mjs',
];
const routines = [];
for (const path of files.filter((name) => name.endsWith('.sqlinc'))) {
  const text = source(path);
  for (const match of text.matchAll(/create or replace function\s+([a-z0-9_.]+)\s*\(([\s\S]*?)\)\s*returns\s+/gi)) {
    const args = match[2].trim() === '' ? [] : match[2].split(',').map((parameter) => {
      const parts = parameter.trim().replace(/\s+default\s+[\s\S]*$/i, '').split(/\s+/);
      if (parts.length !== 2 || !/^[a-z_]+(\[\])?$/.test(parts[1])) throw new Error(`Unsupported declared signature ${match[0]}`);
      return parts[1];
    });
    const signature = `${match[1]}(${args.join(',')})`;
    const end = text.indexOf('$function$;', match.index);
    if (end < 0) throw new Error(`Function body missing ${signature}`);
    routines.push({ signature, path, body_sha256_lf: hash(text.slice(match.index, end + '$function$;'.length)),
      service_execute: text.includes(`grant execute on function ${signature} to service_role;`) });
  }
}
const manifest = {
  contract: 'HEALTHROSTER_PROTECTION_SOURCE_HANDOFF_V1',
  status: 'SOURCE_ONLY_NOT_INSTALLED_NOT_ACTIVATED',
  branch: 'codex/healthroster-v2-protection-build-13102026',
  base_commit: '83547d19a4e8407d5a330849f0e82736c33e48de',
  files: files.map((path) => ({ path, bytes: read(path).length, sha256: hash(read(path)) })),
  routines,
  root_owned_missing_boundaries: [
    'private.weekly_source_bulk_producer_ready_v1(uuid)',
    'private.weekly_source_bulk_source_cas_v1(uuid,uuid[])',
    'private.weekly_source_shift_on_self_bill_context_v1(uuid,uuid,jsonb)',
    'private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)',
    'private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)',
  ],
  installation_authority: false,
};
writeFileSync(resolve(root, 'docs/healthroster-build/PROTECTION_SOURCE_MANIFEST.json'), `${JSON.stringify(manifest, null, 2)}\n`);
process.stdout.write(`Protection source manifest: ${files.length} files; ${routines.length} declared routines.\n`);
