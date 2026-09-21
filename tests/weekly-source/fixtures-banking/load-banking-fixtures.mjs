#!/usr/bin/env node
// Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a).
//
// Thin loader.  It shells out to `psql` exactly as the repository's own
// `tests/weekly-source/adapters/database-scenario-adapter.mjs` does (`PSQL_BIN`,
// `-X -v ON_ERROR_STOP=1`), so it adds no dependency and no second connection
// style.  All the work is in the SQL; this file only sequences it.
//
// Usage:
//   node tests/weekly-source/fixtures-banking/load-banking-fixtures.mjs \
//     --url postgresql://postgres:***@127.0.0.1:55433/ws62_wp16a_run [--selfcheck]
//
//   Or set WEEKLY_SOURCE_BANKING_FIXTURE_URL instead of --url.
//
// Exit codes: 0 success, 1 failure.
//
// The loader never creates or drops a database and never connects to anything
// hosted: `ws_banking_fixture.assert_local_only()` refuses to run outside a
// disposable local proof database, and that check is in the SQL, not here.

import { spawnSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const FIXTURE_DIRECTORY = dirname(fileURLToPath(import.meta.url));

// `build-all.sql` must run with psql's default autocommit: the real pre-bank
// cancellation chain needs each phase in its own transaction, because
// `pay_payment_correction_expand_work` sets `run_after_utc = clock_timestamp()`
// while `banking_pay_operation_claim_next` compares against `now()`.
const STEPS = Object.freeze([
  { name: 'install', file: 'install.sql' },
  { name: 'build', file: 'build-all.sql' },
]);

function parseArguments(argv) {
  const options = { url: process.env.WEEKLY_SOURCE_BANKING_FIXTURE_URL ?? '', selfcheck: false };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === '--url') {
      options.url = argv[index + 1] ?? '';
      index += 1;
    } else if (argument.startsWith('--url=')) {
      options.url = argument.slice('--url='.length);
    } else if (argument === '--selfcheck') {
      options.selfcheck = true;
    } else {
      throw new Error(`Unrecognised argument: ${argument}`);
    }
  }
  if (!options.url) {
    throw new Error('A connection URL is required (--url or WEEKLY_SOURCE_BANKING_FIXTURE_URL).');
  }
  return options;
}

function runPsqlFile(url, file) {
  const binary = process.env.PSQL_BIN ?? 'psql';
  const result = spawnSync(binary, [url, '-X', '-v', 'ON_ERROR_STOP=1', '-f', join(FIXTURE_DIRECTORY, file)], {
    cwd: FIXTURE_DIRECTORY,
    encoding: 'utf8',
    // JIT off, exactly as the environment report requires for this database.
    env: { ...process.env, PGOPTIONS: process.env.PGOPTIONS ?? '-c jit=off' },
    maxBuffer: 64 * 1024 * 1024,
  });

  if (result.error) {
    throw new Error(`Could not run ${binary}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    const detail = `${result.stdout ?? ''}\n${result.stderr ?? ''}`.trim();
    throw new Error(`psql failed on ${file} (exit ${result.status}).\n${detail}`);
  }
  return `${result.stdout ?? ''}`;
}

function main() {
  const options = parseArguments(process.argv.slice(2));

  for (const step of STEPS) {
    process.stdout.write(`[fixtures-banking] ${step.name}: ${step.file}\n`);
    runPsqlFile(options.url, step.file);
  }

  if (options.selfcheck) {
    process.stdout.write('[fixtures-banking] selfcheck: selfcheck.sql\n');
    const output = runPsqlFile(options.url, 'selfcheck.sql');
    process.stdout.write(output.endsWith('\n') ? output : `${output}\n`);
  }

  process.stdout.write('[fixtures-banking] done\n');
}

try {
  main();
} catch (error) {
  process.stderr.write(`[fixtures-banking] ${error.message}\n`);
  process.exitCode = 1;
}
