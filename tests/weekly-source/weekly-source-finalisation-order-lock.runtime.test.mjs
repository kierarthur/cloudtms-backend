import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import test from 'node:test';

const container = process.env.PLAN6_FINALISATION_CONTAINER;
const database = process.env.PLAN6_FINALISATION_DATABASE;
const enabled = Boolean(container && database);

const psqlBaseArgs = [
  'exec', '-i', container,
  'psql', '-U', 'postgres', '-d', database,
  '-X', '-qAt', '-v', 'ON_ERROR_STOP=1',
];
const psqlArgs = sql => [...psqlBaseArgs, '-c', sql];

const waitForMarker = (child, marker) => new Promise((resolve, reject) => {
  let stdout = '';
  let stderr = '';
  const timeout = setTimeout(() => {
    child.kill();
    reject(new Error(`Timed out waiting for ${marker}. stdout=${stdout} stderr=${stderr}`));
  }, 10_000);

  child.stdout.on('data', chunk => {
    stdout += chunk.toString();
    if (stdout.includes(marker)) {
      clearTimeout(timeout);
      resolve();
    }
  });
  child.stderr.on('data', chunk => {
    stderr += chunk.toString();
  });
  child.once('error', error => {
    clearTimeout(timeout);
    reject(error);
  });
  child.once('exit', code => {
    if (!stdout.includes(marker)) {
      clearTimeout(timeout);
      reject(new Error(`Lock holder exited ${code} before ${marker}. stderr=${stderr}`));
    }
  });
});

const waitForExit = child => new Promise((resolve, reject) => {
  child.once('error', reject);
  child.once('exit', (code, signal) => resolve({ code, signal }));
});

test('the logical finalisation history lock serialises distinct cycles for one authority domain', {
  skip: enabled ? false : 'set PLAN6_FINALISATION_CONTAINER and PLAN6_FINALISATION_DATABASE',
}, async () => {
  const domain = [
    'weekly_source_finalise_order',
    'a0000000-0000-4000-8000-000000000005',
    'a0000000-0000-4000-8000-000000000002',
    'HEALTHROSTER_ACTUAL_ROWS',
  ].join(':');
  const escapedDomain = domain.replaceAll("'", "''");
  const key = `pg_catalog.hashtextextended('${escapedDomain}',0)`;
  const holder = spawn('docker', psqlBaseArgs, {
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  });
  const holderExit = waitForExit(holder);
  holder.stdin.write(`begin;\nselect pg_catalog.pg_advisory_xact_lock(${key});\n`);
  holder.stdin.write('\\echo PLAN6_FINALISATION_LOCK_HELD\n');
  await waitForMarker(holder, 'PLAN6_FINALISATION_LOCK_HELD');

  const competing = spawnSync(
    'docker',
    psqlArgs(`begin;select pg_catalog.pg_try_advisory_xact_lock(${key});rollback`),
    { encoding: 'utf8', windowsHide: true },
  );
  assert.equal(competing.status, 0, competing.stderr);
  assert.equal(competing.stdout.trim(), 'f');

  holder.stdin.end('commit;\n\\q\n');
  const completed = await holderExit;
  assert.equal(completed.code, 0, `holder signal=${completed.signal ?? 'none'}`);

  const afterCommit = spawnSync(
    'docker',
    psqlArgs(`begin;select pg_catalog.pg_try_advisory_xact_lock(${key});rollback`),
    { encoding: 'utf8', windowsHide: true },
  );
  assert.equal(afterCommit.status, 0, afterCommit.stderr);
  assert.equal(afterCommit.stdout.trim(), 't');
});
