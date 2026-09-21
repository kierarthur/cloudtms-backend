import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import test from 'node:test';

const container = process.env.PLAN6_CANDIDATE_CONTAINER;
const database = process.env.PLAN6_CANDIDATE_DATABASE;
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

test('the Candidate materialisation workflow lock serialises two different save keys', {
  skip: enabled ? false : 'set PLAN6_CANDIDATE_CONTAINER and PLAN6_CANDIDATE_DATABASE',
}, async () => {
  const workflowId = 'fc500000-0000-4000-8000-00000000c001';
  const domain = `WEEKLY_SOURCE_CANDIDATE_MATERIALISE:${workflowId}`;
  const escaped = domain.replaceAll("'", "''");
  const lockKey = `pg_catalog.hashtextextended('${escaped}',0)`;

  const holder = spawn('docker', psqlBaseArgs, {
    stdio: ['pipe', 'pipe', 'pipe'],
    windowsHide: true,
  });
  const holderExit = waitForExit(holder);
  holder.stdin.write(`begin;\nselect pg_catalog.pg_advisory_xact_lock(${lockKey});\n`);
  holder.stdin.write('\\echo PLAN6_CANDIDATE_MATERIALISATION_LOCK_HELD\n');
  await waitForMarker(holder, 'PLAN6_CANDIDATE_MATERIALISATION_LOCK_HELD');

  const competing = spawnSync(
    'docker',
    psqlArgs(`begin;select pg_catalog.pg_try_advisory_xact_lock(${lockKey});rollback`),
    { encoding: 'utf8', windowsHide: true },
  );
  assert.equal(competing.status, 0, competing.stderr);
  assert.equal(competing.stdout.trim(), 'f');

  holder.stdin.end('commit;\n\\q\n');
  const completed = await holderExit;
  assert.equal(completed.code, 0, `holder signal=${completed.signal ?? 'none'}`);

  const afterCommit = spawnSync(
    'docker',
    psqlArgs(`begin;select pg_catalog.pg_try_advisory_xact_lock(${lockKey});rollback`),
    { encoding: 'utf8', windowsHide: true },
  );
  assert.equal(afterCommit.status, 0, afterCommit.stderr);
  assert.equal(afterCommit.stdout.trim(), 't');
});
