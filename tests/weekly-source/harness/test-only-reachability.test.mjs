import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, '../../..');

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const target = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(target));
    else if (/\.(?:js|mjs|cjs)$/.test(entry.name)) files.push(target);
  }
  return files;
}

test('TH-028 production Worker modules cannot import the Weekly Source test harness', async () => {
  const files = await sourceFiles(path.join(repoRoot, 'broker', 'src'));
  for (const file of files) {
    const source = await readFile(file, 'utf8');
    assert(!source.includes('tests/weekly-source/harness'), `${path.relative(repoRoot, file)} imports test-only Weekly Source code`);
    assert(!source.includes('weekly-source/harness/'), `${path.relative(repoRoot, file)} imports test-only Weekly Source code`);
  }
});

test('TH-017 harness does not import network client modules', async () => {
  const files = (await sourceFiles(here)).filter((file) => !file.endsWith('.test.mjs'));
  for (const file of files) {
    const source = await readFile(file, 'utf8');
    assert(!/node:(?:http|https|net|tls|dgram)/.test(source), `${path.basename(file)} imports a network client`);
  }
});

test('TH-028 deployed Worker entries and Wrangler configuration cannot reach harness or controller code', async () => {
  const protectedFiles = [
    path.join(repoRoot, 'wrangler.toml'),
    path.join(repoRoot, 'candidate-broker', 'wrangler.jsonc'),
    path.join(repoRoot, 'broker', 'src', 'index.js'),
  ];
  for (const file of protectedFiles) {
    const source = await readFile(file, 'utf8');
    assert(!source.includes('run-weekly-source-harness'), `${path.relative(repoRoot, file)} reaches the test controller`);
    assert(!source.includes('postgres-cluster.mjs'), `${path.relative(repoRoot, file)} reaches the local PostgreSQL controller`);
    assert(!source.includes('c1-contract-emulator.mjs'), `${path.relative(repoRoot, file)} reaches the C1 emulator`);
  }
});
