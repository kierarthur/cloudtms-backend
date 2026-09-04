import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const contractPath = 'supabase/release/current-contract.json';
const databaseSourcePattern = /^supabase\/(?:migrations|repeatable)\/.*\.sql$/i;
const contractBearingPattern = /\b(?:create\s+(?:or\s+replace\s+)?(?:function|procedure|table|view|materialized\s+view|type|policy|trigger)|alter\s+(?:function|procedure|table|view|type|policy)|drop\s+(?:function|procedure|table|view|materialized\s+view|type|policy|trigger)|grant\b|revoke\b)\b/i;

export function contractBearingDatabaseFiles(changedFiles, readFile = relative =>
  fs.readFileSync(path.join(repoRoot, relative), 'utf8')) {
  return changedFiles.filter(relative => {
    if (!databaseSourcePattern.test(relative)) return false;
    const absolute = path.join(repoRoot, relative);
    if (!fs.existsSync(absolute)) return true;
    return contractBearingPattern.test(readFile(relative));
  });
}

export function couplingFailure(changedFiles, readFile) {
  const contractFiles = contractBearingDatabaseFiles(changedFiles, readFile);
  if (contractFiles.length === 0 || changedFiles.includes(contractPath)) return null;
  return `Contract-bearing database source changed without ${contractPath}: ${contractFiles.join(', ')}`;
}

function gitLines(args) {
  const result = spawnSync('git', args, { cwd: repoRoot, encoding: 'utf8' });
  if (result.status !== 0) throw new Error(result.stderr.trim() || `git ${args.join(' ')} failed`);
  return result.stdout.split(/\r?\n/).map(value => value.trim()).filter(Boolean);
}

function verify(label, changedFiles) {
  const failure = couplingFailure(changedFiles);
  if (failure) throw new Error(`${label}: ${failure}`);
}

try {
  verify('HEAD commit', gitLines(['diff-tree', '--root', '--no-commit-id', '--name-only', '-r', 'HEAD']));
  verify('working tree', gitLines(['diff', '--name-only', 'HEAD', '--']));
  console.log('Database source and generated contract coupling: PASS');
} catch (error) {
  console.error(`Database contract coupling failed: ${error.message}`);
  process.exitCode = 1;
}
