import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';

const tracked = execFileSync('git', ['ls-files', '-z'], { encoding: 'utf8' })
  .split('\0')
  .filter(Boolean);

const forbiddenPath = /(^|\/)(?:\.env(?:\..*)?|\.dev\.vars|playwright[^/]*auth[^/]*\.json|storage[-_]?state[^/]*\.json)$/i;
const forbiddenPaths = tracked.filter((file) => forbiddenPath.test(file.replaceAll('\\', '/')));
assert.deepEqual(forbiddenPaths, [], `Secret-bearing files must not be tracked: ${forbiddenPaths.join(', ')}`);

const rules = [
  ['private-key', /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/],
  ['jwt', /eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/],
  ['github-token', /gh[pousr]_[A-Za-z0-9]{30,}/],
  ['aws-access-key', /AKIA[0-9A-Z]{16}/]
];

const findings = [];
for (const file of tracked) {
  if (!/\.(?:c?js|mjs|ts|tsx|jsonc?|ya?ml|toml|html|css|sql)$/i.test(file)) continue;
  if (/^(?:docs|supabase\/baseline|supabase\/release\/current-contract\.json)\//i.test(file)) continue;
  let source;
  try {
    source = await readFile(file, 'utf8');
  } catch {
    continue;
  }
  for (const [rule, pattern] of rules) {
    if (pattern.test(source)) findings.push({ file, rule });
  }
}

assert.deepEqual(
  findings,
  [],
  `High-confidence credential material detected: ${findings.map(({ file, rule }) => `${file} (${rule})`).join(', ')}`
);
console.log(`Tracked source credential scan: PASS (${tracked.length} files)`);
