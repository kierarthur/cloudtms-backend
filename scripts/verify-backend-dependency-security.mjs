import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const packageJson = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
const lock = JSON.parse(await readFile(new URL('../package-lock.json', import.meta.url), 'utf8'));
const root = lock.packages?.[''] || {};

assert.equal(packageJson.packageManager, 'npm@11.12.1');
assert.equal(packageJson.engines?.node, '>=24.0.0 <25.0.0');
assert.equal(packageJson.engines?.npm, '>=11.0.0 <12.0.0');
assert.equal(packageJson.dependencies?.['@cloudflare/puppeteer'], '1.4.0');
assert.equal(packageJson.devDependencies?.wrangler, '4.125.0');
assert.equal(packageJson.overrides?.['@puppeteer/browsers'], '3.2.1');
assert.equal(
  packageJson.dependencies?.xlsx,
  'https://cdn.sheetjs.com/xlsx-0.20.3/xlsx-0.20.3.tgz'
);

assert.equal(root.packageManager, undefined);
assert.equal(root.dependencies?.['@cloudflare/puppeteer'], '1.4.0');
assert.equal(root.devDependencies?.wrangler, '4.125.0');
assert.equal(root.dependencies?.xlsx, packageJson.dependencies.xlsx);

const xlsx = lock.packages?.['node_modules/xlsx'];
assert.equal(xlsx?.version, '0.20.3');
assert.equal(xlsx?.resolved, packageJson.dependencies.xlsx);
assert.match(String(xlsx?.integrity || ''), /^sha512-/);

const puppeteer = lock.packages?.['node_modules/@cloudflare/puppeteer'];
assert.equal(puppeteer?.version, '1.4.0');
const patchedBrowsers =
  lock.packages?.['node_modules/@puppeteer/browsers'] ||
  lock.packages?.['node_modules/@cloudflare/puppeteer/node_modules/@puppeteer/browsers'];
assert.equal(patchedBrowsers?.version, '3.2.1');

for (const blocked of ['basic-ftp', 'extract-zip', 'ip-address']) {
  const matches = Object.keys(lock.packages || {}).filter(
    (path) => path === `node_modules/${blocked}` || path.endsWith(`/node_modules/${blocked}`)
  );
  assert.deepEqual(matches, [], `${blocked} must not remain in the dependency graph`);
}

assert.equal(lock.packages?.['node_modules/wrangler']?.version, '4.125.0');
console.log('Backend dependency provenance and patched graph: PASS');
