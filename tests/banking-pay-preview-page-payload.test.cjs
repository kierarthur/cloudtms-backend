const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const workerSource = fs.readFileSync(
  path.resolve(__dirname, '../broker/src/index.js'),
  'utf8'
);

test('Banking Pay preview-page HTTP response sends one row array only', () => {
  const functionStart = workerSource.indexOf(
    'async function handleBankingPayWorkbenchSessionGetPreviewPage('
  );
  const functionEnd = workerSource.indexOf(
    '\nasync function handleBankingPayWorkbenchSessionOpen(',
    functionStart
  );
  assert.ok(functionStart >= 0 && functionEnd > functionStart, 'preview-page handler must be present');

  const source = workerSource.slice(functionStart, functionEnd);
  assert.match(source, /rows:\s*_rpcRows/);
  assert.match(source, /items:\s*_rpcItems/);
  assert.match(source, /rows:\s*responseRows/);
  assert.doesNotMatch(
    source,
    /items:\s*Array\.isArray\(payload\.items\)\s*\?\s*payload\.items\s*:\s*responseRows/,
    'the HTTP response must not duplicate the Banking Pay row array'
  );
});
