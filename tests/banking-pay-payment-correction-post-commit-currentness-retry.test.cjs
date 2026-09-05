const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const root = path.resolve(__dirname, '..');
const workerSource = fs.readFileSync(path.join(root, 'broker', 'src', 'index.js'), 'utf8');
const processChunkSource = fs.readFileSync(
  path.join(root, 'supabase', 'repeatable', '04082026_1209_pay_payment_correction_process_chunk.sql'),
  'utf8'
);

function nestedConstArrow(source, name, endPattern, endDescription) {
  const startMarker = `  const ${name} =`;
  const start = source.indexOf(startMarker);
  assert.notEqual(start, -1, `${name} missing`);
  const relativeEnd = source.slice(start + startMarker.length).search(endPattern);
  assert.notEqual(relativeEnd, -1, `${endDescription} boundary missing after ${name}`);
  const end = start + startMarker.length + relativeEnd;
  return source.slice(start, end).trim();
}

function buildClassifier(source = workerSource) {
  const classifier = nestedConstArrow(
    source,
    'isRetryablePaymentCorrectionRefreshError',
    /\r?\n  const extractPaymentCorrectionFinaliseErrorEnvelope =/,
    'extractPaymentCorrectionFinaliseErrorEnvelope'
  );
  const releaseState = nestedConstArrow(
    source,
    'determineReleaseState',
    /\r?\n\r?\n  if \(!env\)/,
    'if (!env)'
  );
  const context = {
    JSON,
    Date,
    upperText(value) {
      return String(value == null ? '' : value).trim().toUpperCase();
    },
    asPlainObject(value) {
      return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    },
    parseTimeMs() {
      return null;
    }
  };
  vm.runInNewContext(
    `${classifier}\n${releaseState}\nthis.classify = isRetryablePaymentCorrectionRefreshError; this.release = determineReleaseState;`,
    context,
    { timeout: 1000 }
  );
  return context;
}

const exactPostCommitCurrentnessError = {
  code: 'P0001',
  message: 'PAYMENT_CORRECTION_POST_COMMIT_AUTHORITY_NOT_FINALIZED',
  json: {
    details: JSON.stringify({
      code: 'REFRESH_RETRY',
      reason: 'POST_COMMIT_SCOPE_GENERATION_NOT_FINALIZED'
    })
  }
};

test('database explicitly reports post-commit authority currentness as retryable before any later route work', () => {
  assert.match(
    processChunkSource,
    /PAYMENT_CORRECTION_POST_COMMIT_AUTHORITY_NOT_FINALIZED[\s\S]*'code','REFRESH_RETRY'[\s\S]*'reason','POST_COMMIT_SCOPE_GENERATION_NOT_FINALIZED'/
  );
  assert.match(
    processChunkSource,
    /a PostgreSQL function cannot commit an[\s\S]*internal transaction[\s\S]*return one bounded continuation/
  );
});

test('Worker keeps the exact post-commit currentness boundary runnable without user review', () => {
  const runtime = buildClassifier();
  const context = { operationType: 'PAYMENT_CORRECTION', phase: 'REFRESH_WORKBENCH' };

  assert.equal(runtime.classify(exactPostCommitCurrentnessError, context), true);
  assert.equal(runtime.release(null, exactPostCommitCurrentnessError, context), 'MORE_WORK');

  assert.equal(runtime.classify(exactPostCommitCurrentnessError, { ...context, operationType: 'PAYMENT_EXECUTE' }), false);
  assert.equal(runtime.classify(exactPostCommitCurrentnessError, { ...context, phase: 'FINALISE' }), false);
  assert.equal(runtime.classify({ ...exactPostCommitCurrentnessError, message: 'PAYMENT_CORRECTION_POST_COMMIT_AUTHORITY_NOT_FINALIZED_NEAR_MISS' }, context), false);
});

test('post-commit retry code is an executable fail-closed mutation boundary', () => {
  const mutated = workerSource.replace(
    /[ \t]*'PAYMENT_CORRECTION_POST_COMMIT_AUTHORITY_NOT_FINALIZED',[ \t]*\r?\n/,
    ''
  );
  assert.notEqual(mutated, workerSource, 'exact retry code must have one explicit classifier entry');
  const runtime = buildClassifier(mutated);
  const context = { operationType: 'PAYMENT_CORRECTION', phase: 'REFRESH_WORKBENCH' };
  assert.equal(runtime.classify(exactPostCommitCurrentnessError, context), false);
  assert.equal(runtime.release(null, exactPostCommitCurrentnessError, context), 'REVIEW_REQUIRED');
});
