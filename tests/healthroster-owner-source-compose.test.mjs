import test from 'node:test';
import assert from 'node:assert/strict';
import { composeOwnerSource, sourceSha256 } from '../scripts/healthroster-owner-source-compose.mjs';

const compose = (source, hunks, expectedSha256 = sourceSha256(source)) =>
  composeOwnerSource({ source, expectedSha256, hunks });

test('whole-owner composition retains adjacent Unicode source and exactly reverses sequential overlapping anchors', () => {
  const source = 'α retained owner\nunique OLD marker\nkeep adjacent owner\n';
  const result = compose(source, [
    { name: 'first owner', before: 'unique OLD', after: 'unique INTERMEDIATE' },
    { name: 'second owner', before: 'INTERMEDIATE marker', after: 'FINAL marker' }
  ]);
  assert.equal(result.output, 'α retained owner\nunique FINAL marker\nkeep adjacent owner\n');
  assert.equal(result.reverse.whole_reverse_verified, true);
  assert.equal(result.reverse.hunks[0].byte_offset, Buffer.byteLength('α retained owner\n'));
});

test('stale whole-owner source is refused before applying any hunk', () => {
  assert.throws(() => compose('current', [{ name: 'x', before: 'current', after: 'new' }], sourceSha256('older')),
    /OWNER_SOURCE_PIN_MISMATCH/);
});

test('missing or ambiguous anchors cannot silently replace another owner', () => {
  for (const source of ['absent', 'old and old'])
    assert.throws(() => compose(source, [{ name: 'x', before: 'old', after: 'new' }]), /OWNER_ANCHOR_NOT_UNIQUE/);
});

test('later conflicting edits are refused rather than using an older whole file', () => {
  assert.throws(() => compose('only OLD', [
    { name: 'first', before: 'OLD', after: 'NEW' },
    { name: 'second', before: 'OLD', after: 'OTHER' }
  ]), /OWNER_ANCHOR_NOT_UNIQUE: second/);
});

test('replacement text containing JavaScript substitution tokens is literal', () => {
  assert.equal(compose('before anchor after', [{ name: 'literal', before: 'anchor', after: '$& $` $\'' }]).output,
    'before $& $` $\' after');
});
