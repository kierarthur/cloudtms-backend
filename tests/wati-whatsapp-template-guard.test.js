import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const worker = readFileSync(new URL('../broker/src/index.js', import.meta.url), 'utf8');
const mailshots = readFileSync(new URL('../supabase/repeatable/04032026_mailshots.sql', import.meta.url), 'utf8');

test('accepted WATI receiver results are not misclassified as invalid numbers', () => {
  assert.match(worker, /const hasValidityResult = Object\.prototype\.hasOwnProperty\.call\(rr, 'isValidWhatsAppNumber'\);/);
  assert.match(worker, /normalizedValidityResult === 'true'/);
  assert.match(worker, /normalizedValidityResult === '1'/);
  assert.match(worker, /if \(\(!localMessageId && !isValid\) \|\| errStr\)/);
  assert.doesNotMatch(worker, /if \(!isValid \|\| errStr\)/);
});

test('WhatsApp free text preserves printable Unicode and uses the safe 750-character cap', () => {
  assert.match(mailshots, /v_whatsapp_max int := 750;/);
  assert.match(mailshots, /regexp_replace\(v_rendered_message, '\[\[:cntrl:\]\]\+', ' ', 'g'\)/);
  assert.match(mailshots, /regexp_replace\(v_rendered_message, '\[\[:space:\]\]\+', ' ', 'g'\)/);
  assert.match(mailshots, /rtrim\(left\(v_rendered_message, v_whatsapp_max\)\)/);
  assert.doesNotMatch(mailshots, /\[\^A-Za-z ,\]\+/);
  assert.doesNotMatch(mailshots, /whatsapp_max_chars/);
});

test('the 750-character cap retains at least 100 characters of wrapper headroom', () => {
  const fixedWrapperWithConservativeCrLf = [
    'This is an update from Arthur Rai Medical Services.',
    '   ',
    'Please contact us if any of the above information required clarification.',
    '\r\n',
    'Many thanks',
    '\r\n',
    'Arthur Rai Medical Services',
    '   '
  ].join('');

  assert.equal(fixedWrapperWithConservativeCrLf.length, 172);
  assert.ok(1024 - fixedWrapperWithConservativeCrLf.length - 750 >= 100);
});
