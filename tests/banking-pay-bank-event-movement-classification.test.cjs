const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const root = path.resolve(__dirname, '..');
const migrationPath = path.join(
  root,
  'supabase',
  'migrations',
  '05092026_1330_banking_pay_bank_event_movement_classification_v1.sql'
);
const verificationPath = path.join(
  root,
  'supabase',
  'verification',
  '05092026_1331_banking_pay_bank_event_movement_classification_verification.sql'
);
const eventIngestPath = path.join(
  root,
  'supabase',
  'repeatable',
  '04082026_1210_pay_bank_event_ingest.sql'
);

const migration = fs.readFileSync(migrationPath, 'utf8');
const verification = fs.readFileSync(verificationPath, 'utf8');
const eventIngest = fs.readFileSync(eventIngestPath, 'utf8');

test('bank-event storage accepts the two existing terminal-no-money owner results without changing their meaning', () => {
  assert.match(eventIngest, /movement_classification\s*=\s*v_classification/);
  assert.match(eventIngest, /PROVIDER_CANCELLED_NO_MONEY/);
  assert.match(eventIngest, /PROVIDER_FAILED_NO_MONEY/);

  for (const exactValue of [
    'PRE_BANK_CANCEL',
    'NO_MONEY_UNWIND',
    'TRUE_SETTLED_REVERSAL_REQUIRED',
    'AMBIGUOUS_REVIEW_REQUIRED',
    'PROVIDER_CANCELLED_NO_MONEY',
    'PROVIDER_FAILED_NO_MONEY'
  ]) {
    assert.match(migration, new RegExp(`'${exactValue}'`));
    assert.match(verification, new RegExp(`'${exactValue}'`));
  }

  assert.doesNotMatch(migration, /UPDATE\s+public\.pay_bank_transfer_events/i);
  assert.doesNotMatch(migration, /INSERT\s+INTO\s+public\.pay_bank_transfer_events/i);
  assert.doesNotMatch(migration, /statement_timeout|lock_timeout/i);
});

test('constraint replacement is bounded, validated before the short swap, and fails closed on unknown prior state', () => {
  const add = migration.indexOf('ADD CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk');
  const validate = migration.indexOf('VALIDATE CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk');
  const drop = migration.indexOf('DROP CONSTRAINT pay_bank_transfer_events_movement_classification_chk');
  const rename = migration.indexOf('RENAME CONSTRAINT pay_bank_transfer_events_move_class_v1_replacement_chk');

  assert.ok(add >= 0);
  assert.ok(validate > add);
  assert.ok(drop > validate);
  assert.ok(rename > drop);
  assert.match(migration, /NOT VALID/);
  assert.match(migration, /CONSTRAINT_UNEXPECTED/);
  assert.match(migration, /CONSTRAINT_MISSING/);
  assert.match(migration, /REPLACEMENT_ALREADY_EXISTS/);
  assert.match(verification, /convalidated/);
  assert.match(verification, /TEMPORARY_CONSTRAINT_RETAINED/);
});

test('mutation guards reject removing either lifecycle value or the exact current writer binding', () => {
  for (const removed of ['PROVIDER_CANCELLED_NO_MONEY', 'PROVIDER_FAILED_NO_MONEY']) {
    const mutant = migration.replaceAll(removed, 'REMOVED_MUTANT');
    assert.doesNotMatch(mutant, new RegExp(`'${removed}'`));
    assert.match(verification, new RegExp(`'${removed}'`));
  }

  const writerMutant = eventIngest.replace(
    'movement_classification = v_classification',
    'movement_classification = NULL::text'
  );
  assert.doesNotMatch(writerMutant, /movement_classification\s*=\s*v_classification/);
  assert.match(verification, /movement_classification = v_classification/);
});
