import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const migrationPath = path.resolve(
  here,
  '../../supabase/migrations/20072026_1827_banking_alert_preferences_user_fk.sql'
);
const migration = fs.readFileSync(migrationPath, 'utf8');

test('Banking Alert preferences belong to CloudTMS application users', () => {
  assert.match(migration, /drop constraint if exists banking_alert_user_preferences_user_id_fkey/i);
  assert.match(migration, /drop constraint if exists banking_alert_user_preferences_user_fkey/i);
  assert.match(
    migration,
    /add constraint banking_alert_user_preferences_user_fkey[\s\S]*foreign key \(user_id\)[\s\S]*references public\.tms_users\(id\)[\s\S]*on delete cascade/i
  );
  assert.doesNotMatch(migration, /references auth\.users/i);
});
