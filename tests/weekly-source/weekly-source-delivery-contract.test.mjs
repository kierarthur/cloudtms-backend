import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const mytms = path.resolve(root, '..', 'mytms');

async function source(base, relative) {
  return readFile(path.join(base, relative), 'utf8');
}

test('database delivery contract is per-target, durable, bounded and browser-denied', async () => {
  const [migration, repeatable, verifier] = await Promise.all([
    source(root, 'supabase/migrations/15092026_2310_weekly_source_delivery_targets.sql'),
    source(root, 'supabase/repeatable/15092026_2311_weekly_source_delivery_targets_v1.sql'),
    source(root, 'supabase/verification/15092026_2312_weekly_source_delivery_targets_v1.sql'),
  ]);
  for (const required of [
    'weekly_message_dispatch_targets', 'weekly_message_target_attempts',
    'weekly_candidate_message_notifications', 'weekly_message_delivery_failures',
  ]) assert.match(migration, new RegExp(required));
  assert.match(repeatable, /set state='SUBMISSION_STARTED'/i);
  assert.match(repeatable, /maximum_attempts/i);
  assert.match(repeatable, /PROVIDER_AMBIGUOUS/i);
  assert.match(repeatable, /WEEKLY_SOURCE_PER_TARGET_DISPATCH_REQUIRED/i);
  assert.match(repeatable, /insert into public\.candidate_notifications/i);
  assert.match(repeatable, /WEEKLY_SOURCE_CANDIDATE_APP_UNAVAILABLE/i);
  assert.match(repeatable, /REQUEST_RETIRED/i);
  for (const signature of [
    'weekly_source_candidate_notification_intent_v1\\(\\)',
    'weekly_source_candidate_notification_retire_v1\\(\\)',
    'weekly_source_delivery_aggregate_command_v1\\(uuid\\)',
    'weekly_source_delivery_target_immutable_v1\\(\\)',
    'weekly_source_delivery_target_set_hash_v1\\(uuid,uuid,text,jsonb\\)',
  ]) {
    assert.match(
      repeatable,
      new RegExp(`revoke all on function private\\.${signature}\\s+from public,anon,authenticated,service_role`, 'i'),
    );
  }
  assert.match(verifier, /anon.*authenticated|authenticated.*anon/is);
  assert.match(verifier, /SUBMISSION_STARTED/i);
});

test('query delivery external-call audit is limited to its owned routine inventory', async () => {
  const [repeatable, verifier] = await Promise.all([
    source(root, 'supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql'),
    source(root, 'supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql'),
  ]);
  assert.doesNotMatch(repeatable, /execute[^;]*owner to postgres/i);
  assert.match(repeatable, /execute[^;]*owner to current_user/i);
  assert.match(
    verifier,
    /namespace\.nspname='public' and procedure\.proname=any\(v_public_names\)[\s\S]*namespace\.nspname='private' and procedure\.proname=any\(v_private_names\)[\s\S]*http_post[\s\S]*external provider call/,
  );
  assert.doesNotMatch(
    verifier,
    /procedure\.proname like 'weekly_source_%'[\s\S]{0,300}http_post/,
  );
});

test('MyTMS universally enables agency delivery while preserving personal preferences', async () => {
  const [migration, repeatable, verifier] = await Promise.all([
    source(mytms, 'supabase/migrations/15092026_2320_weekly_source_push_transport.sql'),
    source(mytms, 'supabase/repeatable/15092026_2321_weekly_source_push_transport_v1.sql'),
    source(mytms, 'supabase/verification/15092026_2322_weekly_source_push_transport_v1.sql'),
  ]);
  assert.match(migration, /alter column push_delivery_enabled set default true/i);
  assert.match(migration, /where not push_delivery_enabled/i);
  assert.match(migration, /check \(push_delivery_enabled\)/i);
  assert.match(repeatable, /PUSH_DELIVERY_MUST_REMAIN_ENABLED/i);
  assert.match(repeatable, /timesheet_expense_attention/i);
  assert.match(repeatable, /PERSONAL_PREFERENCE/i);
  assert.match(repeatable, /token_ciphertext_hex/i);
  assert.match(repeatable, /updated_at_utc=v_device\.source_device_updated_at_utc/i);
  assert.match(repeatable, /revoke all on function control\.weekly_push_target_material_v1[\s\S]*anon,authenticated/i);
  assert.match(verifier, /v_default.*not in \('true','true::boolean'\)/is);
  assert.match(verifier, /exists\(select 1 from control\.agency_app_settings where not push_delivery_enabled\)/i);
  assert.match(verifier, /fresh agency did not receive push delivery by default/i);
  assert.match(verifier, /ROLLBACK_WEEKLY_PUSH_DEFAULT_PROBE/i);
  assert.match(verifier, /personal Candidate push opt-out was not honoured/i);
  assert.match(verifier, /ROLLBACK_WEEKLY_PUSH_OPT_OUT_PROBE/i);
});

test('dedicated Worker has its own Queue, DLQ and service binding', async () => {
  const config = await source(root, 'weekly-source-delivery-worker/wrangler.jsonc');
  assert.match(config, /test-cloudtms-weekly-source-delivery/);
  assert.match(config, /test-cloudtms-weekly-source-delivery-dlq/);
  assert.match(config, /"max_concurrency": 1/);
  assert.match(config, /CLOUDTMS_WEEKLY_SOURCE_RUNTIME/);
  assert.doesNotMatch(config, /BANKING|INVOICE|PAYMENT|SETTLEMENT|REMITTANCE/i);
});

test('candidate unavailability is bounded and never silently becomes manager outreach', async () => {
  const routes = await source(root, 'broker/src/weekly-source/routes.js');
  assert.match(routes, /UNAVAILABLE_NO_ACTIVE_APP_ACCOUNT/);
  assert.match(routes, /accepted:\s*false/);
  assert.match(routes, /candidate_id/);
});

test('transport sources contain no Apple build, upload or store action', async () => {
  const files = [
    'broker/src/weekly-source/delivery-auth.mjs',
    'broker/src/weekly-source/delivery-runtime.mjs',
    'candidate-broker/src/weekly-source-push-authority.js',
    'candidate-broker/src/weekly-source-push-providers.js',
    'weekly-source-delivery-worker/src/index.js',
    'weekly-source-delivery-worker/wrangler.jsonc',
  ];
  const combined = (await Promise.all(files.map((file) => source(root, file)))).join('\n');
  assert.doesNotMatch(combined, /TestFlight|App Store Connect|eas submit|eas build|xcodebuild/i);
});
