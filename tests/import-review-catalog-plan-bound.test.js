import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
const read = relative => fs.readFileSync(new URL('../' + relative, import.meta.url), 'utf8').replaceAll('\r\n', '\n');
const original = read('supabase/repeatable/22082026_1706_daily_validation_compatibility_authorities_v1.sql');
const replacement = read('supabase/repeatable/06102026_1523_import_review_catalog_generic_plan_bound_v1.sql');
function body(source) {
  const from = source.indexOf('public._import_review_action_catalog_core_v1(');
  assert.ok(from >= 0);
  const delimiter = '$function$';
  const start = source.indexOf(delimiter, from) + delimiter.length;
  const end = source.indexOf(delimiter, start);
  assert.ok(end > start);
  return source.slice(start, end);
}
test('Import Review catalogue changes only classification materialization, not decisions or limits', () => {
  const current = body(replacement);
  assert.equal(current.split('), classified as materialized (').length, 2);
  assert.equal(current.replace('), classified as materialized (', '), classified as ('), body(original));
  assert.match(current, /order by r.id limit 501/);
  assert.match(current, /p_max_actions>5000/);
  assert.doesNotMatch(current, /plan_cache_mode|statement_timeout/);
});
test('replacement retains exact internal signature, security, owner and browser/service denial', () => {
  assert.match(replacement, /p_import_id uuid, p_preview_generation integer, p_max_actions integer DEFAULT 5000/);
  assert.match(replacement, /SECURITY DEFINER\n SET search_path TO 'public', 'extensions', 'pg_temp'/);
  assert.match(replacement, /alter function public\._import_review_action_catalog_core_v1\(uuid,integer,integer\) owner to postgres;/);
  assert.match(replacement, /revoke all on function public\._import_review_action_catalog_core_v1\(uuid,integer,integer\)\n  from public,anon,authenticated,service_role;/);
  assert.match(replacement, /notify pgrst, 'reload schema';\ncommit;/);
});
test('mandatory complete Mode A verifier exercises generic plans inside its rollback transaction', () => {
  const verifier = read('supabase/verification/17092026_0800_weekly_source_mode_a_dispatch_v1.sql');
  assert.match(verifier, /begin;[\s\S]*?set local plan_cache_mode=force_generic_plan;/);
  assert.match(verifier, /rollback;\s*$/);
  assert.doesNotMatch(replacement, /set(?: local)? plan_cache_mode/i);
});
