import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {repoRoot} from '../../scripts/cloudtms-db-release-lib.mjs';

const sql = readFileSync(path.join(repoRoot,
  'supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql'), 'utf8');
const hooks = [...sql.matchAll(/create or replace function private\.weekly_source_manual_reviews_finalised_v1\([\s\S]*?\$function\$;/gi)];
assert.equal(hooks.length, 1);
const hook = hooks[0][0];

test('Final remedy reads the immutable opening report, not the current display', () => {
  assert.match(hook, /if v_anchor\.opening_final_revision_id is not null then\s+select f\.upload_id into v_opening_upload_id from public\.weekly_source_final_revisions f\s+where f\.id=v_anchor\.opening_final_revision_id;\s+else\s+select p\.upload_id into v_opening_upload_id from public\.weekly_source_projection_publications p\s+where p\.id=v_anchor\.opening_projection_publication_id;/);
  assert.match(hook, /if v_opening_upload_id is null or v_opening_upload_id=v_final\.upload_id then continue; end if;/);
  assert(hook.indexOf('v_opening_upload_id=v_final.upload_id') < hook.indexOf("set state='RESOLVED'"));
});

test('existing durable shift, client manifest and current Final authority guards remain', () => {
  assert.match(hook, /v_final\.state is distinct from 'CURRENT'/);
  assert.match(hook, /current_final_revision_id is distinct from v_final\.id/);
  assert.match(hook, /r\.work_event_id=s\.work_event_id and r\.state='OPEN'/);
  assert.match(hook, /r\.candidate_id=s\.candidate_id and r\.client_id=s\.client_id/);
  assert.match(hook, /r\.contract_id=s\.contract_id and r\.work_date=s\.work_date/);
  assert.match(hook, /if not found or v_anchor\.opening_final_revision_id=p_final_revision_id/);
  assert.match(hook, /weekly_source_client_manifests m where m\.final_revision_id=v_final\.id/);
  assert.doesNotMatch(hook, /update public\.(?:timesheets_financials|weekly_source_entitlement_heads)|insert into private\.bpay_next_/);
});

test('private finalisation hook does not become a browser or service command', () => {
  assert.match(sql, /revoke all on function private\.weekly_source_manual_reviews_finalised_v1\(uuid,uuid\)\s+from public,anon,authenticated,service_role;/);
  assert.doesNotMatch(sql, /grant execute on function private\.weekly_source_manual_reviews_finalised_v1/);
});

test('native proof uses the actual Final owner and past cutoffs, not a fabricated result', () => {
  const runner = readFileSync(path.join(repoRoot, 'tests/run-bp-source-manual-query-native.mjs'), 'utf8');
  const fixture = readFileSync(path.join(repoRoot, 'tests/fixtures/bp-source-manual-query-final-report-boundary.sql'), 'utf8');
  assert.match(runner, /one actual integrated Final owner/);
  assert.match(runner, /the exact actual Final caller owns query resolution/);
  assert.match(fixture, /public\.weekly_source_finalise_atomic_v1\(v_request\)/);
  assert.match(fixture, /later actually finalised report clears contained shift even with unchanged hours/);
  assert.match(fixture, /finalised report containing only another shift cannot clear this query/);
  assert.match(fixture, /finalising the already-open report must NOT clear its manual query/);
  assert.match(fixture, /later contained-target Final resolves Final-anchored query/);
  assert.doesNotMatch(fixture, /insert into public\.weekly_source_final_revisions|perform private\.weekly_source_manual_reviews_finalised_v1|update public\.weekly_source_(?:cycles|report_scopes)/);
});
