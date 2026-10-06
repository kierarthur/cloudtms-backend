import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import path from 'node:path';
import {repoRoot} from '../../scripts/cloudtms-db-release-lib.mjs';

const sql=readFileSync(path.join(repoRoot,
  'supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql'),'utf8');
const matches=[...sql.matchAll(/do \$manual_review\$[\s\S]*?\$manual_review\$;/g)];
assert.equal(matches.length,1);
const fixture=matches[0][0];

test('all four distinct Office decisions use V2 command identities',()=>{
  const commands=[...fixture.matchAll(/'command_id','([^']+)'/g)].map(m=>m[1]);
  assert.equal(commands.length,4);
  assert.equal(new Set(commands).size,4);
  for(const command of commands) assert.match(command,/^ff000000-0000-4000-8000-00000000000[1-4]$/);
  assert.match(fixture,/v_open:=public\.weekly_source_manual_review_open_v1\(v_request\)/);
  assert.match(fixture,/'reason','Same review again',\s*'command_id','ff000000-0000-4000-8000-000000000002'/);
  assert.match(fixture,/'expected_current_row_hash',repeat\('bd',32\),\s*'command_id','ff000000-0000-4000-8000-000000000003'/);
  assert.match(fixture,/'reason','Office is reconsidering protected pay',\s*'command_id','ff000000-0000-4000-8000-000000000004'/);
});

test('exact retry and changed-payload collision are proved separately from existing-open deduplication',()=>{
  assert.match(fixture,/v_replay:=public\.weekly_source_manual_review_open_v1\(v_request\)/);
  assert.match(fixture,/v_replay=v_open\|\|jsonb_build_object\('idempotent_replay',true\)/);
  assert.match(fixture,/v_request\|\|jsonb_build_object\('reason','Changed reason on the same command'\)/);
  assert.match(fixture,/exception when unique_violation then[\s\S]*?v_code='WEEKLY_SOURCE_MANUAL_REVIEW_COMMAND_COLLISION'/);
  assert.match(fixture,/\(v_open->>'already_open'\)::boolean[\s\S]*?\(v_open->>'review_id'\)::uuid=v_review/);
});

test('current source, cutoff membership, Office checks and real rollback boundaries remain mandatory',()=>{
  assert.match(fixture,/'manual review did not resolve against the current source hash'/);
  assert.match(fixture,/set valid_from='2026-09-10'/);
  assert.match(fixture,/review->>'pay_blocking'='true'/);
  assert.match(fixture,/manual pay review remained in Hours questions/);
  assert.match(fixture,/the source-file action did not hide a shift already in Pay Queries/);
  assert.match(fixture,/\(v_open->>'review_id'\)::uuid<>v_review/);
  assert.doesNotMatch(fixture,/create or replace function|alter function|grant execute|weekly_source_pay_query_admit_v2/);
  assert.match(sql,/rollback;\s*do \$fresh\$/);
  assert.match(sql,/rollback did not remove verification fixtures/);
});
