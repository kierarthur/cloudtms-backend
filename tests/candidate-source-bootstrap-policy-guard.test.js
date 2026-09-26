import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const source = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'supabase', 'repeatable',
  '04092026_1603_candidate_expense_email_admission_v1.sql'), 'utf8');
const candidate = source.match(/create or replace function private\._candidate_record_capabilities_v1\([\s\S]*?\n\$function\$;/i)?.[0];

test('Candidate bootstrap projects a missing source policy as unavailable without broad error suppression', () => {
  assert.ok(candidate);
  assert.equal((candidate.match(/private\._weekly_source_effective_policy_v1\(/g) ?? []).length, 1);
  assert.match(candidate, /when sqlstate '55000' then[\s\S]*WEEKLY_SOURCE_GROUP_CARDINALITY_INVALID[\s\S]*then raise; end if;/);
  assert.match(candidate, /when sqlstate '22023' then[\s\S]*WEEKLY_SOURCE_POLICY_SCOPE_NOT_FOUND[\s\S]*then raise; end if;/);
  assert.doesNotMatch(candidate, /when others then/i);
  assert.match(candidate, /v_source_self_allowed:=coalesce\([\s\S]*'SOURCE_AUTHORITY'[\s\S]*'CHECK_ONLY',false\s*\)/);
});
