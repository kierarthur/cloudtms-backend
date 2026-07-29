import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const sql = fs.readFileSync(
  path.join(root, 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql'),
  'utf8'
);

const start = sql.indexOf(
  'CREATE OR REPLACE FUNCTION public.pay_candidate_advances_report('
);
assert.notEqual(start, -1);
const end = sql.indexOf('\n$function$;', start);
assert.ok(end > start);
const functionBody = sql.slice(start, end + '\n$function$;'.length);
const securityTail = sql.slice(end + '\n$function$;'.length, end + 1200);

test('candidate finance report casts enum columns before text fallbacks and unions', () => {
  assert.match(functionBody, /vfc\.status::text as status/);
  assert.match(functionBody, /vfc\.payout_status::text as payout_status/);
  assert.match(functionBody, /vfc\.reason::text as reason/);
  assert.match(functionBody, /coalesce\(vfc\.status::text,\s*''\)/);
  assert.match(functionBody, /coalesce\(vfc\.payout_status::text,\s*''\)/);
  assert.doesNotMatch(functionBody, /coalesce\(vfc\.status,\s*''\)/);
  assert.doesNotMatch(functionBody, /coalesce\(vfc\.payout_status,\s*''\)/);
  assert.doesNotMatch(functionBody, /\bvfc\.reason\s*,/);
});

test('candidate finance report remains read-only and Policy X neutral', () => {
  assert.match(functionBody, /\bSTABLE\b/);
  assert.match(functionBody, /from public\.v_finance_cases_register vfc/);
  assert.doesNotMatch(
    functionBody,
    /\b(?:insert\s+into|update\s+public\.|delete\s+from|alter\s+table|drop\s+table)\b/i
  );
  assert.doesNotMatch(
    functionBody,
    /pay_workbench_prepare_draft|pay_batch_execute|provider_submission|settlement_apply/i
  );
});

test('candidate finance report is callable only through the Worker service role', () => {
  assert.match(
    securityTail,
    /REVOKE ALL ON FUNCTION public\.pay_candidate_advances_report\(uuid, uuid\) FROM PUBLIC/
  );
  assert.match(
    securityTail,
    /REVOKE ALL ON FUNCTION public\.pay_candidate_advances_report\(uuid, uuid\) FROM anon/
  );
  assert.match(
    securityTail,
    /REVOKE ALL ON FUNCTION public\.pay_candidate_advances_report\(uuid, uuid\) FROM authenticated/
  );
  assert.match(
    securityTail,
    /GRANT EXECUTE ON FUNCTION public\.pay_candidate_advances_report\(uuid, uuid\) TO service_role/
  );
});
