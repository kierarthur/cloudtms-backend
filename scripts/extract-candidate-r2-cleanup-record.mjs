import fs from 'node:fs';

const sourcePath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const source = fs.readFileSync(sourcePath, 'utf8');
const startMarker =
  'CREATE OR REPLACE FUNCTION public.timesheet_r2_cleanup_record_v1(';
const endMarker =
  'REVOKE ALL ON FUNCTION public.timesheet_r2_cleanup_record_v1(text, uuid, uuid[], jsonb, uuid) FROM PUBLIC;';

const start = source.indexOf(startMarker);
const end = source.indexOf(endMarker, start);

if (start < 0 || end < 0) {
  throw new Error('Candidate R2 cleanup source boundary is missing');
}
if (source.indexOf(startMarker, start + startMarker.length) >= 0) {
  throw new Error('Candidate R2 cleanup source boundary is ambiguous');
}

const sql = source.slice(start, end);
if (
  !sql.includes('p_delete_operation_id text') ||
  !sql.includes('p_failures jsonb') ||
  !sql.includes('p_claim_token uuid DEFAULT NULL::uuid') ||
  !sql.trimEnd().endsWith('$function$;')
) {
  throw new Error('Candidate R2 cleanup source shape changed');
}

process.stdout.write(sql);
