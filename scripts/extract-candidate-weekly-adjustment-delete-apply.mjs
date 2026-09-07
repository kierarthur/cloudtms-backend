import fs from 'node:fs';

const sourcePath = 'supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql';
const source = fs.readFileSync(sourcePath, 'utf8');
const startMarker =
  'DROP FUNCTION IF EXISTS public.timesheet_weekly_manual_adjustment_delete_apply(uuid, uuid);';
const endMarker =
  'REVOKE ALL ON FUNCTION public.timesheet_weekly_manual_adjustment_delete_apply(uuid, uuid, uuid[], uuid[], uuid[], uuid[], text) FROM PUBLIC;';

const start = source.indexOf(startMarker);
const end = source.indexOf(endMarker, start);

if (start < 0 || end < 0) {
  throw new Error('Candidate weekly adjustment delete apply source boundary is missing');
}
if (source.indexOf(startMarker, start + startMarker.length) >= 0) {
  throw new Error('Candidate weekly adjustment delete apply source boundary is ambiguous');
}

const sql = source.slice(start, end);
if (
  !sql.includes('CREATE OR REPLACE FUNCTION public.timesheet_weekly_manual_adjustment_delete_apply(') ||
  !sql.includes('p_expected_timesheet_ids uuid[]') ||
  !sql.includes('p_expected_row_signature text') ||
  !sql.trimEnd().endsWith('$function$;')
) {
  throw new Error('Candidate weekly adjustment delete apply source shape changed');
}

process.stdout.write(sql);
