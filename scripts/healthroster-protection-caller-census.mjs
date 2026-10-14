import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { createHash } from 'node:crypto';

const root = resolve(import.meta.dirname, '..');
const controlling = resolve(root, '../healthroster-v2-backend-13102026');
const hash = (value) => createHash('sha256').update(value).digest('hex');
const owners = [
  [root, 'supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql', 'public.weekly_source_candidate_check_materialise_atomic_v1', 'Workflow -> outreach generation -> Contract Week -> signature component -> Timesheet -> comparison cycle. Existing outer boundary remains an activation hold.'],
  [root, 'supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql', 'public.weekly_source_candidate_app_submit_atomic_v1', 'Outreach generation precedes materialise; no claim that inner Source family-first changes fix this outer lock.'],
  [root, 'supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql', 'public.weekly_source_timesheet_submission_complete_atomic_v1', 'Submission -> outreach generation -> membership. No direct comparator invocation in this complete function; materialise owns its comparator call separately.'],
  [root, 'supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql', 'public.candidate_submission_finalize_atomic_v1', 'Workflow -> approved request -> Daily Timesheet/current financial, or Contract/Contract Week -> canonical save/process -> authorise. Complete nested writer/trigger graph is not certified.'],
  [controlling, 'supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql', 'public.weekly_exceptional_pay_complete_local_v1', 'Current PP successor preserves native Candidate gate before complete I-1 then original Source locks; authorised publisher reenters the same gate/I-1. Banking external entry/trigger graph remains a joined gate.'],
  [root, 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/26_bulk_command.sqlinc', 'public.weekly_source_protect_all_signed_atomic_v1', 'One real shift per round: every Candidate gate first -> full I-1 -> group UPDATE -> cycle UPDATE -> native complete_local savepoint; partial command/continuation is explicit.'],
  [root, 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc', 'public.weekly_source_automatic_reconcile_atomic_v1', 'COMPLETE Candidate gate -> full I-1 -> group UPDATE -> cycle UPDATE -> root complete-component assertion -> native complete_local. Stage assertion/refusal inside the stage transaction is separate.'],
];
const census = { contract: 'HEALTHROSTER_PROTECTION_CALLER_BOUNDARY_CENSUS_V1',
  status: 'AUTOMATED_SOURCE_LOCATIONS_WITH_SCOPED_REVIEW_NOT_JOINED_LOCK_OR_RUNTIME_PROOF',
  banking_worktree_reads: false,
  owners: owners.map(([directory, path, name, qualification]) => {
    const bytes = readFileSync(resolve(directory, path)); const text = bytes.toString('utf8').replaceAll('\r\n', '\n');
    const start = text.indexOf(`create or replace function ${name}(`);
    const end = text.indexOf('$function$;', start);
    if (start < 0 || end < start) throw new Error(`Owner missing ${name}`);
    const body = text.slice(start, end + '$function$;'.length);
    const firstLine = text.slice(0, start).split('\n').length;
    return { function: name, source_worktree: directory === controlling ? 'healthroster-v2-backend-13102026 (READ ONLY)' : 'healthroster-v2-protection-build-13102026',
      path, file_sha256: hash(bytes), body_sha256_lf: hash(body), qualification,
      lock_and_native_owner_sites: body.split('\n').flatMap((line, index, lines) => {
        if (line.trimStart().startsWith('--') || !/for (update|share)|pg_(?:try_)?advisory|weekly_source_candidate_serial_gate_v1|weekly_source_lock_(?:family_rows|and_resolve_families)_v1|candidate_submission_compare_sync_v1|weekly_exceptional_pay_complete_local_v1/i.test(line)) return [];
        return [{ source_line: firstLine + index, source: lines.slice(Math.max(0,index-2),index+1).join('\n').trim() }];
      }) };
  }),
  remaining_joined_gates: ['Candidate outer workflow/Timesheet/Contract Week and signed materialise entry',
    'Root complete projection publisher with early signed-family fence', 'Mode A and D10 comparator callers',
    'Native Banking external entry, complete nested bounded writers/triggers', 'Client/group save and correction callers',
    'Native module/inventory operation/GUC cleanup', 'Real two-session tests in both winner orders'],
};
writeFileSync(resolve(root, 'docs/healthroster-build/PROTECTION_CALLER_BOUNDARY_CENSUS.json'), `${JSON.stringify(census, null, 2)}\n`);
process.stdout.write(`Protection caller census: ${census.owners.length} scoped owners; no runtime acceptance.\n`);
