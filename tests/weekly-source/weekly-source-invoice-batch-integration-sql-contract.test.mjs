import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const owner = read('supabase/repeatable/15092026_1534_weekly_source_invoice_batch_integration_v1.sql');
const admissionOwner = read('supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql');
const ordinaryClassifier = read('supabase/repeatable/27072026_1042_invoice_async_v8/27072026_1806_private_invoice_batch_generate_classification_v2.sql');
const isolatedOrdinaryClassifier = read('supabase/repeatable/15092026_1534_weekly_source_invoice_batch_ordinary_isolation_v1.sql');
const verifier = read('supabase/verification/15092026_1534_weekly_source_invoice_batch_integration_v1.sql');
const broker = read('broker/src/invoice-async-http.js');
const index = read('broker/src/index.js');

const sliceBetween = (source, start, end) => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return source.slice(from, to);
};

const candidates = sliceBetween(
  owner,
  'create or replace function public.weekly_source_invoice_batch_candidates_v1(',
  'create or replace function public.weekly_source_invoice_batch_admit_atomic_v1(',
);
const admitBatch = sliceBetween(
  owner,
  'create or replace function public.weekly_source_invoice_batch_admit_atomic_v1(',
  'create or replace function public.weekly_source_invoice_edit_context_v1(',
);
const editContext = sliceBetween(
  owner,
  'create or replace function public.weekly_source_invoice_edit_context_v1(',
  'alter function private.weekly_source_invoice_batch_snapshot_v1()',
);
const confirm = sliceBetween(
  broker,
  'async function handleBatchGenerateConfirm(',
  'async function handleBatchIssueConfirm(',
);

test('batch candidate scope is final-source manifests with immutable unplaced movements only', () => {
  assert.match(owner, /manifest\.invoice_state='READY'/);
  assert.match(owner, /revision\.state='CURRENT'/);
  assert.match(owner, /completion\.completion_kind='FINAL_SOURCE'/);
  assert.match(owner, /movement\.placement_state='UNPLACED'/);
  assert.match(owner, /binding\.id is null/);
  assert.match(owner, /manifest\.movement_count>0/);
  assert.doesNotMatch(
    sliceBetween(
      owner,
      'create or replace function private.weekly_source_invoice_batch_snapshot_v1()',
      'create or replace function public.weekly_source_invoice_batch_admit_atomic_v1(',
    ),
    /protected|exceptional|workbench|banking|pay_batch|timesheets_financials/i,
  );
  assert.match(candidates, /'report_number',row_value\.backing_report_number/);
  assert.match(candidates, /RELEASED_AFTER_DISPUTE/);
  assert.match(candidates, /v_page_size not between 1 and 5000/);
  assert.match(candidates, /WEEKLY_SOURCE_INVOICE_BATCH_SCOPE_TOO_LARGE/);
});

test('source admissions are one atomic wrapper over the established guarded invoice admission', () => {
  assert.match(admitBatch, /public\.weekly_source_invoice_admit_atomic_v1/);
  assert.match(admitBatch, /for v_ref in select item from pg_catalog\.jsonb_array_elements\(v_refs\)/);
  assert.match(admitBatch, /for update/);
  assert.match(admitBatch, /'atomic',true/);
  assert.match(admitBatch, /'per_manifest_results',v_results/);
  assert.match(admitBatch, /WEEKLY_SOURCE_INVOICE_CYCLE_CONSOLIDATION_REFUSED/);
  assert.match(admitBatch, /left_result->>'source_cycle_id'<>right_result->>'source_cycle_id'/);
  assert.doesNotMatch(admitBatch, /invoice_operation_start_batch|GENERATE_SELECTED/);
  assert.doesNotMatch(admitBatch, /protected|exceptional|workbench|banking|pay_batch/i);
});

test('mixed selection validates both sources before either ordinary or source creation', () => {
  const mixedStart = confirm.indexOf('const ordinaryPreflightQuery =');
  const mixed = confirm.slice(mixedStart);
  const ordinaryPreflightAt = mixed.indexOf("await deps.rpc('invoice_batch_generate_candidates'");
  const sourcePreflightAt = mixed.indexOf('await preflightWeeklySourceInvoiceBatch(');
  const sourceAdmissionAt = mixed.indexOf('await admitWeeklySourceInvoiceBatch(');
  const ordinaryStartAt = mixed.lastIndexOf('return await startCommands(');
  assert.ok(ordinaryPreflightAt >= 0);
  assert.ok(sourcePreflightAt > ordinaryPreflightAt);
  assert.ok(sourceAdmissionAt > sourcePreflightAt);
  assert.ok(ordinaryStartAt > sourceAdmissionAt);
  assert.match(confirm, /if \(!weeklySourceContract\) \{[\s\S]*return startCommands/,
    'source-free ordinary confirmation must retain its legacy route');
  assert.match(confirm, /partial: true[\s\S]*weekly_source_per_row_results[\s\S]*\}, 207\)/,
    'a later ordinary start failure must report the committed source rows exactly');
});

test('movement projection exposes only compatible idle unissued source invoices', () => {
  assert.match(
    editContext,
    /\) order by destination_manifest\.finalisation_week_ending desc,destination\.invoice_no,destination\.id\)\s+from public\.invoices destination/,
    'the aggregate must close before FROM while the enclosing coalesce owns the empty-array fallback',
  );
  assert.match(editContext, /destination\.client_id=v_invoice\.client_id/);
  assert.match(editContext, /destination\.status='DRAFT'/);
  assert.match(editContext, /destination\.issued_at_utc is null/);
  assert.match(editContext, /destination\.paid_at_utc is null/);
  assert.doesNotMatch(editContext, /'cross_cycle'|destination_manifest\.source_group_id=v_manifest\.source_group_id/);
  assert.doesNotMatch(admissionOwner, /WEEKLY_SOURCE_INVOICE_MOVE_DIFFERENT_WEEK_CONFIRMATION_REQUIRED|confirm_different_finalised_week/);
  assert.match(admissionOwner, /WEEKLY_SOURCE_INVOICE_MOVE_PARTIAL_LINE_REFUSED/);
});

test('issue, render and export remain tied to frozen source invoice lines and report identity', () => {
  assert.match(admissionOwner, /weekly_source_invoice_issue_guard_v1/);
  assert.match(admissionOwner, /weekly_source_invoice_allocation_assert_v1/);
  assert.match(admissionOwner, /WEEKLY_SOURCE_INVOICE_ALLOCATION_MISMATCH/);
  assert.match(index, /weekly_source_invoice_edit_context_v1/);
  assert.match(index, /backing_report_numbers/);
  assert.match(index, /Backing report/);
  assert.doesNotMatch(owner, /insert into public\.invoice_lines/i,
    'the batch wrapper must delegate line freezing to the established admission owner');
});

test('new public RPCs are service-only and do not expose an ordinary browser mutation route', () => {
  for (const signature of [
    'public.weekly_source_invoice_batch_candidates_v1(jsonb)',
    'public.weekly_source_invoice_batch_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_edit_context_v1(jsonb)',
  ]) {
    const escaped = signature.replaceAll('(', '\\(').replaceAll(')', '\\)');
    assert.match(owner, new RegExp(`revoke all on function ${escaped} from public,anon,authenticated;`, 'i'));
    assert.match(owner, new RegExp(`grant execute on function ${escaped} to service_role;`, 'i'));
  }
});

test('ordinary generation excludes source-owned roots without changing any other classifier logic', () => {
  const header = `-- Keep the established ordinary batch classifier byte-for-byte equivalent to
-- its v2 authority, except that a Timesheet owned by immutable Weekly-source
-- lineage can only be invoiced from its final-source Client manifest.  This
-- replacement must be installed after weekly_source_row_timesheet_lineages.
`;
  // Reverse only the two reviewed source-ownership CTEs. Every downstream
  // classifier byte, and the original ordinary candidate facts, must agree.
  const ordinaryCte = ordinaryClassifier.slice(ordinaryClassifier.indexOf('  source_candidates as materialized ('),
    ordinaryClassifier.indexOf('  command as materialized ('));
  const changedStart=isolatedOrdinaryClassifier.indexOf('  source_candidate_families as materialized (');
  const changedEnd=isolatedOrdinaryClassifier.indexOf('  command as materialized (',changedStart);
  assert.ok(changedStart>0 && changedEnd>changedStart);
  const familyCte=isolatedOrdinaryClassifier.slice(changedStart,
    isolatedOrdinaryClassifier.indexOf('  source_candidates as materialized (',changedStart));
  assert.equal(familyCte.replace('source_candidate_families','source_candidates')
    .replace('financial.timesheet_id,\n      private.weekly_source_invoice_family_timesheet_ids_v1(financial.timesheet_id) family_ids',
      'financial.timesheet_id').replace('      and financial.client_id is not null\n',
      '      and financial.client_id is not null\n    order by financial.timesheet_id\n'),ordinaryCte);
  assert.ok(isolatedOrdinaryClassifier.startsWith(header));
  assert.match(isolatedOrdinaryClassifier, /weekly_source_row_timesheet_lineages lineage/);
  // the family is resolved through the one installed resolver adapter, and the
  // bare physical-id predicate is gone
  assert.match(
    isolatedOrdinaryClassifier,
    /where lineage\.timesheet_id=any\(candidate\.family_ids\)/,
  );
  assert.match(isolatedOrdinaryClassifier,/family\.root_timesheet_id=any\(candidate\.family_ids\)\s+and family\.ownership_state='TARGET_MANAGED'/);
  assert.equal((isolatedOrdinaryClassifier.match(/private\.weekly_source_invoice_family_timesheet_ids_v1\(/g)||[]).length,1);
  assert.doesNotMatch(
    isolatedOrdinaryClassifier,
    /where lineage\.timesheet_id=financial\.timesheet_id/,
  );
  const restored = isolatedOrdinaryClassifier
    .slice(header.length,changedStart)+ordinaryCte+isolatedOrdinaryClassifier.slice(changedEnd);
  assert.equal(restored, ordinaryClassifier,
    'only the reviewed Source ownership discovery CTEs may differ');
});

test('runtime catalogue proof recognizes the family-keyed lineage exclusion independent of SQL whitespace', () => {
  // WP-27: the installed-definition proof follows the fix.  The old assertion
  // required the physical-id text and is superseded.
  assert.match(
    verifier,
    /not like '%lineage\.timesheet_id=any\(candidate\.family_ids\)%'/i,
  );
  assert.match(
    verifier,
    /pg_catalog\.regexp_replace\(v_definition,'\[\[:space:\]\]\+','','g'\)\s+like '%lineage\.timesheet_id=financial\.timesheet_id%'/i,
  );
});

test('the isolation verifier executes a rotated lineage-bound family, it does not only read the text', () => {
  assert.match(verifier, /weekly_source_invoice_family_timesheet_ids_v1/);
  assert.match(verifier, /rotated lineage-bound root/i);
  assert.match(verifier, /_invoice_batch_generate_classification_v2\(/);
  assert.match(verifier, /\\ir 05102026_0552_weekly_source_invoice_isolation_fixture_v1\.sql/);
  assert.doesNotMatch(verifier, /\\ir 15092026_1534_weekly_source_ordinary_pay_projection_v1\.sql/);
  const fixture = read('supabase/verification/05102026_0552_weekly_source_invoice_isolation_fixture_v1.sql');
  assert.match(fixture, /weekly_source_import_prepare_atomic_v1/);
  assert.match(fixture, /weekly_source_finalise_atomic_v1/);
  assert.match(fixture, /weekly_source_ordinary_pay_projection_apply_atomic_v1/);
  assert.match(fixture, /financial\.total_hours=7\.50/);
  assert.match(fixture, /first preparation, not Authorise, paid state or TARGET ownership/);
  assert.doesNotMatch(fixture, /paid_at_utc\s*=|insert into public\.weekly_source_root_authorisations|projection-a6-paid-refusal/i);
});
