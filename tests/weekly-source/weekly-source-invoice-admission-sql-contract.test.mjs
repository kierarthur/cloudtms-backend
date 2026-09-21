import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { inventory } from '../../scripts/cloudtms-db-release-lib.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(here, '..', '..');
const read = relativePath => fs.readFileSync(path.join(root, relativePath), 'utf8')
  .replaceAll('\r\n', '\n');

const schema = read('supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql');
const classifierPath = 'supabase/repeatable/15092026_1534_00_weekly_source_private_classifiers_v1.sql';
const classifiers = read(classifierPath);
const classifierVerifier = read('supabase/verification/15092026_1534_00_weekly_source_private_classifiers_v1.sql');
const owner = read('supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql');
const verifier = read('supabase/verification/15092026_1534_weekly_source_invoice_admission_v1.sql');
const issueValidator = read('supabase/repeatable/02092026_1833_weekly_source_invoice_issue_validator_v1.sql');
const candidateRuntimeWorkflow = read('.github/workflows/candidate-db-runtime.yml');
const correctionOwner = read(
  'supabase/repeatable/23072026_2207_invoice_queue_stage1_revision8/'
  + '23072026_2207_private_invoice_correction_validate_batch.sql',
);

const sliceBetween = (source, start, end) => {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  assert.notEqual(from, -1, `missing start marker: ${start}`);
  assert.notEqual(to, -1, `missing end marker: ${end}`);
  return source.slice(from, to);
};

const admit = sliceBetween(
  owner,
  'create or replace function public.weekly_source_invoice_admit_atomic_v1(',
  'create or replace function public.weekly_source_invoice_move_atomic_v1(',
);
const move = sliceBetween(
  owner,
  'create or replace function public.weekly_source_invoice_move_atomic_v1(',
  'alter function private.weekly_source_invoice_movement_only_integrity_v1(uuid)',
);

test('shared Weekly Source classifiers precede their same-minute dependants in release inventory', () => {
  const ordered = inventory().repeatables.map((entry) => entry.path);
  const classifierIndex = ordered.indexOf(classifierPath);
  assert.notEqual(classifierIndex, -1);
  for (const dependant of [
    'supabase/repeatable/15092026_1534_weekly_source_finalisation_v1.sql',
    'supabase/repeatable/15092026_1534_weekly_source_invoice_admission_v1.sql',
  ]) {
    const dependantIndex = ordered.indexOf(dependant);
    assert.notEqual(dependantIndex, -1);
    assert.ok(classifierIndex < dependantIndex, `${classifierPath} must precede ${dependant}`);
  }
  assert.match(classifierVerifier, /namespace_row\.nspname='private'/);
  for (const classifier of [
    'weekly_source_sha256_text_v1',
    'weekly_source_sha256_jsonb_v1',
    'weekly_source_breaks_equivalent_v1',
    'weekly_source_scope_fingerprint_v1',
    'weekly_source_office_authority_v1',
  ]) {
    assert.match(classifierVerifier, new RegExp(`'${classifier}'`));
  }
  assert.doesNotMatch(classifierVerifier, /procedure_row\.proname like 'weekly_source%'/);
});

test('fresh Candidate runtime installs the source invoice validator before its consumer', () => {
  const validatorIndex = candidateRuntimeWorkflow.indexOf(
    'supabase/repeatable/02092026_1833_weekly_source_invoice_issue_validator_v1.sql',
  );
  const consumerIndex = candidateRuntimeWorkflow.indexOf(
    'supabase/repeatable/02092026_1834_candidate_expense_separation_delivery_v1.sql',
  );
  assert.notEqual(validatorIndex, -1);
  assert.notEqual(consumerIndex, -1);
  assert.ok(validatorIndex < consumerIndex);
});

test('self-bill invoice orchestration exposes only its two service-role RPCs', () => {
  assert.equal(
    (owner.match(/create or replace function public\.weekly_source_invoice_(?:admit|move)_atomic_v1\(\s*p_request jsonb\s*\)/gi) || []).length,
    2,
  );
  for (const signature of [
    'public.weekly_source_invoice_admit_atomic_v1(jsonb)',
    'public.weekly_source_invoice_move_atomic_v1(jsonb)',
  ]) {
    const escaped = signature.replaceAll('(', '\\(').replaceAll(')', '\\)');
    assert.match(owner, new RegExp(`revoke all on function ${escaped}\\s+from public,anon,authenticated;`, 'i'));
    assert.match(owner, new RegExp(`grant execute on function ${escaped}\\s+to service_role;`, 'i'));
  }
  assert.match(owner, /notify pgrst, 'reload schema';/i);
  assert.doesNotMatch(owner, /\bMAGNIT\b/i);
  assert.match(classifiers, /'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE'/);
  assert.match(classifiers, /v_operation<>'OPEN_QUERY_NOTICE'[\s\S]*v_actor\.role[\s\S]*<>'admin'/);
  assert.doesNotMatch(
    classifiers.match(/v_payment_operation:=v_operation in \([\s\S]*?\);/)?.[0] ?? '',
    /ADMIT_SOURCE_INVOICE|MOVE_SOURCE_INVOICE/,
  );
});

test('invoice eligibility is movement-only and a zero-source protected root is guarded', () => {
  assert.match(owner, /weekly_source_invoice_movement_only_integrity_v1/);
  assert.match(owner, /weekly_source_row_timesheet_lineages/);
  assert.match(owner, /ownership_state='TARGET_MANAGED'/);
  // Gate 7 item G7-5 (24 section 14; proof/34 section 9; ROT-008): invoice
  // lineage resolves through the Timesheet FAMILY, never through a bare
  // physical id.  The previous assertion required the bare id and is superseded.
  assert.match(owner, /create or replace function private\.weekly_source_invoice_family_timesheet_ids_v1\(/);
  // The Workbench resolver remains the one family normaliser. The seed set is
  // widened only to include a literal booking identity that differs solely by
  // surrounding whitespace, closing the proved split-family collision before
  // the complete set is passed to that resolver.
  assert.match(owner, /with canonical_split_siblings as \(/);
  assert.match(owner, /pg_catalog\.btrim\(sibling\.booking_id\)[\s\S]*pg_catalog\.btrim\(requested\.booking_id\)/);
  assert.match(owner, /public\._pay_timesheet_rotation_scope\([\s\S]*resolver_seeds/);
  // the family is resolved ONCE per evaluation, not once per predicate limb
  assert.match(owner, /with resolved_family as materialized \(\s*select private\.weekly_source_invoice_family_timesheet_ids_v1\(p_timesheet_id\) timesheet_ids/);
  assert.match(owner, /not exists\(\s*select 1 from public\.weekly_source_billing_movements movement\s*where movement\.invoice_timesheet_id=any\(resolved_family\.timesheet_ids\)/);
  // proof/34 section 9: reading the family never re-points a visible line.
  assert.doesNotMatch(owner, /update public\.invoice_lines[\s\S]{0,200}set[\s\S]{0,80}timesheet_id=/i);
  assert.match(owner, /coalesce\(v_owner,''\) not in \(\s*'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE'/);
  assert.match(admit, /from public\.weekly_source_manifest_movements mm/);
  assert.match(admit, /if v_manifest\.movement_count=0 then[\s\S]*'status','NO_MOVEMENTS'/);
  assert.doesNotMatch(admit, /weekly_exceptional_pay|timesheets_financials|authori[sz]ed|pay_batch|banking_pay/i);
  assert.match(verifier, /protected zero-source root integrity failed/);
  assert.match(verifier, /legacy invoice path admitted protected zero root/);
});

test('admission freezes exact manifest economics and preserves presentation policy boundaries', () => {
  assert.match(admit, /mm\.movement_hash is distinct from movement\.movement_economic_hash/);
  assert.match(admit, /WEEKLY_SOURCE_INVOICE_MANIFEST_INVALID/);
  assert.match(admit, /automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'/);
  assert.match(admit, /movement\.source_profile_kind<>'NHSP_TRUST_BACKING_REPORT'/);
  assert.match(admit, /NET_DIFFERENCE_PRESENTATION/);
  assert.match(admit, /v_is_net:=v_group_row\.requested_net and v_count=2/);
  assert.match(admit, /v_count=2 and not v_is_net/);
  assert.match(admit, /bool_and\(source_line_kind='SOURCE_FIXED_EXPENSE'\)/);
  assert.match(admit, /weekly_source_expense_materialisations/);
  assert.match(verifier, /FULL roster correction must expose separate reversal and replacement lines/);
  assert.match(verifier, /NHSP physical signed rows must remain separate and retain backing report number/);
  assert.match(admit, /source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense[\s\S]*'VALIDATED_SOURCE_PENCE'/);
  assert.match(schema, /origin_kind<>'SOURCE_FIXED_EXPENSE'[\s\S]*amount_authority='VALIDATED_SOURCE_PENCE'/);
  assert.match(verifier, /source-fixed expenses must preserve source-pence authority, equal pay\/charge, VAT off and zero-source reversal/);
  assert.match(verifier, /NET roster correction must share one line with exact replacement minus reversal arithmetic/);
  assert.match(verifier, /NET source-fixed expense must preserve exact delta and configured VAT-on arithmetic/);
  assert.match(verifier, /assert_illegal_rebind_rejected/);
  assert.match(verifier, /cross-correction-unit/);
  assert.match(verifier, /cross-cycle/);
  assert.match(verifier, /cross-Client/);
  assert.match(verifier, /duplicate current movement binding was accepted/);
});

test('the schema allows one exact NET pair while retaining immutable movement allocations', () => {
  assert.match(schema, /weekly_source_invoice_line_bindings_current_line_idx[\s\S]*where state='CURRENT'/);
  assert.doesNotMatch(schema, /unique\s*\(invoice_line_id,binding_version\)/i);
  assert.match(schema, /weekly_source_invoice_line_bindings_line_history_idx[\s\S]*invoice_line_id,binding_version,billing_movement_id/);
  assert.match(schema, /unique \(billing_movement_id,binding_version\)/);
  assert.match(schema, /expense_authority_generation_id uuid/);
  assert.match(schema, /EXPENSE_POSITIVE/);
  assert.match(schema, /SOURCE_FIXED_EXPENSE/);
  assert.match(schema, /create table public\.weekly_source_expense_materialisations/);
  assert.match(schema, /create table public\.weekly_source_expense_pay_materialisations/);
  assert.doesNotMatch(
    sliceBetween(
      schema,
      'create table public.weekly_source_expense_materialisations',
      'create table public.weekly_source_expense_pay_materialisations',
    ),
    /candidate_timesheet_financial_id/,
  );
});

test('source-expense invoice and pay facets are separate immutable identities', () => {
  assert.match(owner, /insert into public\.weekly_source_expense_materialisations/);
  assert.doesNotMatch(owner, /weekly_source_expense_pay_materialisations/);
  assert.match(verifier, /pay-first source expense must admit one separate invoice facet on the same authority and root/);
  // Gate 2 (WP-06 handoff N8): the ordinary pay projection owner no longer
  // refuses and no longer publishes.  The surviving safety property is that a
  // later projection on an invoice-bound root proposes and mutates nothing.
  assert.match(verifier, /invoice-first source root must not materialise a later mutable ordinary pay facet/);
  assert.match(verifier, /invoice-first source root must not publish a Candidate financial snapshot/);
  assert.match(verifier, /invoice-bound source root must propose and publish nothing/);
  assert.ok(!/outcome.{0,4}<>.{0,2}.REFUSED_LOCKED./.test(verifier), 'the verifier still asserts the deleted REFUSED_LOCKED outcome');
});

test('one presentation line moves, bounded to idle unissued invoices and exact CAS', () => {
  assert.match(move, /'MOVE_SOURCE_INVOICE'/);
  assert.match(move, /v_source\.client_id is distinct from v_destination\.client_id/);
  assert.doesNotMatch(move, /v_source_manifest\.source_group_id is distinct from v_destination_manifest\.source_group_id/);
  assert.match(move, /WEEKLY_SOURCE_INVOICE_MOVE_REQUIRES_IDLE_DRAFTS/);
  assert.doesNotMatch(move, /WEEKLY_SOURCE_INVOICE_MOVE_DIFFERENT_WEEK_CONFIRMATION_REQUIRED|confirm_different_finalised_week/);

  const moveComment = owner.match(
    /comment on function public\.weekly_source_invoice_move_atomic_v1\(jsonb\) is\s*'([^']+)';/i,
  )?.[1] ?? '';
  assert.match(moveComment, /two idle, unissued DRAFT self-bill invoices for the same Client/i);
  assert.match(moveComment, /no special cross-week confirmation applies/i);
  assert.doesNotMatch(moveComment, /same Client and source group|cross-cycle movement requires explicit confirmation/i);
  assert.doesNotMatch(owner, /cross-week gate below|commit without its confirmation/i);

  // 24 section 12: "The selected unit is one immutable source presentation
  // line, identified by presentation-line ID or by invoice-line ID plus its
  // expected presentation hash... Moving by whole work-event ID is prohibited
  // because it can move more than the Office selected."  25 section 8 Removed,
  // bullet 1.  The previous assertion required exactly the prohibited
  // work-event predicate and is superseded by Gate 7 item G7-1.
  assert.match(move, /'presentation_line_id'/);
  assert.match(move, /'expected_presentation_hash'/);
  assert.match(move, /'invoice_line_id'/);
  assert.match(move, /WEEKLY_SOURCE_INVOICE_MOVE_LINE_IDENTITY_REQUIRED/);
  assert.match(move, /WEEKLY_SOURCE_INVOICE_MOVE_PRESENTATION_HASH_MISMATCH/);
  assert.match(move, /WEEKLY_SOURCE_INVOICE_MOVE_COMPANION_REQUIRED/);
  assert.match(move, /companion_presentation_line_id/);
  assert.doesNotMatch(move, /v_source_shift_group_id/);
  assert.doesNotMatch(move, /movement\.work_event_id=/);

  assert.match(move, /WEEKLY_SOURCE_INVOICE_MOVE_PARTIAL_LINE_REFUSED/);
  assert.match(move, /get diagnostics v_updated_line_count=row_count/);
  assert.match(move, /v_updated_line_count is distinct from v_line_count/);
  assert.match(move, /state='SUPERSEDED'/);
  assert.match(move, /prior_binding_id/);
  assert.match(move, /prior_placement_id/);
  assert.match(move, /'idempotent',true/);
  assert.match(verifier, /presentation line was moved partially/);
  assert.match(verifier, /selecting one line moved more than one line/);
  assert.match(verifier, /the unselected NHSP physical row did not stay put/);
  assert.match(verifier, /work-event move request was accepted/);
  assert.match(verifier, /companion expense was moved on its own/);
  assert.match(verifier, /same-Client cross-source-group destination was not offered/);
  assert.match(verifier, /same-Client cross-source-group move did not carry its companion/);
});

test('issuing validates complete allocation and never reconstructs source economics', () => {
  assert.match(owner, /weekly_source_invoice_issue_guard_v1/);
  assert.match(owner, /weekly_source_invoice_allocation_assert_v1/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_EXTRA_LINE/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_HEADER_INVALID/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_EMPTY/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_ALLOCATION_MISMATCH/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_TOTAL_MISMATCH/);
  assert.match(owner, /WEEKLY_SOURCE_INVOICE_NET_CARDINALITY_INVALID/);
  assert.match(owner, /movement\.placement_state='ISSUED'/);
  assert.match(verifier, /issue did not mark every movement issued/);
  assert.match(verifier, /unissue did not restore movable placement state/);
  assert.match(verifier, /emptied source invoice accepted a non-source line/);

  // Gate 7 item G7-6 (gap row XSG-017; 25 section 4 Removed, bullet 1): "a
  // direct database status change to ISSUED" is not proof that the real issue
  // route accepts a source invoice.  The verifier step that wrote
  // status='ISSUED' was removed, so the assertion on its message is superseded;
  // issue proof now runs through the real direct and asynchronous owners.
  assert.match(verifier, /public\.invoice_issue_one\(/);
  assert.match(verifier, /public\.invoice_unissue_one\(/);
  assert.match(verifier, /private\._invoice_issue_validate_batch\(/);
  assert.match(verifier, /private\._invoice_batch_issue_classification_v2\(/);
  assert.match(verifier, /the real direct owner issued an empty source invoice/);
  assert.match(verifier, /the real async validator admitted an empty source invoice/);
  assert.match(verifier, /real direct issue owner refused the source invoice/);
  assert.match(verifier, /skippable ordinary evidence survived/);
  // The only remaining direct status writes are ON_HOLD -> DRAFT resets that
  // let the fixture continue after a deliberate refusal; none asserts an issue.
  assert.doesNotMatch(verifier, /set status='ISSUED'/);
});

test('the one-line move request is a strict value contract without a week category', () => {
  assert.doesNotMatch(move, /confirm_different_finalised_week|v_confirm_different_week|DIFFERENT_WEEK_CONFIRMATION/);
  for (const key of [
    'actor_user_id', 'source_invoice_id', 'destination_invoice_id',
    'expected_presentation_hash', 'reason',
  ]) {
    assert.match(
      move,
      new RegExp(`jsonb_typeof\\(p_request->'${key}'\\)[\\s\\S]{0,40}is distinct from 'string'`),
      `${key} is not type-gated`,
    );
  }
  assert.match(move, /jsonb_typeof\(p_request->'expected_source_document_revision'\)[\s\S]{0,40}is distinct from 'number'/);
  assert.match(move, /jsonb_typeof\(p_request->'expected_destination_document_revision'\)[\s\S]{0,40}is distinct from 'number'/);
  // no unguarded cast is left in the immutable-header checks of the move owner
  assert.doesNotMatch(move, /header_snapshot_json#>>'\{meta,self_bill\}'\)::boolean/);
  assert.match(move, /pg_input_is_valid\(\s*coalesce\(v_source\.header_snapshot_json#>>'\{meta,client_manifest_id\}',''\),'uuid'\)/);
});

test('the issue skip list is a closed enumeration, not a correction-code wildcard', () => {
  // WP-05 review F2.  "like 'INVOICE_CORRECTION\\_%'" skipped all 37 codes the
  // installed private._invoice_correction_validate_batch can emit, and every
  // future code in the family.  24 section 11 closes the list and 25 section 4
  // Removed bullet 3 adds exactly one rule.
  assert.doesNotMatch(issueValidator, /like 'INVOICE_CORRECTION/);
  const skippable = sliceBetween(
    issueValidator,
    'create or replace function private.weekly_source_invoice_issue_skippable_code_v1(',
    '$function$;',
  );
  // The quoted literals are the closed list itself; the two financial-record
  // codes appear in this slice only inside the comment that explains why they
  // were removed, so they are excluded from the parsed list.
  const listed = [...skippable.matchAll(/^\s+'(INVOICE_CORRECTION_[A-Z_]+)',?$/gm)]
    .map(m => m[1]).sort();
  assert.deepEqual(listed, [
    'INVOICE_CORRECTION_PAIR_PLACEMENT_INCOMPLETE',
    'INVOICE_CORRECTION_STREAM_MISMATCH',
    'INVOICE_CORRECTION_TARGET_STREAM_MISMATCH',
    'INVOICE_CORRECTION_UNIT_SPLIT_ACROSS_INVOICES',
  ]);
  // every correction code the installed owner can emit, minus the six above,
  // must be absent from the skip list
  const emitted = [...new Set(
    [...correctionOwner.matchAll(/'(INVOICE_CORRECTION_[A-Z_]+)'/g)].map(m => m[1]),
  )];
  assert.ok(emitted.length >= 37, `expected at least 37 correction codes, found ${emitted.length}`);
  for (const code of emitted) {
    if (listed.includes(code)) continue;
    assert.ok(
      !listed.includes(code),
      `${code} is skipped but is outside the closed 25 section 4 enumeration`,
    );
  }
  // the ordinary evidence families 24 section 11 names are still skipped
  for (const code of [
    'MISSING_TIMESHEET', 'MANUAL_TIMESHEET_SOURCE_MISSING', 'QR_TIMESHEET_UNSIGNED',
    'TIMESHEET_DOCUMENT_FAILED', 'MISSING_REFERENCE', 'MISSING_MILEAGE_EVIDENCE',
    'MISSING_EXPENSE_EVIDENCE', 'ASSET_NOT_REGISTERED', 'ASSET_WORKFLOW_MISSING',
    'REQUIRED_ASSET_FAILED', 'MISSING_IMPORT_SOURCE_EVIDENCE',
    'CORRECTION_LINES_NOT_UNIT_SAFE',
  ]) {
    assert.ok(skippable.includes(`'${code}'`), `${code} is no longer skipped`);
  }
  // and every ordinary control the review named as uncovered still blocks
  for (const code of [
    'INVOICE_CORRECTION_TSFIN_STALE', 'INVOICE_CORRECTION_FROZEN_POLICY_DRIFT',
    'INVOICE_CORRECTION_SOURCE_LOCK_CONFLICT', 'INVOICE_CORRECTION_SEGMENT_LOCK_CONFLICT',
    'INVOICE_CORRECTION_CHAIN_CYCLE', 'INVOICE_CORRECTION_CHAIN_DEPTH_EXCEEDED',
    'INVOICE_CORRECTION_CLIENT_MISMATCH', 'INVOICE_CORRECTION_CONTRACT_MISMATCH',
    'INVOICE_CORRECTION_WEEK_MISMATCH', 'INVOICE_CORRECTION_VAT_POLICY_MISMATCH',
    'INVOICE_CORRECTION_MEMBER_MISSING', 'INVOICE_CORRECTION_UNIT_INVALID',
  ]) {
    assert.ok(!listed.includes(code), `${code} is skipped`);
  }
});

test('a missing financial record is never skipped on the code text alone', () => {
  // WP-05c, HANDOVER 2 round 5 Part E.  STATIC assertion; the executed proof is
  // supabase/verification/02092026_1833_weekly_source_invoice_issue_validator_v1.sql,
  // block $tsfin_states$, which drives the three states through the installed
  // owner and the real asynchronous seam.
  const skippable = sliceBetween(
    issueValidator,
    'create or replace function private.weekly_source_invoice_issue_skippable_code_v1(',
    '$function$;',
  );
  const listed = [...skippable.matchAll(/^\s+'(INVOICE_CORRECTION_[A-Z_]+)',?$/gm)]
    .map(m => m[1]);
  for (const code of [
    'INVOICE_CORRECTION_TSFIN_MISSING',
    'INVOICE_CORRECTION_TSFIN_NOT_READY',
    'INVOICE_CORRECTION_TSFIN_STALE',
  ]) {
    assert.ok(!listed.includes(code), `${code} is still skipped with no evidence`);
  }
  // the evidence gate exists and takes the invoice
  assert.match(
    issueValidator,
    /create or replace function private\.weekly_source_invoice_issue_tsfin_skippable_v1\(\s*p_invoice_id uuid,\s*p_code text\s*\)/,
  );
  const gate = sliceBetween(
    issueValidator,
    'create or replace function private.weekly_source_invoice_issue_tsfin_skippable_v1(',
    '$function$;',
  );
  // WP-33.  This assertion used to be `!gate.includes('INVOICE_CORRECTION_TSFIN_STALE')`,
  // on the authority of HANDOVER 2 round 5 Part E ("Stale TSFIN always blocks"),
  // which HANDOVER 2 CORRECTION ADDENDUM R8A section 1 WITHDRAWS for a wholly
  // sealed source-backed self-bill.  R8A is later authority and is accepted by
  // the product owner, so the test changed and the gate did not.  Executed
  // measurement behind that decision: the installed predicate keys on
  // public.timesheets_financials.is_stale alone, reads no reason, and catches no
  // source-side staleness, so it was a Candidate-pay admission predicate, which
  // sealed 02_CONTROLLING_POLICY.md section 15 and annex row ISS-013 forbid.
  //
  // What the test asserts INSTEAD is strictly stronger, and it fails against the
  // pre-WP-33 gate: the three Candidate-pay codes are a CLOSED enumeration, and
  // the ONLY thing that can return true for them is a POSITIVE wholly-sealed
  // proof read three-valued.  Nothing here relaxes the rule this test is named
  // for - the codes are still absent from the TEXT-ONLY list asserted above, so
  // none of them is ever skipped on the code text alone.
  const gateEnumeration = [...gate.matchAll(/'(INVOICE_CORRECTION_[A-Z_]+)'/g)]
    .map(m => m[1]);
  assert.deepStrictEqual(
    [...new Set(gateEnumeration)].sort(),
    [
      'INVOICE_CORRECTION_TSFIN_MISSING',
      'INVOICE_CORRECTION_TSFIN_NOT_READY',
      'INVOICE_CORRECTION_TSFIN_STALE',
    ],
    'the evidence gate names a correction code outside the closed Candidate-pay enumeration',
  );
  // the positive wholly-sealed test, and both flags read three-valued so an
  // unreadable verdict cannot inherit the boundary
  assert.match(gate, /wholly_sealed_source_self_bill/);
  assert.match(
    gate,
    /jsonb_typeof\(v_verdict->'is_source_invoice'\)='boolean'/,
    'the gate does not read is_source_invoice three-valued',
  );
  assert.match(
    gate,
    /jsonb_typeof\(v_verdict->'ok'\)='boolean'/,
    'the gate does not read ok three-valued',
  );
  // stale is skippable ONLY through that branch: the fall-through refuses it by
  // name before any state is read
  assert.match(
    gate,
    /if p_code='INVOICE_CORRECTION_TSFIN_STALE' then\s*--[^\n]*\n\s*return false;/,
    'stale can reach the pre-R8A fall-through, where it would be decided by state rather than by seal',
  );
  assert.match(gate, /not in \(\s*'TSFIN_PRESENT','TSFIN_NOT_APPLICABLE_PROVED'\s*\)/);
  // and the asynchronous seam consults it
  assert.match(
    issueValidator,
    /and not private\.weekly_source_invoice_issue_tsfin_skippable_v1\(\s*p_invoice_id,code_row\.code\)/,
  );
  // the three states are named outcomes the owner returns
  for (const state of [
    'TSFIN_PRESENT', 'TSFIN_STALE', 'TSFIN_EXPECTED_BUT_MISSING',
    'TSFIN_NOT_APPLICABLE_PROVED',
  ]) {
    assert.ok(issueValidator.includes(`'${state}'`), `${state} is not a named outcome`);
  }
});

test('the issue validator cannot abort the batch it shares with other invoices', () => {
  // WP-05 review F3.  One malformed source header aborted the verdict of every
  // other invoice in the same asynchronous batch, including ordinary invoices.
  assert.doesNotMatch(
    issueValidator,
    /manifest\.source_group_id is distinct from \(\s*v_invoice\.header_snapshot_json#>>'\{meta,source_group_id\}'\s*\)::uuid/,
  );
  assert.match(issueValidator, /pg_input_is_valid\(\s*coalesce\(v_invoice\.header_snapshot_json#>>'\{meta,source_group_id\}',''\),'uuid'\)/);
  assert.match(issueValidator, /v_codes:=v_codes\|\|'WEEKLY_SOURCE_ISSUE_VALIDATOR_ERROR'::text/);
  assert.match(issueValidator, /exception when others then\s*get stacked diagnostics v_validator_error=message_text;/);
});
