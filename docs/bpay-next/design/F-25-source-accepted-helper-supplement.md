# F-25 Source accepted helper supplement

Read-only finite source supplement, 9 October 2026. Root alone edits product. No SQL/runtime/tests/fixtures/hosted calls. Current 2303 acceptance certificate shape, 2306 NO_CASE, 2308 case PREPARE and 2314 accepted ordinary instruction/period scopes remain the case authority; this supplement does not replace them. SQL blocks below are design fragments, not installed functions. Root's separate local 2258 compilation repair is outside this investigation.

## 1. Exact helper signatures and typed snapshot

Use explicit accepted-context helpers; preserve strict current 1246/0018 readers for publication/current eligible Workbench/current Daily callers. Do not add ignore_current or session flags.

```sql
-- New accepted initial assertion; immutable member certificate only.
private.bpay_next_initial_tsfin_accepted_assert_v1(
  p_effective_work_cut_id uuid,p_binding_version_id uuid,p_component_key text) returns void

-- New snapshot: exactly the current1246 ordered21-column return contract.
private.bpay_next_accepted_source_snapshot_v1(
  p_effective_work_cut_id uuid,p_binding_version_id uuid,p_expected_book_revision bigint)
returns table(qualification_state text,selection_state text,candidate_id uuid,
  canonical_work_id uuid,canonical_component_key text,binding_version_id uuid,source_book_revision bigint,
  approval_work_id uuid,approval_revision_id uuid,approved_line_id uuid,head_id uuid,source_receipt_id uuid,
  source_channel text,tax_treatment text,classification_line_id uuid,valuation_policy_id uuid,
  approved_source_ex_vat private.bpay_next_penny_amount,
  native_realised_paye private.bpay_next_penny_amount,native_realised_umbrella private.bpay_next_penny_amount,
  native_held_paye private.bpay_next_penny_amount,native_held_umbrella private.bpay_next_penny_amount)

private.bpay_next_source_book_seed_proof_accepted_v1(
  p_effective_work_cut_id uuid,p_component_key text,p_seed_effect_id uuid,p_seed_hold_id uuid)
returns table(seed_kind text,basis_revision_id uuid,basis_line_id uuid,source_channel text,
  realised_source private.bpay_next_penny_amount,held_source private.bpay_next_penny_amount,seed_digest bytea)

private.bpay_next_source_disposition_seed_accepted_v1(
  p_effective_work_cut_id uuid,p_component_key text,
  p_seed_effect_id uuid default null,p_seed_hold_id uuid default null)
returns table(qualification_state text,source_book_revision bigint)
```

All new helpers retain audited logical owner postgres, fixed `search_path=pg_catalog,private,public`, private-only ACL and module/control/actual caller ownership. Keep current caller SECURITY modes. There is no need for a new seed table: 2153 schema already has `position.source_book_seed_effective_cut_id` FK to effective_work_cut.

## 2. One exact retained member lookup

Use record aliases/INTO STRICT to avoid output-variable column ambiguity (notably `candidate_id`, `source_channel`, `binding_version_id`, `qualification_state`). Effective cut seals after earlier Candidate commands finish and original publication APPLIED/DONE; its compound FK retains exact approved effective revision/member count and original sequence `<accepted_sequence`.

```sql
select cut.* into strict v_cut
from private.bpay_next_effective_work_cut cut where cut.id=p_effective_work_cut_id;
select wi.* into strict v_input
from private.bpay_next_accepted_work_input wi
where wi.id=v_cut.accepted_work_input_id and wi.accepted_input_id=v_cut.accepted_input_id
  and wi.work_id=v_cut.work_id and wi.effective_revision_id=v_cut.effective_revision_id;
select member.* into strict v_member
from private.bpay_next_accepted_work_member member
where member.accepted_work_input_id=v_input.id and member.work_id=v_cut.work_id
  and member.effective_revision_id=v_cut.effective_revision_id and member.component_key=p_component_key;
select binding.* into strict v_version
from private.bpay_next_source_component_binding_version binding
where binding.id=v_member.source_binding_version_id;
select approved.* into strict v_line
from private.bpay_next_approved_line approved
where approved.id=v_member.effective_approved_line_id
  and approved.revision_id=v_cut.effective_revision_id and approved.component_key=v_member.component_key;
```

Then require exact candidate/work/revision/line/component identity, `v_version.id=p_binding_version_id` where supplied, binding_digest=`member.source_binding_sha256`, and exact matching binding tuple in `wi.origin_proof.bindings`. The accepted guard already certifies these; delayed helper re-proves immutable linkage instead of selecting today's binding. Classification/policy/channel/nominal amount come from immutable binding/approved line/effective revision. `approved_line` has no work_id column; ownership is through its revision.

Source capture's current supported shape is self canonical work/component, one publication command, WORK/TAXABLE, PAYE or UMBRELLA same target channel. Retain it. Current `source_component_binding.current_binding_version_id`, WORK.current/applied_revision_id, position.applied_revision_id, root current/withdrawn and HEAD COMMITTED_CURRENT are not delayed terms predicates. Retained publication command/job/receipt identities remain mandatory. No new Source comparison or two-command/cross-channel execution is admitted here.

## 3. Immutable initial TSFIN certificate

2303:258–266 retains this actual per-member shape, also enforced by 2153 member insert guard:

```json
{"candidate_id":"uuid","work_id":"uuid","revision_id":"uuid","command_id":"uuid",
 "origin":{},"basis":{},"certificate_digest":"hex",
 "component":{"component_id":"uuid","component_sha256":"hex"}}
```

It has no top-level source_channel or full components array. Do not call 0018 `initial_tsfin_certificate_v1(root_auth)` again: it intentionally reads today's physical/current TSFIN/root/contract/effective inventory. The accepted assertion adapts the exact old 0018 tuple check against this retained origin/basis/component, and derives channel from immutable v_version.approval_source_channel + effective revision. In addition to candidate/work/revision/command/line identity, preserve original root version/generation/row signature and financial/inventory/entitlement/detail/policy fingerprints.

Critical digest checks:

```sql
v_cert:=v_member.source_origin_certificate;
v_origin:=v_cert->'origin';v_basis:=v_cert->'basis';v_component:=v_cert->'component';
if pg_catalog.decode(v_cert->>'certificate_digest','hex') is distinct from
     private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_INITIAL_TSFIN_CERTIFICATE_V1',v_basis)
  or pg_catalog.decode(v_cert->>'certificate_digest','hex') is distinct from v_version.initial_certificate_digest
  or (v_component->>'component_id')::uuid is distinct from v_version.source_component_id
  or pg_catalog.decode(v_component->>'component_sha256','hex') is distinct from v_version.component_sha256
  or v_line.evidence_ref is distinct from 'source-component:'||v_version.source_component_id::text||'#'||
       pg_catalog.encode(v_version.component_sha256,'hex')
  or v_version.binding_digest is distinct from pg_catalog.sha256(pg_catalog.convert_to(
       pg_catalog.jsonb_build_array('INITIAL_AUTHORISED_TSFIN_V1',v_version.canonical_work_id,
         v_version.canonical_component_key,v_version.approval_revision_id,v_version.approved_line_id,
         v_version.initial_root_authorisation_id,v_cert->>'certificate_digest',v_cert->>'command_id',
         v_component->>'component_sha256')::text,'UTF8')) then
  raise exception using errcode='23514',message='BPAY_NEXT_INITIAL_CAPTURE_CERTIFICATE_CHANGED';
end if;
```

Also preserve the complete 0018:201–217 tuple comparisons using saved cert origin/basis; digest-only equality is not a substitute for these identities. A later inventory/TSFIN pointer does not matter; a damaged retained fingerprint/line does.

## 4. ZERO first seed is an actual monetary proof

ZERO is about the maintained actual book, not absence of older rows, not approved_source=0 and not a missing current component. It is valid even when retained accepted nominal is positive, provided every actual realised/held SOURCE and TARGET counter is zero and the exact retained member line/binding/certificate is valid.

```sql
if basis_line_id is null then
  if v_position.realised_source_ex_vat<>0 or v_position.held_source_ex_vat<>0
    or v_position.realised_target_ex_vat<>0 or v_position.realised_target_vat<>0
    or v_position.realised_target_inc_vat<>0 or v_position.held_target_ex_vat<>0
    or v_position.held_target_vat<>0 or v_position.held_target_inc_vat<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_ZERO_BASIS_UNAVAILABLE';
  end if;
  -- v_line is the exact retained effective approved line, not a current reader.
  if v_line.source_component_id is null or v_line.component_key is distinct from p_component_key
    or p_component_key is distinct from 'SOURCE:'||v_line.source_component_id::text then
    raise exception using errcode='23514',message='BPAY_NEXT_SOURCE_BOOK_COMPONENT_MISMATCH';
  end if;
  basis_revision_id:=v_cut.effective_revision_id;basis_line_id:=v_line.id;seed_kind:='ZERO';
end if;
```

2153 accepted_member.effective_approved_line_id is NOT NULL with exact approved-line FK. Therefore supported accepted ZERO is anchored to an actual retained PRESENT line (possibly nominal zero); do not copy old1246's fallback to today's binding canonical line/current HEAD absence. An original REMOVED/no-line binding is not represented by these members. Missing original member proof must retain typed unavailability, not synthesize it from a later removal. Later current removal of an originally retained PRESENT line is harmless to the immutable old member. This supplement does not claim missing accepted removal/Source comparison shapes implemented.

## 5. Remaining finite caller/guard blocks

### Original nonzero seed and COMMON_HEAD receipts

The accepted seed proof replaces only 1246's current/applied revision/HEAD terms discovery. Preserve its genuine nonzero ORIGINAL_POSITION checks :337–390: real original PAYROLL_SETTLED effect, original timesheet/work/component/Candidate, REALISED paid hold and original frozen line/source/target, actual settlement job and transfer/member; one exact ACTIVE held witness with its frozen source/target values. Existing unbound adoption requires original frozen line.source_binding_version_id IS NULL; preserve it. A bound frozen line necessarily passed READY initialization before hold and must not be used to explain an otherwise UNBOUND book. Do not SUM history or force current maintained realised to equal an original whole payment after legitimate later recovery; seed the actual qualified scalar counters, using the original witness as typed basis. Negative/signed/mixed states retain their current unavailable/refusal codes.

COMMON_HEAD publication members already exist in accepted `wi.origin_proof.bindings[].publication_members`; their actual old1246 return shape is `(candidate_id uuid,member_no integer,head_id uuid,prior_head_id uuid,work_id uuid,revision_id uuid,publication_command_id uuid,predecessor_revision_id uuid)`. Choose the exact binding array element by binding_version UUID/digest; preserve cardinality1/2 and member_no uniqueness. Parse retained members rather than calling current `source_binding_members_v1`:

```sql
select value into strict v_retained_binding
from pg_catalog.jsonb_array_elements(v_input.origin_proof->'bindings') witness(value)
where witness.value->>'binding_version_id'=v_version.id::text
  and witness.value->>'approved_line_id'=v_line.id::text
  and witness.value->>'binding_sha256'=pg_catalog.encode(v_version.binding_digest,'hex');
-- Scope/check this parsed tuple against the exact receipt arrays and binding
-- publication1/publication2 tuples before accepting it as an original member.
select member.*
from pg_catalog.jsonb_to_recordset(v_retained_binding->'publication_members') as member(
  candidate_id uuid,member_no integer,head_id uuid,prior_head_id uuid,
  work_id uuid,revision_id uuid,publication_command_id uuid,predecessor_revision_id uuid);
```

Then re-prove each exact prior publication APPLIED/DONE/applied_at and POSITION_APPLY command COMPLETE/sealed/member count, job DONE/released lease, matching immutable command sequence/module and strict predecessor `<accepted_sequence`. Verify saved receipt ID/request_digest/decision_bundle/revision, head/member root/family/version arrays and binding's publication_work/revision/command tuples. The pre-clock acceptance already certified COMMITTED bundle/current head/root at that point. Do not demand today's head/root/current WORK pointer; retained original committed receipt integrity is sufficient. One- or two-member **proof** does not broaden the current simple execution shape: 2018 execution still refuses binding.publication_command_id_2 nonnull.

### First-seed/READY transition and money snapshot

After immutable cut/member/certificate proof, the seed owner follows module→Candidate control→WORK→position. It does not borrow a POSITION_APPLY job's lease. Its actual capture or settlement caller has already established the correct live owner; do not require an expired PREPARE lease in a legitimate outcome reader. F26 old execution epoch/resume remains separately fenced by real job/module checks.

UNBOUND first seed sets existing first-seed fields once:

```sql
-- Only in UNBOUND; expected_applied term now means retained effective revision.
update private.bpay_next_position position
set source_book_state='READY',source_book_revision=1,
    source_book_seed_effective_cut_id=v_cut.id,
    source_book_seed_applied_revision_id=v_cut.effective_revision_id,
    source_book_seed_kind=v_proof.seed_kind,
    source_book_seed_revision_id=v_proof.basis_revision_id,
    source_book_seed_line_id=v_proof.basis_line_id,
    source_book_seed_effect_id=p_seed_effect_id,source_book_seed_hold_id=p_seed_hold_id,
    source_book_seed_channel=v_proof.source_channel,source_book_seed_digest=v_proof.seed_digest,
    native_realised_paye=case when v_proof.source_channel='PAYE' then v_proof.realised_source else 0 end,
    native_realised_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.realised_source else 0 end,
    native_held_paye=case when v_proof.source_channel='PAYE' then v_proof.held_source else 0 end,
    native_held_umbrella=case when v_proof.source_channel='UMBRELLA' then v_proof.held_source else 0 end
where position.work_id=v_cut.work_id and position.component_key=p_component_key
  and position.source_book_state='UNBOUND';
```

The accepted proof digest binds cut.id, work/component/effective revision, chosen basis revision/line, actual channel/counters/effect/hold/seed_kind (domain-separated new accepted proof digest). `source_book_guard_v1()` first-seed branch recomputes **accepted** proof only when new.seed_effective_cut_id nonnull; old exact proof remains for genuine non-F25 unbound adoption. Add this field to only its first-seed paired JSON exclusion arrays. Keep READY JSON comparison immutable including this seed-cut field, original digest and seed metadata; never clear/rewrite it. Keep caller native/cache write prohibition and once-only scalar delta→native/book-revision derivation unchanged.

On READY, the accepted seed helper verifies the existing actual book and returns its current revision. Do not repeat old seed's cut/effective revision/effect/hold parameters as replay-equality against a later accepted cut, and do not reseed. The first seed remains evidence for that book; a later accepted member is another immutable approval snapshot against it.

Snapshot's actual money qualification remains:

```sql
if v_position.source_book_state is distinct from 'READY' then
  qualification_state:='BOOK_UNBOUND';return next;return;
end if;
if v_position.source_book_revision is distinct from p_expected_book_revision then
  qualification_state:='BOOK_REVISION_STALE';return next;return;
end if;
if v_position.source_book_seed_channel not in ('PAYE','UMBRELLA')
  or v_position.source_basis_channel is distinct from v_position.source_book_seed_channel
  or (v_position.source_book_seed_channel='PAYE' and
      (v_position.native_realised_paye,v_position.native_held_paye,
       v_position.native_realised_umbrella,v_position.native_held_umbrella) is distinct from
      (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,0::numeric,0::numeric))
  or (v_position.source_book_seed_channel='UMBRELLA' and
      (v_position.native_realised_umbrella,v_position.native_held_umbrella,
       v_position.native_realised_paye,v_position.native_held_paye) is distinct from
      (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,0::numeric,0::numeric)) then
  qualification_state:='BOOK_POSTING_REQUIRED';return next;return;
end if;
```

Return immutable approval nominal/line/classification/policy/channel and actual qualified monetary native fields. Keep selection_state's existing `UNBOUND` token unless a real caller contract requires otherwise; accepted context is the explicit cut. Capture is the point at which current **money** revision is read, not acceptance.

### 2018 exact delayed owners

| Final current signature | Minimal successor blocks |
|---|---|
| `capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` :739 | Enumerate accepted members through exact cut/run_work, bounded original work/component cursor. Read nominal from immutable accepted line. Keep actual position locking/missing-position refusal. Replace current binding lookup :929 and current snapshot/seed :973–975 with accepted member+helpers. Preserve simple same-channel self-bound WORK/TAXABLE execution limits, saved CLEAR query pin and actual SOURCE/TARGET snapshots. Do not omit a selected member because its current applied pointer moved. Missing original member is not permission to invent a later line. |
| `hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` :1161 | Use frozen run_position→cut/member exact identity and accepted snapshot. At :1304 remove only applied_revision/current nominal equality. Preserve every real realised/held SOURCE/TARGET scalar and actual source_basis_channel equality and expected pre-hold monetary book revision; one shared selected quote target valuer is called once. Preserve frozen line/ACTIVE hold and actual positive hold delta. |
| `post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` :2298 | Bind frozen line/run_work/worker to original accepted cut if it exists. Retain actual current settlement job/module/lease, transfer/member and exact owned ACTIVE hold/source/target. F25 Source lines already had READY books before hold. Do not compare current book revision to immutable line.source_book_revision: its **own hold** has advanced the monetary revision, and other valid book postings can also advance it. Read current actual revision under lock, qualify current book, then once-only H→R. Original frozen nominal/binding/certificate remains exact; current query/head/target settings are not rediscovered. |

A retained non-F25 unbound line can still use original2018 reconciliation branch :2437–2454, with actual admitted ACTIVE hold and original typed seed. It has no accepted member, so no retroactive cut/receipt is fabricated. A F25 bound line with an UNBOUND book contradicts READY initialization/immutable READY guard; refuse damaged proof rather than lazily adopt it from today's binding. Preserve all effect/hold/transfer idempotence and exactly-once native scalar derivation. No outcome helper may demand the old PREPARE lease or reset real money to its acceptance counters.

## 6. 1519/2314 managed run_position branch

2314 deliberately preserved this branch; current capture2018 :1003–1018 still calls current collection origin and compares observed applied/nominal to capture. The F25 accepted WORK successor must close **both** caller and trigger together. 2314's accepted_collection_origin helper requires selected CASE and cannot be reused blindly: managed WORK capture may point at a genuine collection that is excluded in the sealed case choices. Managed completion is not case admission.

Resolve exact new.run_work_id/run_worker/Candidate/work/captured revision to its effective_cut and accepted member (same join as section2 plus real run/command). Require real current PREPARE lease/control/module and phase RESERVE on INSERT, ALLOCATE on its single managed_capture_completed transition. Preserve 2314's CASE instruction/period branches verbatim. For this separate branch, bind captured nominal/line/revision to accepted WORK; collection origin is its immutable genuine original paid tuple plus real current money as in2314 helper, **without forcing CASE.is_selected** or reading today's observed nominal/applied terms. If an accepted_case child for that collection is present, its exact FK/origin/policy is retained even when excluded; exclusion must never turn into an instruction.

Finite check split:

```sql
-- Use exact retained member line/revision, never today's observed approval.
(new.captured_revision_id,new.approved_line_id,new.approved_source_ex_vat)
  is not distinct from (v_cut.effective_revision_id,v_member.effective_approved_line_id,v_line.source_pay_ex_vat)
-- Actual current monetary snapshot at RESERVE remains equal to new capture.
and (new.realised_source_ex_vat,new.held_source_ex_vat,new.realised_target_ex_vat,
     new.realised_target_vat,new.realised_target_inc_vat,new.held_target_ex_vat,
     new.held_target_vat,new.held_target_inc_vat,new.source_basis_channel)
  is not distinct from (v_position.realised_source_ex_vat,v_position.held_source_ex_vat,
     v_position.realised_target_ex_vat,v_position.realised_target_vat,v_position.realised_target_inc_vat,
     v_position.held_target_ex_vat,v_position.held_target_vat,v_position.held_target_inc_vat,v_position.source_basis_channel)
```

Retain new.work_collection_revision=current actual reconcile_revision, original ordinary paid-effect/line/REALISED-hold/transfer proof, PAYE original/native/principal/component invariants, negative residual management, no ACTIVE held SOURCE/TARGET for this existing managed lane, exact row immutability and one managed_capture_completed spend. If the collection origin arose from an earlier queued command, prove that exact original command precedes accepted_sequence; never use a later command as a missing pre-cutoff case receipt. NO_CASE remains NO_CASE; an unsupported real managed negative shape retains its typed handling and cannot create a new accepted case/instruction.

Do not silently omit an accepted member for managed bookkeeping. ALL original certified-zero/no-line removal is not represented by accepted_member's NOT NULL approved-line FK; the pre-existing current-only fallback cannot become an accepted component. Missing retained shape remains an explicit unresolved boundary, separate from valid retained PRESENT/zero-money first seed.

Current ordinary period money has an important schema exception: 1637:89–92 replaces the old recovery<=opening_due constraint with `work_collection_id IS NOT NULL OR recovery+active<=opening_due`. Ordinary unscheduled collection opening is audit evidence, not a cap on later genuine debt in the week. Preserve this final constraint; do not restore the original unconditional scheduled-debt cap. The existing period UPDATE/component/real-capacity checks remain mandatory.

## 7. Finite apply scope and source receipts

Define accepted initial assertion, original/accepted Source proof/seed/snapshot and source_book_guard first-seed successor before capture/hold/outcome callers. Add only the managed-position hunk to the current2314 guard when that caller is changed; keep newest2303/2308/2314 CASE scope unchanged. Preserve final-definition/source selection inventory and audited private ACL/owner mapping. F26 epoch resume and later WB/SP/CM calculators remain separate; no local compilation or fixture verdict is supplied here.

Saved source SHA256 checkpoint below. Root is editing concurrently: re-read before edits or receipt reuse. This is a design source checkpoint, not a runtime/compile/install verdict.

| Source under supabase | SHA256 |
|---|---|
| repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql | `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340` |
| repeatable/07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql | `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2` |
| repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| repeatable/09102026_2153_banking_pay_next_accepted_input_v1.sql | `4462d4e9a45909ba5e428b1916f74a7eeacf99b73add469b64eae8f185e28810` |
| repeatable/09102026_2236_banking_pay_next_accepted_prepare_consumers_v1.sql | `2881978637f38c72a6db81ed982f11a0e684bae6bac54a24bfbbb76979c3203c` |
| repeatable/09102026_2303_banking_pay_next_accepted_case_terms_capture_v1.sql | `db987eb3eba2ba945e48bbc7ac76b5e68f2ac6ffa8122d8360c67f80a1327f7b` |
| repeatable/09102026_2308_banking_pay_next_accepted_case_prepare_v1.sql | `a6e84241e6622386b2645e9373cd714d64fc661ba244bcde5fdb248f094f82f2` |
| repeatable/09102026_2314_banking_pay_next_accepted_collection_capture_guard_v1.sql | `8fa44b6075008486114ed8d95e4befac9905790c491dc8a1c4300129a784ead3` |
| migrations/09102026_2153_banking_pay_next_accepted_input_v1.sql | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| migrations/04102026_1637_bpay_next_work_collection_recovery.sql | `96c18eba594b864429858b65f1ff8e27225500f7c81b07058f2d17e61012ea9c` |
