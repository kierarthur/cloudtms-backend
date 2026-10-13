# WB-29 W2 Source UNBOUND seed implementation

12 October 2026. Read-only implementation investigation for the existing RESOLUTION_SAVE/CLEAR transaction. **Design evidence only; product, SQL execution and joined acceptance NOT_RUN.** Whole WB-29 and its captured composer remain open. Root alone edits product.

## Current owners and the precise gap

All paths below are relative to the backend worktree; line references describe the current saved source.

| Existing owner | Current physical source | Relevant behavior |
|---|---|---|
| `private.bpay_next_source_disposition_seed_v1(uuid,text,uuid,uuid,uuid)` | `supabase/repeatable/10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql:5–44` | Locks NEXT module, Candidate control, Work and exact Position; initializes an UNBOUND book from the original proof, then calls the existing Work refresh. READY replay compares the original immutable seed tuple and performs no refresh. |
| `private.bpay_next_source_book_seed_proof_v1(uuid,text,uuid,uuid,uuid)` | `supabase/repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql:295–418` | Exact current/applied Work and Position revision; native APPLIED/DONE publication; original paid Effect→Hold→frozen Run line and active Hold→Run line point proofs; ZERO observes maintained balances without proving history absence. |
| `private.bpay_next_bound_source_snapshot_v1(uuid,bigint)` | `supabase/repeatable/12102026_2146_banking_pay_next_treatment_bound_source_snapshot_v1.sql:24–113` | Qualifies the exact current binding/version, native bundle/publication and original approval root while retaining actual current/applied Position eligibility. Returns BOOK_UNBOUND before checking the supplied positive book revision; after seeding requires real READY/current book, native postings and classification. |
| Initial Source qualification reached by that snapshot | `private.bpay_next_initial_tsfin_bound_assert_v1(uuid)`, original `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:187–218`; its certificate is currently `12102026_2146_banking_pay_next_treatment_initial_source_certificate_v1.sql:17–75` | Preserve the original immutable certificate/line tuple and native I-1 inventory/currentness check. The current certificate qualifies actual treatment through its positively validated approval root; W2 does not rebuild I-1 or replace the retained source digest. |
| Existing collection anchor | `supabase/repeatable/12102026_2155_banking_pay_next_treatment_collection_identity_v1.sql:950–1010` | Exact `(work_id,component_key)` collection retains `original_effect_id` and original frozen line identities. The first paid-history anchor at 959–962 is attribution, not proof of the whole maintained R; 988 explicitly compares R to the Effect amount. Do not invoke reconcile to discover or certify a seed. |
| Existing Work refresh | `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql:318–409` | A seed may refresh/queue ORIGIN and acquire related Case locks. W2 must acquire all prerequisite Work/Position locks before the first seed, and prehold the finite related Case set in native order. |

The native seed proof verifies an Effect/Hold against its own frozen line, **but does not compare that witness's source amount with the whole Position R/H** (1246:341–377). A W2-only caller qualification must add those two exact scalar equalities. No source-book reset, history sum, new producer, new proof store or rewritten seed owner is needed.

Native schema points: `09102026_2010_banking_pay_next_source_book_binding.sql:24–69` defines UNBOUND/0 versus READY/positive revision, four native Source bucket scalars and exact seed Effect/Hold composite FKs. Its binding PK is `(canonical_work_id,canonical_component_key)` at73–91; the current version has an exact composite FK at155–159. `04102026_1519_banking_pay_next_work_collection.sql:9–43` gives the exact collection unique `(work_id,component_key)` and immutable original Effect/Run-line/approved-line identities; current2155:623–636 freezes them. `03102026_1324_banking_pay_next_core.sql:1011–1016` provides the two existing scoped witness indexes below. No new index or storage is necessary for this procedure.

## Exact native tuple and finite discovery

Seed only a submitted SOURCE Work/component whose actual current/applied revision equals its W2 expected revision, whose exact Position has that applied revision, and whose current binding is keyed by that same canonical Work/component. Retain the original Source publication/bundle/root checks through the current 2146 snapshot. A binding on another canonical Work is a scope mismatch, not permission to expand or seed another Work implicitly. A missing Position/binding, pending publication, unsealed actual revision or unsupported signed book is a typed refusal.

Use the existing Position scalars `realised_source_ex_vat` (R) and `held_source_ex_vat` (H), with `source_basis_channel`. These are native source units. Retain all native target tuple, Candidate, original Timesheet, transfer, job and original-unbound frozen-line checks in 1246; never compare a whole worker total or another component's amount.

* R=0: pass a NULL Effect ID; do not search paid history. H=0: pass a NULL Hold ID; do not search active history. R=H=0 uses the original ZERO arm, which also requires all six maintained realised/held target values to be zero and proves a real current or removed Source component (1246:384–411). Older child rows do not disqualify ZERO.
* R>0: a positively qualified existing exact collection pointer may supply `original_effect_id`; re-read that Effect by PK and its native Hold/Run line by PK. Require `effect.source_disposition_ex_vat IS NOT DISTINCT FROM position.realised_source_ex_vat`, then the complete original paid witness proof. A collection's AUTO/MANUAL or `paid_basis_qualified` flag is not a substitute for that proof; the ordinary paid-support flag deliberately does not qualify SOURCE (2155:970–988).
* If no usable immutable native paid pointer exists, discover only a declared finite physical prefix for this exact Work/component using `bpay_next_effect_work_history_idx(work_id,component_key,occurred_at_utc,id)`. Select the unfiltered history rows in that native order with `LIMIT declared_cap+1`; refuse overflow **before** filtering by PAYROLL_SETTLED, amount, transfer or line. Within the complete bounded set require one exact full-R native witness, not the first/latest payment and not a sum. A concrete W2 cap of 100 plus one overflow sentinel fits its existing 100-row envelope; document the constant in the owner rather than pretending the schema proves every history has at most 100 rows.
* H>0: use `bpay_next_hold_active_key_idx(work_id,component_key,id) WHERE status='ACTIVE'` for the exact component. Bound the physical ACTIVE membership with the same declared cap plus sentinel before any amount/channel/line qualification; require one full-H witness with `hold.source_reserved_ex_vat IS NOT DISTINCT FROM position.held_source_ex_vat`, then the original held proof. Multiple qualifying witnesses or incomplete/oversized membership refuse; do not aggregate active Holds or silently choose a UUID.

These predicates discover PKs solely from trusted server state; W2 does not accept browser Effect/Hold IDs. Positive R/H with no complete exact witness remains BOOK_UNBOUND with an honest qualification refusal. The accepted request must not claim zero or READY to bypass it.

Concrete discovery shape, **before** economic filters (read-only pseudocode for the named W2 implementation, not a new installed function):

```sql
-- v_cap is the explicitly declared physical witness cap (proposed 100).
select e.* from private.bpay_next_financial_effect e
where e.work_id=v_work_id and e.component_key=v_component_key
order by e.occurred_at_utc,e.id limit v_cap+1;

select h.* from private.bpay_next_hold h
where h.work_id=v_work_id and h.component_key=v_component_key
  and h.status='ACTIVE'
order by h.id limit v_cap+1;
```

Materialize only this bounded prefix, reject `count > v_cap`, then filter/point-join the complete small result. Check cumulative discovered count and internal encoded witness-ID bytes across the already bounded W2 scope too; never let a later component silently exhaust or truncate a declared transaction budget. For a known collection pointer use a direct PK lookup instead and the same full-R/original native tuple checks. The active partial-index predicate is membership, not an economic filter. These reads perform no financial UPDATE and no history aggregate. A required existing row-type point proof can still be called by the owner after exact singleton discovery; no broad exception handler should turn its structural mismatch into success.

## Transaction procedure and lock order

1. Preserve receipt-first exact replay/readback, full original request equality, actor/agency/module epoch and raw intent validation. A historical accepted receipt does not re-run seed qualification.
2. Under the NEXT module lock acquire the Candidate worker control and **all** explicit Works in UUID order. Acquire all exact Positions in `(work_id,component_key)` order. Precompute the complete finite related collection Case set with overflow detection; acquire Case parents in UUID order before the first native seed/refresh. Do not acquire Work2 after Work1 has refreshed a Case. Respect the existing source binding point/SHARE guards and existing finite ORIGIN continuation. Refuse an unproved oversized prerequisite lock set before mutation instead of truncating it. The existing1438 wrapper's `LIMIT100` Case path queues ORIGIN on reaching100 (339–344); `refresh_many` uses101 as an overflow sentinel (367–395). W2 must distinguish that durable continuation from a complete prerequisite prelock set. Do not call1246's receipt-wide `source_binding_lockset_v1` (85–105) as single-component discovery: it deliberately expands the complete native receipt union and belongs to the existing Source publisher.
3. Validate the disclosed W1 context and expected actual revisions before own seed/refresh effects advance Workbench/financial metadata. Qualify each exact binding with current `bound_source_snapshot(binding.current_binding_version_id, 1)` while UNBOUND; `1` here is only the helper's required positive probe operand, not an invented current book revision. Accept only the expected BOOK_UNBOUND qualification after all earlier native publication/currentness checks. Missing/superseded/processing/classification evidence is not repaired by W2.
4. Discover and revalidate each full-R/full-H witness under the already held Candidate/Work/Position serialization. Call the unchanged `source_book_seed_proof_v1(actual_work,component,expected_actual_revision,effect_pk,hold_pk)` for final tuple qualification, then the unchanged `source_disposition_seed_v1` once for that UNBOUND Position. Its UPDATE touches only native source-book initialization fields and invokes the existing refresh (1309:33–43).
5. Immediately re-read the actual seeded Position/book revision and require current `bound_source_snapshot(binding_version_id,position.source_book_revision)` to return READY with the exact requested Source/approval identities and native amounts. A later failure rolls back seed and refresh with the whole SAVE/CLEAR transaction. Do not catch it and publish a partial success receipt.
6. Existing READY books are qualified using their actual current book revision and retained postings. Do **not** call seed with the new treatment revision or newly discovered witnesses: READY's original immutable seed IDs/revision intentionally remain historical (1309:24–29). Do not set book revision to 1 again.
7. After every Work/Position/Case prerequisite and all finite Source qualification succeeds, stage all complete treatment revisions and only then call the current publisher/global command clock. Keep original ACTIVE sibling carry/event order and the one immutable W2 receipt unchanged. Seed qualification changes neither A/R/H nor accepted pricing or existing command/job ownership.

Current E2/BD019 remains authoritative: an actual Work revision changed after accepted Create is held back/system-released. The original Source approval root qualifies native authority only; it never revives the old accepted offer or migrates an accepted decision to new pricing.

## Refusal vocabulary and required support

Preserve the original native errors: `BPAY_NEXT_SOURCE_BOOK_SEED_ID_REQUIRED` (22023); `BPAY_NEXT_BINDING_NEXT_REQUIRED`, `BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED`, `BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE`, `BPAY_NEXT_SOURCE_BOOK_SIGNED_SEED_UNAVAILABLE`, `BPAY_NEXT_SOURCE_BOOK_TYPED_WITNESS_REQUIRED`, `BPAY_NEXT_SOURCE_BOOK_ZERO_BASIS_UNAVAILABLE`, `BPAY_NEXT_SOURCE_BOOK_ZERO_COMPONENT_UNAVAILABLE` (55000); `BPAY_NEXT_SOURCE_BOOK_PAID_WITNESS_MISMATCH`, `BPAY_NEXT_SOURCE_BOOK_HELD_WITNESS_MISMATCH`, `BPAY_NEXT_SOURCE_BOOK_COMPONENT_MISMATCH` and `BPAY_NEXT_SOURCE_BOOK_SEED_REPLAY_CONFLICT` (23514). W2's own finite discovery overflow, missing or ambiguous full-scalar witness can raise the existing closed `BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE` (23514; current2213:109–119). Do not catch arbitrary23514/55000, unexpected SQL/structural errors, or an actual seed replay conflict as that ordinary qualification failure. Source snapshot states retain their original meaning; only READY authorizes downstream quote/publication.

Small necessary support is a private W2 qualification/discovery helper or equivalent finite block in the named W2 owner, using existing indexes and original five-argument seed/proof APIs. No Source seed/bind producer rewrite, money formula, READY reset, all-Candidate/history inventory or new command/queue/protocol is proposed. The cap is a finite service envelope with refusal, not a completeness claim for oversized historical books. Exact SQL/source review and compile remain future work.

## Read-source hashes

| Current physical source | SHA-256 |
|---|---|
|1309 disposition seed | `E1330272DFAA2978E537C49E074669BB9273410620F016BDCD1D4F05BF567241` |
|1246 original binding/proof | `2E9A297FF362E41DBFBADC869B211B37176CF76D06EDD3E78808A2E4C2A57340` |
|2146 bound snapshot | `560982266CF01CCC9F743F9008E88107013E7831E0ACDFA79837EB88AFCF9400` |
|2155 collection identities | `9FA243C98655E950E670BCB65EB73EF554883CF00CB2E1824982A5ED407C8EEE` |
|2146 initial Source certificate | `86DE08DD3BF9A64E7E347E9D41CB1430515E3ED83DC906ECAB1CE7940B470A16` |
|1438 refresh wrappers | `B8D445C4A4CFC02978E8AC77A159350B0E1978AAFF9A1172DA8E7E157A6414F5` |
|2135 publisher | `89A98FC7688803AABE93011EDFCF588C7BAA602DC46C903AA6E3000B1E0751E8` |

Source trace closed for this bounded seed design. No product changes or runtime assertions are included. The subsequent concrete W2 source must still prove these finite guards, whole transaction ordering and original receipt semantics.
