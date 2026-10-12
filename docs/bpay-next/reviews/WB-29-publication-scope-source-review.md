# WB-29 publication scope — independent source review

12 October 2026. **SOURCE PASS for the two private 2316 helpers.** No blocking defect found in the reviewed bytes. This is a bounded supporting batch, not approval of the unfinished W2 constructor or whole WB-29. Read-only source/schema/document inspection and this review write only; no SQL, tests, compilation, catalogue probe, installation, business data or hosted action was performed by this reviewer.

## Exact reviewed source

`supabase/repeatable/12102026_2316_banking_pay_next_resolution_publication_scope_v1.sql`, SHA-256 **5e0e66249e76555fe14a65d68aee704ef7518e3c38883d13d5cc6b656508bc56**.

The registry names only `private.bpay_next_resolution_revision_census_v1(uuid[])` and `private.bpay_next_resolution_source_seed_plan_v1(uuid,text,uuid)` for this file. Both are private VOLATILE SECURITY INVOKER helpers with fixed `pg_catalog,private` search paths, owner `postgres`, and EXECUTE revoked from PUBLIC, anon, authenticated and service_role (128–131). No producer, public trigger, seed owner, publication owner or money formula is replaced.

## Census and native traversal

Lines 15–50 reject NULL/empty/oversized revision arrays, deduplicate IDs, require real approved/sealed revisions, and consume the complete physical tree before any intent/bucket filtering: revision, all lines, rates, schedules, shifts, breaks, approved segments and revision worker basis. The 10001-row prefix is an overflow sentinel; cumulative count and encoded `to_jsonb(row)::text` bytes are checked while consuming rows. The 10000-row/8388608-byte budget applies across all distinct supplied current/root IDs; no unbounded JSON aggregate or partial clone is returned. Line/schedule counts and each line's native rate count are verified after that bounded census. Worker basis must exist. The native schema has no expected-shift/break count to fabricate.

Actual relation/column names match core1324 and segment1100. Existing revision/line/shift/rate/schedule indexes, break `(shift_detail_id,break_no)` uniqueness, approved-segment revision index and worker-basis revision PK support the scoped predicates. The joins are native indexed parent-child scopes; this source review does not claim a universal planner choice, constant physical visit cost or measured latency. Supplied IDs must be the entire locked current/root union. This helper cannot discover a root omitted by its caller and intentionally does not substitute for the original treatment-root/payload assertions.

## Source seed qualification

Lines 63–85 preserve exact SOURCE Work/current/applied/Position revision and current canonical binding qualification through the current2146 bound snapshot. READY returns `seed_required:false` with its actual book revision and no new witness IDs. It performs no replay seed, book reset, reconciliation or refresh. UNBOUND must have the native BOOK_UNBOUND qualification; signed R/H remain refused.

For positive R, the existing exact collection pointer is a point lookup and must match Work/component/PAYROLL_SETTLED plus **the entire maintained R**. Without that pointer, lines 99–106 take the physical whole-component effect prefix, in the actual `occurred_at_utc,id` order, with 101 sentinel before kind/amount filtering. Exactly one full-R witness is required. Positive H likewise bounds native ACTIVE membership before the whole-H equality filter, with exactly one witness. Zero R/H supplies NULL witnesses and retains the original ZERO proof; historical children are not falsely treated as zero disqualifiers. The two reads match core1324's effect-history and ACTIVE-hold indexes (1011–1014).

Line 121 invokes unchanged1246 `source_book_seed_proof_v1` with those trusted native IDs. Its complete paid Effect→realised Hold→frozen Run line, settlement/transfer/job/source tuple, held Run-line tuple, component and ZERO predicates remain authoritative. The helper adds scalar R/H coverage rather than summing history or treating the collection's first paid anchor as amount authority. Original proof returns one row or raises; it cannot silently authorize an absent proof. There is no catch converting structural errors into success. Current2146 snapshot's native receipt member helper itself restricts the receipt to one or two native members (1246:23–24); it does not introduce an all-Candidate expansion.

## W2 caller obligations retained, not claimed implemented here

The file explicitly requires the full Candidate/Work/Position/related-Case scope to be preheld. Final W2 review must still verify receipt-first exact replay; native Candidate fence; sorted complete prelocks/public NOWAIT qualification and retention premarking; all current/root census before the first seed; complete request/pair/context checks; all-Work staging before first publication/agency clock; actual native seed followed by a READY snapshot recheck; inclusive mapped sibling carry; and atomic final receipt. Native oversized related-Case ORIGIN continuation is retained. These two helpers perform no mutation and cannot independently prove that transaction ordering.

The proposed bounded W2 context refinement is sound at this boundary: echo the exact native offer context without its large observational `works` array, and retain row expected revision/decision pair in the typed intent. Recompute Source/Position/quotes under prelocks rather than accepting browser money facts. **Current2241 offer_context has 14 scalar keys, not 15** (111–116); use its exact native key set. Its metadata/financial/workbench/adoption/sequence/target/date operands plus per-row revision/pair must all be compared before W2's own seed/refresh advances them. No extra counter or financial hash is required by this refinement. The actual constructor remains subject to independent source review.

## Evidence hashes

| Source | SHA-256 |
|---|---|
| W2 publication prelocks design | `297f4556471ab054aa3d011406f55770b3fde8eefd2f250fe2122ee6ec8c02dc` |
| W2 Source seed implementation design | `330194643c6e101286ebed4a81840ff4e3ba8c00fa73dd43daba727fef3fa385` |
| Original1246 native binding/proof | `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340` |
| Current2146 bound snapshot | `560982266cf01ccc9f743f9008e88107013e7831e0acdfa79837eb88afcf9400` |
| Current registry at this review | `498ea9a503a43b4b2cd7fea44f610ccfe01eee78e4b42fac4e58e072b526101d` |

The supporting helper batch is source-approved at the above bytes. Whole WB-29 remains open; no joined/runtime acceptance is asserted.
