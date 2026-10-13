# FC-16 implementation design review

11 October 2026, sequence 100. Independent read-only review of the root's proposed implementation and current native sources. Only this note was authored. No product, database, business action, test, fixture or runtime was invoked.

**Design verdict: PASS with the exact source qualifications below.** The shared typed protected-binding predicate, separate bounded references, fixed four-field assertion summary and current write-off integration are the narrow card-required seam. No new financial owner, producer, command, table or future Case surface is needed. This is not implementation or runtime acceptance.

## Authority and current parents

Full FC-16 card: ordinary backend R3 plan 03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1372–1394; all nine checklist rows: IMPLEMENTATION_CHECKLIST.md:2945–2968; master sequence/dependencies: 08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:184–218; R3:452–460; R2:270. The existing allocation chain and final-definer registry remain controlling by signature. See FC-16-writer-census.md for eight actual mutation owners and source hashes.

Current logical write-off parents are accept_write_off_v1/apply_write_off_v1 in 04102026_1647_banking_pay_next_work_collection_write_off_v1.sql. Original accepted Create and immutable membership/header guard are 11102026_1358_banking_pay_next_guarded_create_v1.sql. Sealed Case selection is 11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql. DRAFT capture/allocation is 11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql; PAYE NET is 11102026_0559_banking_pay_next_worker_reprepare_v1.sql. Native release/discard completion is 0814; use the registry's exact final bodies rather than copied historical versions.

## Native binding predicate

The proposed private.bpay_next_case_protected_bindings_v1(case_id) can serve both named reader and assertion as a typed relational predicate. It must include:

* Actual selected CASE_COMPONENT accepted-choice members, before a worker, Case instruction or hold exists. Exact native state here is **pay_run.status PREPARING, selection_state OPEN** after accepted Create; 1358:262,570–573. My earlier message's phrase “OPEN run” was incorrect and is superseded by these exact fields. Protection does not begin only at SEAL or READY.
* The original selected sealed Case selection fallback, qualified by the accepted/current economic selection binding. Exclude false choices. Do not union arbitrary historical selection revisions; prefer actual accepted membership and deduplicate its corresponding sealed input.
* Every exact ACTIVE Case hold, with original instruction/result/source tuple and native Candidate/run/component/channel identity. Do not hide a still-ACTIVE hold because a separate completion flag is inconsistent.

Do not drop a binding for a deadline having passed, a command having been accepted, CANCELLING status, or a released **old** hold alone. Exclusion requires the exact completed native release/discard/expiry binding for this run/Candidate, or proved final zero allocation as described below. Original economic preparation revision is fixed across retry; mutable execution preparation revision must not select new economic inputs.

Deduplication must preserve all genuinely distinct active holds and their exact source amounts while counting an accepted input and its own allocated hold only once. A generic DISTINCT on run/component must not silently discard another native positive hold. Broken identity, missing typed result/use or conflicting counters fails closed.

## Final zero and NET qualification

No-hold plus a completed **zero** result can establish that an originally selected input protects no allocated slice. The zero must belong to the exact instruction, economic preparation/selection revision and applicable final allocation state. Any old completed zero result is insufficient.

The stage distinction is essential: GROSS_DEDUCT/GROSS_ADD/NET_ADD retain their DRAFT result through PAYE NET (0559:3168–3182). **NET_DEDUCT** alone takes the NET allocation/replacement branch (3183–3257), so its exclusion must use the worker's current completed native NET projection/result. Do not label every PAYE recovery as pending until NET merely because its instruction is a deduction. A partial or REVIEW allocation remains pending unless the exact final result is already established by its native stage.

The design's conservative unknown NULL for a genuinely pending, not-yet-final binding is acceptable. Never substitute £0, sum a nominal estimate or re-run the money kernel to fill it.

## Summary and assertion

An aggregate approved-minus-disposals-minus-all-active-holds figure describes combined authority, but must not authorise a purpose-specific UNRESERVED_AMOUNT. For example, a fully funded £100 Advance with no recovery or holds has no remaining payout capacity even though that aggregate is £100. A small typed capacity-purpose operand is necessary. Reuse the current 0204 native factual bases: PAYOUT is approved minus funded, paid credit and written off, then active payout; RECOVERY is funded for Loan/Advance, otherwise approved, minus recovered, written off and paid credit, then active recovery. See 0204:179–182,206–210,277–286. Do not sum mirrored components or use weekly/affordability/kernel calculations. The later owner's original component/period limits still apply.

Default NULL is sufficient: ordinary debt can infer RECOVERY, Credit PAYOUT, while Loan/Advance needs an explicit purpose for monetary authority. Current write-off passes RECOVERY explicitly. FUTURE_ONLY/WHOLE_CASE can report NULL unreserved capacity for unspecified mixed purpose. Protected count and amount remain whole-Case protection and must not omit the other purpose's holds. Validate native active source hold/counter consistency and nonnegative purpose capacity; do not clamp an inconsistent negative balance into usable zero. This precise correction supersedes the earlier aggregate-capacity permission.

Return exactly unreserved_capacity, protected_count, protected_amount and next_cursor. Unknown protected amount/unreserved capacity stays NULL when pending authority makes the figure unknown. Detail pages retain each known allocated source amount; the fixed summary must not conceal pending protection behind an exact-looking allocated total.

Closed modes FUTURE_ONLY, UNRESERVED and WHOLE_CASE are sufficient. WHOLE_CASE refuses whenever protected_count is positive. UNRESERVED refuses unknown pending capacity, missing/invalid requested amount and amount above known unreserved capacity. Numeric NULL/NaN/nonfinite/negative operands and stale expected Case revision fail closed. FUTURE_ONLY is for preserving accepted authority while updating future authority; it is not a bypass allowing principal/hold/instruction/opening rewrites. Current write-off uses the monetary modes, never FUTURE_ONLY.

The small summary may aggregate indexed native facts without returning a protected array. The slice reader applies a finite limit and total keyset ordering (kind,run_id,component_id,native_id); all-or-none cursor shape and expected Case revision checks remain native. Do not materialize an unbounded protected JSON array inside a helper and limit only its outer serialization.

## Writer and lock closure

Current apply_write_off holds Candidate → job/request → position → Case/component (1647:278–305). Its VOLATILE assertion runs after those existing locks and before the event/counter writes at 326–332. Original Create's component SHARE (1358:225) serializes with component UPDATE; the assertion must use a fresh native snapshot after waiting on that lock. A STABLE assertion inheriting a pre-wait statement snapshot can miss newly accepted protection.

Catch only the exact protection refusal code into original BLOCKED outcome. Preserve existing stale basis REVIEW, exact replay, actor/request identity, remaining_amount and protected_amount meanings. In particular, old write-off receipt protected_amount continues to mean the exact allocated active recovery hold counter; pending protection gets its truthful issue code rather than changing that field's meaning or fabricating money. Other assertion exceptions must not become a successful or generic BLOCKED financial result.

No protected foreign header lock or inline release is needed. Existing WORKER_RELEASE already takes header before Candidate and moves the run to PREPARING/review++ before any hold mutation (0814:3608–3609,3699–3704). Reuse it as the separately ordered exact whole-Candidate action. If the accepted input has no worker yet, do not guess a worker UUID or call that worker-only route; the reader can identify the protected preparation and the existing whole-preparation discard/wait path.

The other seven Case mutation owners are native creation/allocation/NET/settlement/current-principal reconciliation/release/cancellation owners. Preserve their original captured-authority and live balance rules; do not add a blanket logical-change refusal that blocks their own reservation or disposal.

## Completion limit

This approves the narrow architecture, subject to the explicit predicates and source qualifications above. Future FC-03/FC-09–13 commands remain their assigned owners and must later wire the assertion. EX-27 is a named future next action, not a present dependency/call. Implementation source/ACL/index/compile checks and joined business scenarios remain pending author evidence; no whole-item PASS is claimed.

## Initial 1509 source pass — essential amendments

I read all three authored helpers, the single partial index migration and FC-16-implementation-boundary.md. The initial assertion is correctly VOLATILE, reads after caller-owned locks, compares expected Case revision, produces exactly four keys, rejects invalid amount/mode operands, refuses unknown pending or whole protected authority, and acquires no foreign header lock. Its three functions are owner-only with exact public/anon/authenticated/service-role revokes. The new CASE_COMPONENT partial lookup index matches the actual accepted member predicate.

Two initial source amendments are needed before source closure:

1. **Purpose-specific monetary authority:** the initial generic capacity formula can overstate PAYOUT authority for funded Loan/Advance. Add the narrow purpose qualification described above; preserve whole-Case protection and the required four response keys. Root agreed this supporting operand, retaining native 0204 bases and no new allocation formula.
2. **Indexed sealed fallback:** the initial fallback filters selection_item.case_id directly, while existing 0300 indexes lead with case_component_id or run/Candidate/revision. Drive that branch from existing case_component WHERE case_id, joining selection_item on case_component_id. Existing component (case_id,component_key) and selection component indexes suffice; no second index is needed.

The final-zero predicate distinguishes NET_DEDUCT from other stages, uses captured economic/selection revisions and the current completed NET projection. Accepted members exist without a worker and remain pending; exact completed release/discard/expiry qualifications are distinct from status/deadline guesses. ACTIVE hold rows remain independently protected.

Initial saved repeatable bytes inspected: c24f2a1def03d95e3a0e2f894a221a6abb986f59d418177d3b13cc72f96ceb58; a subsequent root save during this read was d2d9521ea65e8f17c8b88b46fdb46e6edb4325cc8de0b20000eec1c2c92ff966. Index migration SHA-256: 1486b5b007d39ba1e74c7ca31b35a01ea81a82139161be340915b6a0b6f23b21. These identify the initial read, not a final immutable-source claim. The forthcoming 1647 write-off successor still requires independent review; no source PASS or installed status is claimed here.

## Final saved-source closure

**SOURCE PASS.** This section supersedes the initial pending amendments and hashes above. I independently reread the final 1509 definitions, boundary, migration, two Worker issue enums and four exact registry entries. No essential source finding remains. This verdict is static only; no database, business, compile, test, fixture or runtime operation was performed by this reviewer.

The two original amendments are closed: the five-argument assertion qualifies PAYOUT/RECOVERY using the exact existing 0204 factual bases, infers ordinary debt/Credit only, refuses missing Loan/Advance purpose for monetary authority, and returns mixed-purpose NULL for nonmonetary scope. Sealed fallback now drives from Case components and the existing component/selection indexes. The private helper takes seek and limit inside its SQL query; the reader retains at most 100 references and a 120000-byte bound, preserving nullable grandfathered component identity in rows while ordering by kind/run/native identity.

A further pager finding is also closed: the outer reader join to native actor display_name now explicitly orders by binding_kind,run_id,native_id (1509:208–211). This preserves the helper's continuation order after the join and prevents a reordered actor lookup from changing the last emitted seek tuple.

The current predicate retains accepted members with no worker/hold, deduplicates their corresponding sealed fallback, independently retains every ACTIVE hold, and treats unknown source amounts as pending. It distinguishes native completed release/discard/expiry/cancellation from flags/deadlines. Per-Candidate terminal evidence may complete while siblings continue. Positive allocation disposal requires the native realised hold/use/event tuple; RELEASED alone is insufficient. NET_DEDUCT finality uses current NET projection/request/job and captured economic selection identity; the DRAFT-final stages remain distinct. The Case family is indexed; no agency-wide membership list or new accepted-input producer was introduced.

Write-off application remains the **complete current 1647 parent** plus one post-lock assertion block. A static inverse comparison removing that block exactly reconstructed the normalized complete original function statement. The call occurs after position, Case and component locks and before event/counter mutation; it passes explicit RECOVERY and WHOLE_CASE/UNRESERVED_AMOUNT according to the original request. Only exact BPAY_NEXT_CASE_PROTECTION_PENDING and BPAY_NEXT_CASE_PROTECTED_SLICE become the existing BLOCKED result. All other check violations rethrow. Original replay, stale-basis REVIEW, request/actor/lease identity, event formula, native counters, remaining_amount and allocated protected_amount semantics are unchanged.

Migration1509 adds only the accepted selected CASE_COMPONENT lookup index and the exact BPAY_NEXT_WRITE_OFF_INPUT_PENDING alternative in the original write-off issue-code CHECK. The two existing Worker allowlists each add only that same finite value; their command/receipt shape, unknown replay, monetary validation and transport remain unchanged. No current Office write-off schema or screen was added. The three helpers and write-off successor retain owner postgres and public/anon/authenticated/service_role revokes.

Registry final signatures are exactly:

* private.bpay_next_case_protected_bindings_v1(uuid,jsonb,integer)
* private.bpay_next_case_reserved_slices_v1(uuid,jsonb,integer)
* private.bpay_next_case_change_assert_v1(uuid,bigint,text,numeric,text)
* private.bpay_next_apply_write_off_v1(uuid,uuid,bigint)

All four point to 1509; only apply_write_off advances a pre-existing mutation owner. The other seven census mutation owners, one-plan/kernel, Rule-B immutable cut and existing whole-Candidate release/header ordering remain unchanged. Later FC-03/FC-09–13 callers still own their actual logical actions and exact future-version application. The assertion acquires no foreign header and performs no inline release.

| Final saved file | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_1509_banking_pay_next_case_change_guard_v1.sql | 087c3c5d3ae9cfaa94b66ea58b338358d0236f2b4953be5ee6adf7e3dfc60fd4 |
| supabase/migrations/11102026_1509_banking_pay_next_case_protection_lookup_indexes.sql | f9fe0d603447db42aff4f3837e5896fa6e1f5ab24e67d67c3f1ceb262dfca027 |
| broker/src/bpay-next-command.js | 792d94f0ef97fb12e8daffaa9afa6ab92cf467664f22d4c02fda4e188f830923 |
| broker/src/bpay-next-job-step.js | 93f8eb2b99ab9f09fe1ef4a727d1a1d3a643fa94207affc4cea71bc02306f179 |
| docs/bpay-next/final-definer-closures.json | 9e74386e4b9d3155746a96ed62e3650f7f8f363da0daf5463da3575442f4994c |
| docs/bpay-next/design/FC-16-implementation-boundary.md | 0d28a0fd03a4c6a5009a2540be056c6b2132df6dee31b04edfd1c7e32abf5d6c |

No whole-item, joined business, final installation or release acceptance is asserted by this source receipt.
