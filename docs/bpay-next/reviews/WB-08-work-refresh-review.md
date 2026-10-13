# WB-08 work refresh source review

**PASS — final WB08 native work-refresh source scope; F1–F5 closed, with no open findings in this review.** Earlier draft and intermediate verdicts below are retained as history. This is source review only, not runtime/app or WB44 producer/adoption acceptance.

## F1 — Source book qualification omitted (high)

Draft work_members parent completeness at67–69 requires generic position applied revision and A equality only. It does not read source_book_state/source_book_revision, actual canonical binding version, native channel C/H or binding publication qualification. refresh_work at163 consequently regards an applied Source component's generic A/C/H as amount_known, computes a positive remainder175 and can set selectable READY211 while its Source book remains UNBOUND or posting-required. `query_state='UNAVAILABLE'` handoff at209 does not correct this native money qualification; WB07's later query branch is separate.

The actual native current1236 capture checks matching Source binding/current approval/origin at3138–3157; if needed it seeds through the genuine existing owner and then checks exact bound snapshot at3185–3199. The real1246 `bpay_next_bound_source_snapshot_v1(uuid,bigint)` at585 onwards returns BOOK_UNBOUND655, BOOK_REVISION_STALE657, BOOK_POSTING_REQUIRED665, classification/publication/identity qualifications, and actual native book values. Generic p.C/H are explicitly insufficient before that qualification.

Small fix: add the exact current Source binding/book qualification as a typed native premise of work_members/refresh_work. Unavailable/unbound Source money is unknown/nonselectable with DATA_UNAVAILABLE (or an explicit design-approved native qualification handoff), not absent-zero. Validate actual current approval line/revision/canonical component and native source channel C/H; do not add a current-target-channel rejection belonging to later resolver decisions. Refresh must never invoke seeding, allocate, invent history or add an F25 protocol. Existing1246 seed owner remains the only actual book initializer; its required WB08 post-UPDATE refresh seam is separate. Reuse existing predicates/authority with deliberate volatility/lock placement: the existing bound snapshot helper is a locking definer, not automatically a pure STABLE function call.

## F2 — Fully reserved accepted selection disappears from selected totals (medium)

For genuine A=100,C=0,H=100, positive175 is0, recovery176 is0. A current nonzero approval is not a certified-zero wage. Therefore lines212–220 emit no base AVAILABLE/RECOVERY/ZERO row. A RESERVED row is emitted221–229, but226 unconditionally clears is_selected, selected_amount_known and selected_source_ex_vat after accepted_work found a genuine live sealed PREPARE/run/choice membership at184–189. The actual retained accepted_run/command IDs remain on the cloned row, yet the selection fact disappears from the selected dimensions.

This is observable directly in current group_contribution: selected_count is DISTINCT member identity filtered by is_selected at1252repeatable61; selected_amount sums only selected known rows80. The fully reserved Draft component consequently has selected_count=0/selected_amount=0 despite its genuine captured run_line or segment-parent100. This contradicts design DP03 live accepted offers through DRAFT/EXECUTING and D04's exact selected quantity facts. Job DONE/command COMPLETE alone must not release the offer.

Small fix: carry the genuine accepted selection **once** on the surviving RESERVED contribution when no base contribution was emitted. When an AVAILABLE/RECOVERY/EVIDENCE/ZERO base exists, keep its selection dimension there and clear the duplicate RESERVED copy. Preserve the immutable accepted amount (or explicitly unknown before capture); never substitute today's positive/held amount. Do not double-count selected money across portions or turn RESERVED selectable.

## F3 — Removed unpaid snooze keys silently lose their stale warning (medium)

work_members segment_keys75–76 contains only current approved_segment and historical native origin_share identities. A genuinely snoozed unpaid shift removed from the next approval has neither a current approved_segment nor a paid/held origin_share. Its active actual snooze row remains, but no WORK_SEGMENT member exists. The stale warning check196–201 runs only for emitted member keys; it cannot see that vanished key. A removed zero position may still emit a null-segment parent member, but no monetary row survives212–220, so it also carries no stale segment warning. The new current wage rows thus omit required SNOOZE_SEGMENT_STALE.

Small fix: census actual active/unexpired segment snooze keys against current approved-segment identities in the exact bounded work scope independently of paid origin inventory. Retain the stale warning through typed current-group/control metadata (or on genuine current contribution metadata with the exact stale control identity), exclude nothing, and leave all actual native financial origins unchanged. Do not manufacture an approved_segment, origin_share, financial ZERO row or blocking unknown-amount contribution just to show a warning. Existing stale paid/held removed segments are already found through genuine origin_share and the current per-member stale warning path can remain.

## Bounded positive findings

- Accepted membership follows SEALED work choice/run selection, actual selected Candidate, real run_command/PREPARE and allowed live worker/run lifecycle; it is scoped to exact selected component or segment and uses actual accepted-cut snooze. Missing workers before enrollment are included deliberately. Command status is not used to falsely release a failed/interrupted offer; actual module epoch is retained. Captured segment amount reads actual `run_work_segment_parent` (its real schema has component_key AND segment_key); indivisible amount reads real unique run_line.
- Current work/revision ownership, sealed approval, applied/current pointer and approved-line count are checked before amount availability. Missing pointers cannot become a zero wage; null amounts remain EVIDENCE/unknown. Current target channel is Candidate.pay_method while source basis stays native.
- Exact ordinary segment keys include genuine old origin shares; signed gross balance plus ACTIVE hold shares reconciles aggregate source AND target C/H against the parent before offering segments. Moved keys retain old parent credit; no NET ratios, ordinal guesses or cross-day redistribution.
- Positive remainder, negative informational recovery and active reservation are disjoint. Negative gross is not hidden by snooze. A/C are carried once between base and RESERVED rows; genuine certified zero requires actual approval with C/H zero. Fully settled rows are not fake tickable zero wages.
- The current schema's WORK/CASE/group/member/portion ownership checks and actual composite FKs match the rows constructed by these three helpers. Historical removed segments intentionally have nullable current approved FKs only through actual origin inventory and exact parent reconciliation. Group evidence is nonselectable and unknown.
- Before/after typed group contribution arrays surround a complete exact work replacement in one transaction; only apply_delta mutates summary. Section/group counting and distinct-member selection behavior is visible in the actual summary owner. F2 concerns a lost selection fact, not a request for a new count framework.

Pending product owner integration, complete linked-case refresh, bounded-many controls, final private ACL/registry and source/position successor publication are outside this draft-only review and remain explicit work; no whole WB08/source installation claim is made.

## Reviewed raw SHA-256

- `.codex-local/WB08-refresh-owners.sql`: `243769665e5a3e2f997604503d3b0898eb954f4565cea4e1ffb5c104664be971`
- `docs/bpay-next/design/WB-08-workbench-projection.md`: `81fb741983bb6addb970b97b3a077cd9758789aa008c8e809c49d9e8d3c75182`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql`: `1409b1151c6d73c1336f201f5fe3c794626f04fdac78f03c1d7491c5f5bbf189`
- `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql`: `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`
- `supabase/migrations/10102026_1100_banking_pay_next_segment_exclusion_v1.sql`: `0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`
- `supabase/repeatable/06102026_1246_banking_pay_next_source_book_binding_v1.sql`: `2e9a297ff362e41dbfbadc869b211b37176cf76d06edd3e78808a2e4c2a57340`


## Fresh full-product re-review — 1252 plus narrow1309

Read the complete saved1252 repeatable (all707 lines at this checkpoint), current1252 migration, reviewed projection design, native1236/1100 and original1246 Source helpers. Source-only; no product edits, SQL, tests or runtime. Scope verdict: **original fixes verified; F4 remains; not whole-batch PASS**. Root's pending per-member total reconciliation guard is explicitly not waived.

- **F1 present-WORK scope closed.** work_members is now VOLATILE (default) at1252:196–201. MATERIALIZED source_books206–218 selects the actual current canonical binding, actual READY book revision and matching approved WORK line, then calls original locking bound_source_snapshot. Native approval line/revision/channel and all four native channel C/H operands are compared at227–239 before amount availability. No refresh function seeds, allocates, adds a command clock or invents history. The native shared locks are deliberately retained. Removal support is separately F4 below.
- **F2 closed.** RESERVED at394–404 retains accepted_run/command/epoch and selected amount/knownness when it is the only emitted contribution. It clears duplicate A/C/residual/overhang and selection only when a positive AVAILABLE or RECOVERY base exists at399–402. Thus A100/C0/H100 live Draft retains its exact selected100 once while RESERVED remains nonselectable; A/C remain carried once. Case portions use the equivalent conditional at561–564.
- **F3 closed.** Actual active/unexpired segment snoozes with no current approved key are independently counted at423–428 and retained through group.stale_snooze_count/code. This is metadata only: no fabricated financial origin, zero wage, amount unknown or stale exclusion. Current schema51–52 provides finite typed count/code. The existing per-member warning remains for real origin-bearing removed keys.
- **1309 seam PASS.** Extracted the exact Source seed function from1246 and1309 and compared source bodies after removing only the one new `perform private.bpay_next_workbench_refresh_work_v1(p_work_id);` line: exact equal. The new call is immediately after actual source book UPDATE, before READY return. POSITION_PENDING, exact READY replay and replay conflict paths remain unchanged and bypass refresh; original module/Candidate/work/position locks and proof remain. Owner/revokes match the exact signature and the successor supplies reload notification/atomic transaction.

### F4 — Bound removed Source component excluded / SQL NULL makes parent incomplete

At1252:212 source_books INNER JOINs a current approved_line with component_kind='WORK'. A genuinely removed component has no such current line even when actual current binding and native Source book are READY. It is consequently absent from source_books. At230–232 `NOT (w.work_kind='SOURCE' AND a.component_kind='WORK' AND genuine_source_origin)` becomes SQL NULL for the missing line, and `OR` missing-book comparisons remain NULL; the eventual coalesce at269/277 makes parent_complete=false. Ordinary removed components do not suffer this because ordinary work_kind short-circuits false.

This is a real supported native shape, not missing future logic. Original1246 bound_snapshot669–677 explicitly handles non-PRESENT current binding: it proves no current approval line, sets approved_source_ex_vat=0 and returns actual native C/H, approval/canonical identity and nullable approved_line_id at678–687. A removed Source parent may retain genuine C>0 (informational recovery) or ACTIVE H>0 (reservation). The draft incorrectly turns those entirely into unknown EVIDENCE despite the exact available native witness.

Small correction: allow source_books to qualify the actual current binding for both PRESENT WORK and genuine REMOVED component state, using the binding's real classification/origin identity. Avoid nullable line.kind as the sole needs-book discriminator; make the Source native qualification predicate explicitly null-safe. Compare nullable approved_line_id using `IS NOT DISTINCT FROM` and require actual bound removal with approved A=0 and no current approved line. Preserve the exact source/native C/H reconciliation; do not bypass qualification for all removed keys, fabricate an approved line, revive a missing origin, seed from refresh or alter any money owner. This is within WB08's original native-facts classification and source-book qualification, not future WB02/07/40/41/29 behavior.

Additional bounded source inspection: refresh_case reads real Case/component/rule native capacity, realised/written-off and ACTIVE payout/recovery counters; does not compute earnings headroom or PAYE NET; stores positive available and ACTIVE reserved portions separately; real completed zero outstanding capacity is not a tickable new repayment. PAUSED/REVIEW are explicit native reasons, and snooze basis matches the existing1100 typed fields. refresh_many accepts at most100 exact owners/16KiB UUID envelope, discovers only real linked collection Cases, prelocks Candidate/work/Case/metadata in deterministic order and rejects oversized linked sets. Its per-member cumulative work/byte/total reconciliation guard remains explicit unfinished root work. Metadata setters and named Candidate trigger change finite name/reference/bank warning versions and pending barrier only, without earnings or Candidate-wide item updates. All new private functions have exact owner-only revokes and fixed search_path; no new service/browser EXECUTE. Complete current-source fanout/adoption still belongs to WB44, and the projection remains UNSEEDED.

Fresh raw SHA-256:

- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql`: `6ea5cafbda147e8eb58bf8a6ef976f1d1f02fe3cf6a46cdf374f53dc527dd83a`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `supabase/repeatable/10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql`: `e1330272dfaa2978e537c49e074669bb9273410620f016bdcd1d4f05bf567241`
- `docs/bpay-next/design/WB-08-workbench-projection.md`: `81fb741983bb6addb970b97b3a077cd9758789aa008c8e809c49d9e8d3c75182`


## Latest1252 + additive1316 re-review

Source checkpoint reviewed after removed-Source corrections and mandatory member conservation. F1–F4 are closed for their stated shapes. **One finite classification edge remains F5; no whole-batch PASS yet.** No SQL/tests/runtime/product edits.

F4 closure: source_books now LEFT JOINs the current approval line213; absence is admitted for actual current binding/native book qualification218. Parent check231 makes absence explicit, and nullable approved_line_id is compared with `IS NOT DISTINCT FROM`234. A real removed WORK binding can therefore return the original1246's A0/current-removal/native C/H facts rather than SQL NULL. It does not fabricate a current approved line or seed any book.

Conservation closure: each known emitted native WORK member aggregates its actual item portions at408–419 and compares exact A/C/H, native A−C−H residual, and included+excluded−recovery−overhang to the same native residual before the sole apply_delta call. RESERVED zeros duplicate A/C/residual/overhang only when a base exists; fully reserved keeps them once. Genuine fully settled native members deliberately emit no new wage and are excluded from this emitted-member assertion. Every emitted Case component aggregates exact capacity/realised/ACTIVE H/written-off/residual and deduction balance591–598 before apply_delta. Loan/advance recovery uses funded basis, native credit/payout uses actual approved/funded/paid counters, and no earnings/headroom/NET allocation is calculated. New1316 adds only typed nonnegative written-off-knownness and genuine certified_zero group metadata; original locked1252 schema is retained.

Certified-zero closure: actual sealed/applied certified-zero approval with no approved lines/positions432–435 produces GROUP_EVIDENCE/EVIDENCE, INFORMATIONAL, nonselectable, all financial amounts NULL. It remains honest actual group evidence, not a synthetic financial ZERO member or invented origin. The actual group certified_zero flag and CERTIFIED_ZERO reason exclude that precise evidence from unknown-money-count while monetary sums remain zero68; no generic missing evidence is excluded. Existing genuine approved zero component members still use their exact native current line/C/H proof and ZERO constraints. No financial fact is manufactured.

### F5 — Removed Source EXPENSE/ADDITIONAL classified as book-required WORK

The new absent-line discriminator `(a.component_kind='WORK' OR a.id IS NULL)` at218/231 fixes removed WORK but also classifies every removed non-WORK Source component as requiring the WORK Source book. Current1236 Source publisher1675–1718 genuinely emits SOURCE:<component_id> rows of kinds WORK, ADDITIONAL and EXPENSE; these are not theoretical later kinds. When an actual expense/Additional disappears, its position may still exist with A=C=H=0 after normal removal application. The current line is absent, but its actual retained native original classification is non-WORK. source_books either has no READY book or bound_source_snapshot correctly returns CLASSIFICATION_UNBOUND (Source non-WORK tax treatment can be null). parent_complete becomes false and refresh emits unknown EVIDENCE, making a previously complete genuine work group incomplete even though that removed reimbursement has no remaining monetary obligation.

Small exact fix: derive removed component classification from the actual retained binding/version classification_line_id or canonical_line_id joined to immutable approved_line (existing1246 removal explicitly carries actual prior classification206/215). Use that exact kind to require a book for removed WORK and preserve non-WORK's independent generic native completeness. Do not treat every NULL current line as WORK, do not guess from SOURCE:<uuid> text, do not bypass a missing WORK book, and do not reconstruct money or traverse arbitrary history. When removed kind itself lacks genuine retained evidence, remain explicitly unavailable. This is an original WB08 native classifier edge; future WB07 query/WB29 resolver branches remain excluded.

Latest raw source hashes:

- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql`: `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `supabase/migrations/10102026_1316_banking_pay_next_workbench_native_conservation.sql`: `367f4e133ff30453e6eeb9ce86cf96ff503f45ebb4da94b3ad96a91bf4098339`
- `supabase/repeatable/10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql`: `e1330272dfaa2978e537c49e074669bb9273410620f016bdcd1d4f05bf567241`


## Final1321 source closure — PASS

Fresh-read the full uninstalled1321 successor and the two corrected CASE expressions. **F5 closed; F1–F5 all closed for this review's native work refresh scope. No remaining concrete source finding.** Root-reported local compilation remains a separate receipt; this review executes no SQL/runtime/tests and does not certify first-use, app acceptance or future card closure.

- `workbench_work_members_v1(uuid,uuid)` now obtains missing current classification from the actual canonical binding/version's retained `classification_line_id` (fallback actual `canonical_line_id`) joined to immutable approved_line25/61. It performs no generic history walk, source-key parsing, invented reimbursement classification or financial seeding.
- Actual current line wins. Component kind is NOT NULL on genuine current approved_line, so kind COALESCE33 only falls back when that current line is absent. The corrected **CASE WHEN a.id IS NOT NULL** expressions34–35 preserve genuine current NULL tax/date; older values are used only for a missing current line. They do not make unknown current expense tax/date appear inherited or known.
- Strict Source native book qualification applies to actual WORK kind27/43, including retained removed WORK. Real removed EXPENSE/ADDITIONAL do not become WORK merely because a current line is missing. Unknown missing Source classification fails parent completeness42, rather than guessed non-WORK bypass. Native book approval revision/current-null line/source channel/all four native channel C/H comparisons remain exact45 onwards; genuine removal's nullable line identity uses IS NOT DISTINCT FROM.
- Present approval A, true removed A0, original generic native C/H, exact segment source and target C/H reconciliation, old-parent credit and all WORK/Case member conservation guards remain as reviewed in1252/1316. No new money/effect/Case owner, NET ratio, per-shift parent, accepted-cut protocol or todayterms replacement was added.
- Successor preserves exact signature/security-definer/fixed search_path; VOLATILE default remains deliberate for native locking bound snapshot. Exact postgres owner and all public/anon/authenticated/service_role revokes, atomic BEGIN/COMMIT and schema reload are present. Registry maps exactly this signature's finalRoot/finalPhysical to1321 with installed1252 as explicit predecessor/allowed physical; no unrelated registry owner is changed by this closure.
- Reviewed1309 Source seed seam remains the sole post-actual-UPDATE refresh addition to original1246 and bypasses pure existing READY replay. Real native seed proof and locks remain unchanged.

Current final work-refresh reader authority is1321; installed1252 is the immutable predecessor. Required future WB02/07/40/41/29 branches and WB44 complete seed/producer fanout/read adoption remain their named owners, explicitly outside this source PASS. Group/source scope stays truthful and UNSEEDED does not become enabled by this review.

Final raw SHA-256:

- `supabase/repeatable/10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql`: `4f07a5ab56fd01aa40ccdb9163f3458c66a838af8e056f35987be2c24cb70542`
- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql`: `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`
- `supabase/migrations/10102026_1252_banking_pay_next_workbench_summary.sql`: `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab`
- `supabase/migrations/10102026_1316_banking_pay_next_workbench_native_conservation.sql`: `367f4e133ff30453e6eeb9ce86cf96ff503f45ebb4da94b3ad96a91bf4098339`
- `supabase/repeatable/10102026_1309_banking_pay_next_workbench_source_seed_refresh_v1.sql`: `e1330272dfaa2978e537c49e074669bb9273410620f016bdcd1d4f05bf567241`
- `docs/bpay-next/final-definer-closures.json`: `fc844c8d17ed8cc7b04f4e650e0e38ed418365233c7771424d07a910f398838e`
