# FC-13 SQL source review

12 October 2026. Independent read-only SOURCE review of BD148 and the current uninstalled 1646 repeatable / 1649 migration. No product edits, SQL/RPC, business data, fixtures, tests, hosted calls or runtime acceptance. Root alone writes product. Full FC-13 ITEM and dependent capture/NET/transfer consumption remain BLOCKED_ON_CM-13a.

**Initial verdict: OPEN — two concrete findings need closure.** The batch is being assembled concurrently; these anchors identify the reviewed saved snapshot, not a final immutable release.

## S13-01 — put terminal result validation after STEP, not in CLAIM loser classification

In `12102026_1646_banking_pay_next_case_channel_resolution_v1.sql:1823–1825`, the CASE_CHANNEL_RESOLVE DONE/DONE/null cursor and exact receipt/result comparison is inside CLAIM's known-not-claimable exception path. That path still has the original pre-claim `v_job` and no apply `v_result`. A legitimate queued NEW/READY or currently LEASED job therefore fails the terminal predicate and rethrows the original 55000 instead of reaching the existing lease-busy/not-claimable classification.

Keep CLAIM's native request-present / NEW / null-cursor qualification. Move the exact terminal result proof to the post-STEP validation immediately after the updated job is re-read (same file:1992 onward). This is a finite placement correction, not a change to lease takeover or unknown-result replay.

## S13-02 — fail closed on unconsumed accepted FC-13 decisions

Current FC-13 apply activates a new manual Rule and writes RESOLVED component target fields (1646:605–615), but current DERIVE/prepare remains the 1536 money owner. Its eligibility at1536:2791–2792 checks only original source PAYE and worker target PAYE; it later calls the full-approved-principal automatic PAYE→PAYE quote at2971–2985 and its writer replaces component target/ref at3416–3422. It does not consume an FC-13 decision.

Thus a PAYE-source Case accepted for an Umbrella target can later encounter a PAYE worker after the Candidate changes again; the old consumer can silently replace the accepted target with its automatic PAYE quote. A selected manual amount is also not a current money authority merely because its component target columns were written. This violates BD148's explicit deferred-consumption boundary.

Root has accepted the narrow fix: refuse the exact applied FC-13 decision when it is at/before this original accepted Case/component cut, using the existing INPUT_UNBOUND code until CM-13a implements its consumer. Preserve a genuinely older captured cut before the decision and all immutable instructions/results/holds. Do not remove channel/equality gates or add conversion/allocation logic here. The new guard's actual source still requires review.

## Snapshot evidence

- 1646 repeatable SHA256 `6A8200264E0CAAE8EDE07C3840F1783EDDBC653E59A9655BF3B16487E2DDC38D`.
- 1649 migration initial SHA256 `2FE630108DAEE29D0B59263975F0A50DCF9D486F56DE4439195F68084D3AA5A6`; final bytes will be re-read.
- Current 1536 parent SHA256 `14364DA406573B3031C30E9F2F74148862109EB2C03613E0035B3897B6F0665B`.
- BD148 design SHA256 `E23A7C7E79469C0BB6959DA1F752F26F57AFA82B4C2707C358600DE94A84E68E`.

Admission, origin/receipt qualification, schema guards/ACL, manual/fixed quote distribution, breakdown bounds and final complete-parent integration are still under source review. No completion or visual claim is made.

## Stable assembled source amendment

Re-read stable1646 SHA256 `E4E3B335AE66C17481AE21C57AE163DCCAB0CE8C23A3FEE4C333FF5CAC54F6F9`, unchanged1649 `2FE630108DAEE29D0B59263975F0A50DCF9D486F56DE4439195F68084D3AA5A6`, and current BD148 `E23A7C7E79469C0BB6959DA1F752F26F57AFA82B4C2707C358600DE94A84E68E`. The signature inventory contains17 functions:10 new private functions and7 current-parent successors.

**S13-01 CLOSED:** the exact DONE/DONE receipt/result comparison is now post-STEP after re-reading the mutated job (1646:2009–2011). CLAIM's loser path checks only native request presence/NEW/null cursor, then preserves the old busy/not-claimable classification.

**S13-02 CLOSED:** DERIVE's new point guard (1646:3111 onward; line shifts follow added bounds) joins the exact immutable component decision, APPLIED outcome and native COMPLETE command/DONE job at matching Candidate/module/sequence. It compares applied Case and component revisions using `<=` against both original memory-basis revisions, or the current native revisions on the existing non-memory path. It does not require equality to mutable later reservation revisions. Earlier positively proved memory cuts before the decision remain on their original Rule; later/current cuts refuse with existing CASE_INPUT_UNBOUND. No channel/equality gate or arithmetic was lifted.

**S13-03 CLOSED:** the actual assembled Preview reply now has a120000-byte check, its token a32768-byte check and emitted cursor an8192-byte check at1646:377–385. The whole Case sums/manual pool precede the bounded breakdown; an oversized envelope is a named refusal, not a fake empty result or silently truncated total.

Admission validates closed input and exact Preview under Candidate→Case/component locks and replays its original request before new current-fact validation. Apply uses the accepted target/company/VAT/date/mode, native live capacity under its original locks, and the DR37 Candidate financial fence. Its single Case transaction preserves approved/funded/realised/written-off counters and automatic original Rule/source identity; monetary instruction/hold creation is absent. The manual Rule uses independently stored **source** schedule terms, retains approved principal and original creation/funding age, and requires the actual APPLIED/DONE/COMPLETE origin. Per-component decisions survive later component revisions through immutable command/member identity and the supporting version index. Private schemas/functions revoke all four exposed roles; only the three original public service boundaries retain service_role EXECUTE. Append-only update/delete/truncate guards remain private.

Static inverse comparisons restore the complete1536 parent statement exactly after removing the finite additions for intake, enrollment, components, edit-origin, and the DERIVE refusal. The new claim is an exact named clone of the current restructure claim. The public page restores exactly after its Suggest/channel-receipt additions; the job owner's residual difference is only one leading indentation change on its retained restructure admissibility line after the finite CASE_CHANNEL branches are removed. No unrelated formula, source owner, kernel or legacy route change was found.

### Remaining named reader contract fact

The full card03:1292 expressly requires native percentage/VAT tiles. `case_channel_quote_v1` already captures `erni_pct` and `vat_rate_pct` in its immutable `quote_operands`, but the current Preview row at1646:363–369 drops those facts. Office cannot infer percentages from money. Expose those exact qualified native operands in the bounded component row, or show their honest unavailability where the source quote is unqualified; this requires no new calculator. The current SQL arithmetic/lifecycle stage otherwise passes static source review, but final reader closure awaits this small contract decision.

The migration's `case_channel_period` table is unused: no current1646 body inserts a period receipt. BD148 defers new-period/old-unreserved-period money consumption to CM13a. Remove/defer unused future period storage/triggers unless a real owned point receipt is written now; do not describe an empty table as implemented period-amendment behavior. Current original openings/history remain untouched.

All runtime, queue, business and screen acceptance remains NOT_RUN; no whole FC13 ITEM approval is inferred from this scoped receipt.

## Candidate balance and schedule scope findings

The card03:1292 requires Candidate balances, so a reader fact must be present rather than silently omitted. Existing native evidence is `11102026_1939_banking_pay_next_case_edit_v1.sql:3046` and `11102026_2129_banking_pay_next_case_audit_v1.sql:212`: maintained Case principal (funded for Loan/Advance, approved otherwise), less realised recovery, write-off and paid credit. The report's summary at2129:395 deliberately keeps Advances and Overpayments separate. The old taxable suggestion at26052026:56216 exposes the current Case's source remaining balance; it does not prove a universal Candidate net balance.

The smallest new Preview fact is a read-only Candidate-scoped aggregate of these existing native balances in the same economic bucket as this Case: genuine kind/subtype, tax, source channel, direction and instruction purpose. Label that scope explicitly. Alternatively expose separate buckets. Never subtract Credit from Debt or add PAYE and Umbrella source units into one scalar. A historical/unmapped or mixed source-unit scope must return NULL with a reason; NULL is not zero. Open/Paused scope is already supported by1324:1019's Candidate index and1242's partial Candidate index. This scalar aggregate needs no new table, calculator, agency inventory or financial writer. The current Case's outstanding includes reservations; unreserved capacity is a distinct FC16 fact and must not replace it.

**S13-04 — preserve the original schedule contract.** The new paired schedule's1646:84–87 `ceil(total/weekly)=week_count` check is stricter than the reused native helper. `11102026_1536_banking_pay_next_case_create_intake_v1.sql:81–86` explicitly accepts nominal requested weeks greater than the number of positive penny instalments. A MANUAL target £0.03 over3 yields £0.01 for3 weeks; source £0.02 over3 yields native £0.01 for2 positive weeks with1 spare nominal week. The new source equality refuses this before any consumer has disposed source capacity. Remove this additional source acceptance restriction; preserve the original helper's arithmetic and show its nominal/final schedule facts truthfully. CM13a must reconcile paired source/target penny remainders at its owned consumption seam. The target equality is also an additional contract restriction and should not be silently introduced under “reuse existing schedule arithmetic”; no new refusal is needed merely to avoid presenting a spare nominal week as a positive payment.

Root has agreed to remove the unused period storage and expose the existing qualified ERNI/VAT operand facts; their actual final saved bytes, Candidate-balance fact and schedule correction still await a fresh final hash/closure. All above findings are source-only.

### Fresh native facts amendment

Fresh1646 SHA256 `0B2A271A3F619D501C1B974B1DF9BF7B712781E86E336E48BE19566298B05DD1`;1649 SHA256 `78B8D481614E38B236AF6CDD05239FBB1E3A3E3509DD0FE8CD3DD5E9685A27E9`.

S13-04 is CLOSED: the schedule preserves native nominal terms and separately returns positive target-payment count, last positive payment week and amount. Native terms bound nominal weeks/date range first, so the smaller actual payment count does not extend that range. The unused period table and its update/delete/truncate triggers are removed. Preview rows now expose only their qualified native taxable ERNI/VAT operand facts; a single top conversion percentage is NULL if the converting rows disagree. No browser ratio or VAT calculation is required.

Candidate balances now group the exact native Candidate's OPEN/PAUSED components by kind/subtype/tax/source channel/instruction, without debt/credit or channel netting. **S13-05 remains OPEN:** the aggregate at1646:306–307 uses funded source for every Loan/Advance component, including its PAYMENT/PAYOUT component. That reverses its open payout meaning: a fully funded payout would still appear open. Reuse the original component-specific outstanding formula at1939:3066–3069: PAYMENT approved−funded−paidCredit−writtenOff, RECOVERY funded for Loan/Advance (approved otherwise)−recovered−writtenOff. Native Credit's funded operand is zero under its existing family guards. Keep the group/unit labels; do not call a funded payout amount an open balance. Root was notified immediately; final closure requires the actual corrected branch.

## Final scoped SOURCE verdict

**SOURCE PASS for the FC13 decision/Preview/application batch, with money consumption and the whole ITEM still BLOCKED_ON_CM-13a.** This supersedes the historical OPEN statuses above. No compile, business, runtime, joined queue or visual acceptance is claimed by this review.

Final re-read1646 SHA256 `4DE995FF3034A9E067EB55E5D7F802027AB8F1BCF7C1A73FC5092819125CADCD`;1649 SHA256 `78B8D481614E38B236AF6CDD05239FBB1E3A3E3509DD0FE8CD3DD5E9685A27E9`; current design SHA256 `E23A7C7E79469C0BB6959DA1F752F26F57AFA82B4C2707C358600DE94A84E68E`.

**S13-05 CLOSED:**1646:306–309 now uses instruction-specific native RECOVERY versus PAYMENT formulas. The existing0300 component family CHECK at107–114 proves instruction direction and that funded money is zero outside Loan/Advance, paid-credit zero outside Credit. Separate current Candidate buckets retain kind/subtype/tax/source/instruction units and NULL+reason for an invalid balance; holds are not subtracted from their gross outstanding. The source-channel header is an actual distinct native PAYE/UMBRELLA list, not an assumed common source. ERNI/VAT rows and common-conversion percentage retain exact native operands and honest NULLs.

The final schema has exactly three private append-only decision tables (request/component/outcome), their native foreign keys/version seek indexes and owner-only ACL. No unused period table remains. The unchanged native0310 Rule insert guard permits the retained manual-debt creation/funding lineage; the new future rule changes no approved principal or original automatic-collection Rule. Apply's original Candidate financial fence precedes Case/component money locks. Receipt proof runs only after original STEP, the source prefix guard retains older immutable accepted cuts, and whole assembled reply/token/cursor bounds remain120000/32768/8192 bytes. All five concrete findings and the native percentage/Candidate facts requirement are closed at these saved bytes.

## Local compiler correction / current hashes

SOURCE PASS retained after re-reading the corrected unpublished migration and finite aggregate casts. Current1646 SHA256 `FEC5DD533F37C45F26E46011A551009AE8D62FB93A18862E082B2ACF74197456`; current1649 SHA256 `7F8DAE48B95E67950AAB1D3E898F60B912FD119274D4C3EE7B5798FAE8C68C5A`. The current migration-lock entry matches that1649 hash. Design hash remains `E23A7C7E79469C0BB6959DA1F752F26F57AFA82B4C2707C358600DE94A84E68E`.

Root's first local compiler found an extra closing parenthesis on the outcome CHECK, missed by the earlier static receipt. The corrected1649:113–118 now has the original two closed APPLIED/BLOCKED arms and the proper CHECK terminator; no predicate/NULL semantics changed. Root reports a fresh local catalogue proved all three tables and the migration ledger entry absent before correcting and re-locking its unpublished bytes. That absence/load evidence is root-reported, not an independent database observation by this reviewer. The four issue-code aggregates at1646:215/299/366/605 now use `min((q-or-x->>'issue_code')::text)`; JSON text extraction was already text, so the casts expose the same native type to the existing source gate without changing classification.

This receipt is a read-only source review, not a compiler execution or proof of business acceptance. Root's local reload/check results remain separate evidence; CM13a consumption, whole ITEM and runtime/visual acceptance limits above remain unchanged.

