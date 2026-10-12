# DR-14 frozen capacity source review

12 October 2026. Independent read-only review of BD133, both investigations, uninstalled migration `12102026_0404_banking_pay_next_captured_other_draft_capacity.sql` and repeatable `12102026_0408_banking_pay_next_other_draft_preparation_v1.sql`. Product/checklist unchanged; no business SQL, database calls, fixtures, tests, compilation or runtime observation. D05 remains **BLOCKED_ON_DR37** and the whole item remains open.

## C01 — new foreign-hold aggregation lacks a finite row bound

Current 0408:319–327 computes the diagnostic by summing every other ACTIVE hold in the Case. The native ACTIVE Case index (`03102026_1324...core.sql:1021–1022`) avoids released history and is a useful exact Case seek, but a Case can have many simultaneous active arrangements; it does not impose a finite number of visited rows. The new SUM is repeated for every captured instruction. A bounded outer capture page is not a row bound on this inner aggregation. Do not claim constant/bounded work from that index alone.

The smallest correction is to use the already locked maintained Case/component/period ACTIVE counters, without a foreign Case scan. The current fresh capture ordering proves the relevant own contribution is zero: 0408:730 enters INSTRUCTION_CAPTURE; all selected instructions are captured before the state becomes ALLOCATE (`820–823`, which exits the page); the positive ACTIVE Case hold INSERT occurs only later in ALLOCATE (`887`). A retained issue saves the exact prior stage/capture cursor (`981–984`), and current DR05 retry restores that stage (`2259:2813–2817`), so a retry with partial ALLOCATE holds resumes ALLOCATE rather than opening a new instruction-capture pass. Retained frozen instructions are not recreated. No new business refusal is needed to enforce this diagnostic premise.

Use the original unique `(run_id,candidate_id)` worker lookup (`1324:492`) and the existing partial ACTIVE worker index `04102026_0450...case_cancel_binding.sql:55`, `(run_worker_id,id) WHERE status='ACTIVE'`, for a point `EXISTS` of this economic worker's own ACTIVE Case hold. This checks the premise with at most one matching row; no own SUM or full Candidate/Case inventory is required. When no own ACTIVE Case hold exists, the foreign contribution is exactly:

- recovery Case = locked `case.active_recovery_hold_amount`;
- recovery component = locked `component.active_recovery_source_ex_vat`;
- recovery period = locked `period.active_unrealised_recovery_source_ex_vat`;
- payout Case/component = their locked `active_payout_hold_amount` / `active_payout_source_ex_vat`.

Keep realised recovery, current principal, written-off/paid amounts and all original eligibility/nominal caps unchanged when restoring those foreign deductions in the observational counterfactual. Existing native CHECKs bind these maintained counters to finite exact capacity, and the native typed hold/use writers are the only current positive reservation owners.

If a genuinely other read context carries an own ACTIVE reservation, do not restore that own amount or throw a new financial refusal. Either use separately proved bounded own contributions or leave all three diagnostic fields NULL (honest unknown) for that context, preserving the authoritative capacity and legitimate retained input. The normal named fresh capture path above has zero own contribution; a diagnostic point guard/fallback does not alter its money or replay behavior. An old accepted-run read used outside that capture path is not authority to assume zero.

## Current narrow changes otherwise checked

The migration adds exactly a source/target penny pair and a finite nullable reason on the existing immutable instruction, plus two narrow read indexes. All-old-NULL is preserved without backfill. Its CHECK explicitly distinguishes NULL/NULL/NULL, known 0/0 with NULL reason, and strictly positive paired values with SAME_WEEK_INSTALMENT/CASE_BALANCE; each reduction is bounded by nominal minus original usable capacity. Half-null and one-sided-positive shapes fail closed. The existing full-row instruction immutable trigger (`0310:286–289`) covers the new columns automatically.

The new plan keeps the original authoritative `v_capacity` calculation unchanged (`0408:303–312`). The counterfactual (`328–338`) restores only foreign ACTIVE reservation deductions, retaining realised recovery and outstanding principal; it stores the difference from actual capacity, rather than the raw foreign hold sum or nominal-minus-allocation. Same-channel PAYE identity valuation is still checked (`353–356`); copied source/target diagnostic values introduce no conversion or calculator authority. A floor/headroom reduction is not inferred from these fields.

Positive SAME_WEEK_INSTALMENT currently requires a positive causal difference, foreign period activity and the actual period residual being an attained capacity minimum (`339–342`). This is a captured capacity cause; it is not by itself authority to say the final allocation became zero solely because the whole week's instalment is held elsewhere. The later renderer must combine the native frozen nominal/usable/allocation and cause facts, and must not reinterpret CASE_BALANCE as a same-week exhaustion.

The named prepare successor appends the three fields to the explicit instruction INSERT/SELECT (`796–806`) after the original Case/component/period scope, exact prospective-period recheck and post-write revision capture (`763–795`). The monetary columns, original economic revision, immutable selected Rule B cut and positive hold/result loop are retained. Metadata is captured at actual resumed instruction capture, not reconstructed from the original command's timestamp or later live hold history.

## Status and saved bytes

**SOURCE OPEN — C01 correction pending.** Exact whole-parent inversion is being checked separately before final PASS. This review does not assert that later native readers/Office transport are already complete, and it does not close D05 or concurrent application acceptance.

| Reviewed saved source | SHA-256 |
| --- | --- |
| migration 0404 | `36569801cb8b8d8989150779e7e4d9aaf6590c169aa6e792ed86ee1d314eb225` |
| repeatable 0408 | `016be228b8568c108c88c293070798703179e9634817e22d75a299fc8975091a` |
| current pure-plan parent 0207 | `15518a516d7e4e1264e44b949ee432b694418daafd9df9fdaf6fed22abca9f27` |
| current prepare parent 2129 | `cdfd284bf1026875c30b8b472719dc31650e5a4f783e50ce9e241024407c84a8` |

## Final current-source closure — C01 fixed, SOURCE PASS

Fresh saved 0408 SHA-256 is **`278096454907768263320ba6d53eaa28bce367c79b30fac08597e86bc2845698`**. This supersedes the earlier 016be228 source snapshot and OPEN verdict. Migration 0404 remains **`36569801cb8b8d8989150779e7e4d9aaf6590c169aa6e792ed86ee1d314eb225`**; current BD133 scope is **`25499d8010f16ca49136e9e3079cf8d1dcb39622c62aeb96a2cd5968d3375ee9`**.

C01 is closed. Current plan `319–325` resolves the unique own run/Candidate worker and checks only its ACTIVE hold presence using the existing partial worker index; an exceptional own-hold context sets the captured amount pair to NULL and reason remains NULL. Normal fresh INSTRUCTION_CAPTURE has no own Case ACTIVE rows at all, as proved above, so this native point has no own rows to scan. No false financial refusal, discarded prefix or recapture of retained instructions is added. The current Case predicate is an additional residual qualifier; no universal constant-cost claim is made for arbitrary private callers that already carry many own holds. It does not affect the finite no-own native capture path.

When no own hold exists, `329–331` now takes the exact already locked period/component/Case ACTIVE counters. No foreign hold SUM, foreign-run enumeration or history scan remains. `332–346` compares the original constrained capacity with the same operands after restoring only those foreign deductions; realised recovery, recovered/written-off/paid principal, original nominal and own reservations are preserved. The final amount is the positive causal capacity difference, not a guessed outstanding/affordability difference. All-zero produces a known zero pair and NULL cause; unsupported own context and historical rows retain an honest unknown triple. SAME_WEEK_INSTALMENT and CASE_BALANCE remain distinct; current readonly estimates do not gain captured financial authority.

Static full-function inverse comparisons passed for both current parents: remove the one diagnostic declaration, the capture-only block and three JSON members from the complete plan, and its normalized text exactly equals the current 0207 plan statement. Remove only the appended three named INSERT columns/SELECT operands from the complete prepare, and its normalized text exactly equals the current 2129 prepare statement. Thus original nominal/capacity math, ALLOCATE branch, 0290 call, periods/rules/funding, original money locks, all result/hold/use mutations, replay/cursors, E2/retry economic identity and terminal projection handoff are unchanged. These are static source comparisons, not runtime tests or a raw-byte claim about formatting.

The existing whole-row instruction immutable trigger covers all three new fields after insertion. The additive nullable CHECK introduces no historical rewrite, current rate lookup, new table/command/protocol, financial counter, acquisition mechanism or calculation owner. The two supporting read indexes remain narrowly qualified to live native owner and positive captured reduction. No essential source defect remains in the reviewed migration/plan/prepare capacity slice.

**Final verdict: SOURCE PASS for the frozen diagnostic schema and exact plan/prepare successors only.** Root reports the sole local PG17 compile; this reviewer did not execute it or independently query a catalogue. Reader/Worker/Office/A4 closure is separate, D05 remains BLOCKED_ON_DR37, and whole-item/concurrent/business/application acceptance remains open/NOT_RUN.
