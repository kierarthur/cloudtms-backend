# DR-13 SQL source review

12 October 2026. **SOURCE PASS for the three reviewed SQL files; DR-13 ITEM is not complete.** No essential source defect found within BD135's original-observation contract. D03 and the WAIT-specific D05 acceptance remain explicitly BLOCKED by the retained-membership contradiction. SQL compilation, migration installation, business/queue journeys and runtime/timing checks were not performed by this reviewer; joined acceptance NOT_RUN. Root alone writes product.

## Reviewed authority and bytes

Read full card03:357–376, R3:124–130, every current DR-13 ITEM/D01–D06 checklist row, `design/DR-13-build-scope.md` and the three complete files. D01/D06 are retained context, not additional allocation-owner tasks. BD135 expressly selects original instruction/selection observations, not later NET-time reservation facts. Owner E2's system release supersedes the older no-pay manual-removal example.

| Saved file | SHA-256 |
|---|---|
| `supabase/migrations/12102026_0552_banking_pay_next_frozen_recovery_balances.sql` | `B5DBDC84BE7669DA4FE65D79AB352269E9AEC4E2B5A10C37CE3621D7CC002A86` |
| `supabase/repeatable/12102026_0552_banking_pay_next_recovery_balance_capture_v1.sql` | `2A99688A8C754ED47419D2D6398C894C2321E330CBD8D51F603F64F288CECFBA` |
| `supabase/repeatable/12102026_0552_banking_pay_next_recovery_explanation_v1.sql` | `36D4639005DBA6CD7B43EF406E14623F22DD6D692717F38E509D2EFE40E6DA45` |

Historical first source checkpoint: these were new untracked saved files and the registry still identified the two capture parents in `12102026_0442_banking_pay_next_run_channel_totals_trigger_v1.sql` and the reader parent in `12102026_0442_banking_pay_next_run_review_totals_v1.sql`. Complete function-body comparisons against those parents show only the stated capture operands, added balance CTE/output, and named reason wording changes. No prepare, pure plan, row kernel, NET owner, command, job, hold writer or permission owner is replaced by this batch. The current registry and root's local installation evidence are recorded below; the initial pending wording is historical.

## Frozen capture and immutable storage

Migration0552:4–11 adds exactly two nullable matching-purpose reserved observations to the existing instruction. The pair CHECK rejects partial-NULL operands and requires the existing component/Case outstanding pair for any new reservation facts. Historical NULL remains permitted; it creates no default or backfill. Excluded0552:12–20 adds exactly three nullable original observations; the all-NULL or all-present/time-bound branches cannot pass a partial tuple through CHECK UNKNOWN. The existing penny domain admits only finite, exact penny amounts (core0300:13–15), and the new nonnegative checks do not round input.

Capture0552:9–24 preserves the existing principal expressions and exact Candidate/Case/component/channel/captured-revision point joins. It reads recovery counters for RECOVERY and payout counters for CREDIT/PAYOUT, not the aggregate mixed-purpose `.active_hold_amount`. It overwrites the new instruction metadata at INSERT (27–30), rather than accepting caller values. Current parent0408:734–745 already holds exact Case/component/period locks before DERIVE and instruction INSERT; capture adds no lock and no financial mutation. The original result/hold allocation occurs afterward. The retained generic fact-immutability trigger covers all columns on instruction and selection item (0310:286–293; immutable helper1342:188–198), so the additive fields cannot be refreshed by UPDATE or retained Retry.

Excluded0552:41–45 resets observation fields and returns immediately for selected membership. Its single SELECT preserves the immutable original/inherited LEAF Rule/revision tuple and native component/Case identity (60–68); no current Rule substitution or extra business lock is introduced. The existing selection-time timestamp remains the observation's scope. The new CP/CR/effective-unreserved facts are assigned together (77–80); failed original qualification remains unknown. The parent CASE_PAGE replay returns before INSERT, as documented in the investigation, so identical replay does not recapture these observations.

## Reader facts, units and reasons

Reader0552:34–58 preserves exact original selection, captured economic preparation/selection revision, completed DRAFT state, current accepted NET projection and unique instruction-result binding. It contains no live Case/component/hold/period money lookup. The existing work-collection identity-only check in the historical original-basis branch is retained, not a current money source.

At 79–90, CP and CR remain component source facts. Accepted `allocated_target_ex_vat` supplies recoverable-this-run only for deductions; source A supplies the residual:

`least(greatest(CP-CR-A,0), greatest(FP-FR-A,0))`.

This is the agreed original-basis remaining-unreserved explanation, not later/current foreign capacity, settled recovery or weekly due. The Case aggregate limit is not added to the component reservation. Its note is present only when the captured Case-side balance is strictly smaller; excluded rows compare their stored effective balance against CP−CR. Ties do not claim one limiting side. PAYOUT/CREDIT use their matching purpose observations but do not invent a recoverable deduction amount. `this_draft` retains target units; outstanding and residual retain source units. The existing accepted NET owner independently bounds NET capacity by original DRAFT allocation (2129:1191–1197), so this reader introduces no financial authority.

Historical reservations remain NULL because the selected residual is gated on a present reservation observation; excluded absent observations are returned directly as NULL. Known older principal or actual accepted result can remain visible independently. There is no fake historical reserved0. Output retains captured/selection observation time (140), result basis and native allocation codes. New wording distinguishes genuine zero PAY_HEADROOM/TAKE_HOME_FLOOR from positive capped recovery (113–115). The existing captured causal evidence is still required for SAME_WEEK_INSTALMENT/CASE_BALANCE wording (99–101); CASE_CAPACITY alone does not establish another Draft. CASE_NO_PAYABLE_AMOUNT is not introduced as a row cap reason, and no E2 system-release behavior changes.

## Inherited safety and remaining boundary

The current context/completed-offer guard, sealed selection, ten finite native sorts, exact tail validation, deterministic component tie, p_limit+1 window, original whole-scope counts, cursor and 120000-byte final checks remain (reader0552:10–33,117–161). Private functions remain STABLE/security-invoker where applicable, with bounded existing point joins and all PUBLIC/anon/authenticated/service_role direct EXECUTE revoked; the existing admin/service public wrapper invokes this reader (parent0442:287–303). No new grants, public trigger or exposed table are introduced. Worker/Frontend closed-schema amendments and actual presentation are separate ongoing deliverables, not passed by this SQL receipt.

The dormant ORIGIN_UNAUTHORISED_WAIT interpretation remains unchanged (63–64,98,108). Current WB41 informational WAIT is excluded from accepted Create LEAF membership, and Rule B bars a later live withdrawal substitution. This review therefore does **not** claim a genuine accepted WAIT producer or mark D03/ITEM complete. No result-time trigger, old-hold proof, transition reconstruction, new result columns or NET-owner edit is needed or recommended under this scope.

## Current local catalogue follow-up

Read `design/DR-13-current-owner-catalogue.json` and the freshly saved `final-definer-closures.json` on 12 October. Current registry resolves both capture signatures to `12102026_0552_banking_pay_next_recovery_balance_capture_v1.sql` and `private.bpay_next_run_review_cases_v1(uuid,uuid,uuid,text,jsonb,integer)` to `12102026_0552_banking_pay_next_recovery_explanation_v1.sql`.

Root's sole-local-PG17 catalogue records three exact source/installed body matches: outstanding capture MD5 `8d98ad0a6530eafa003babce4b99802e`, excluded capture `8b820dc6e059683f2638496249a35741`, reader `75f1588f05b2738bd8df78a872544390`. All are security-invoker, fixed `search_path=pg_catalog, private`, local release-owner owned and owner-only EXECUTE ACL; trigger functions are volatile and the reader STABLE, matching the source. The saved general isolation tuple remains `839|76|0||338226c4e9b878c5aa92fc8e319dd7bf`, public security-definer membership delta0, and both saved browser-isolation verifiers report successful DO results.

This is **root-attributed compile/catalogue/isolation evidence**, inspected from saved receipts only. This reviewer made no database call. It establishes the local definition/configuration/owner/ACL correspondence, not business first use, hosted TEST installation, capped/NET/queue/runtime acceptance or a genuine accepted WAIT path. SOURCE PASS remains scoped to the reviewed SQL; DR-13 D03/WAIT-specific D05 and ITEM remain open as stated above.
