# FC-14 reader source review

Review mode: independent read-only source review of BD121 and the new 2129 migration/repeatable. No SQL installation, business invocation, fixture, test, browser, screenshot, commit or checklist change was performed by this reviewer. This receipt does not establish runtime or visual acceptance.

## Current verdict: OPEN — essential source corrections

1. **P2 — cursor is not a closed typed continuation.** In `11102026_2129_banking_pay_next_case_audit_v1.sql:334–345`, the admitted two-member key has only its first boolean checked; the second member can be an object/array or have a contradictory null discriminator. Source is unrestricted text and event ordinal is accepted via `->>` without requiring a JSON string. Qualify the native source/section/ordinal combinations and the exact scalar/null key representation produced for the selected sort. This is a small reader-contract correction, not a new proof system.

2. **P2 — issued cursor may exceed its own next-request limit.** The request cursor is capped at 16,384 bytes at line335, but the emitted cursor at lines392–393 has no corresponding check. Full legal Client/Reason/Note sort values can exceed that bound (the current text sort value uses UTF-8 hex, which doubles bytes), while the enclosing reply still fits 120,000 bytes. A page can therefore return a continuation that the same reader refuses. Preserve the full sort value; use the existing bounded canonical scalar transport pattern or a truthful named bound refusal, and enforce the issued cursor bound before returning it.

3. **P2 — a cleared current Note reappears.** Report source line204 uses `coalesce(er.input->>'note',cr.note)`. A current approved edit with `note:null` intentionally clears the original Note; the existing FC09 reader chooses the whole amended input when present (`1939_case_edit_v1.sql:73–78`). Select the exact current edit's nullable Note when that edit exists, rather than falling back because its value is null.

4. **P2 — real shortfall is labelled as a balance posting.** The CASE_EVENT branch at lines111–124 omits native `SHORTFALL`, yielding “Case balance changed” and “Money recorded”. The unchanged posting owner at lines1718–1723 stores an actual SHORTFALL with zero monetary deltas and native `shortfall_source_ex_vat`. Present that actual event without claiming money or balance movement; retain its actual source timestamp and identity.

## Confirmed scope and retained behavior

The earlier unbounded full Case DTO packing defect is closed in the current source: report sources now return scalar typed values (lines186–245), and `candidate_case_report_row_v1` invokes the original Case row reader only for the selected page (lines247–265,385). Full-scope sorting/counting may inspect the native scope; this review does not claim constant-cost or universal timing. JSON rows are capped at 100, request bytes at32KiB, response bytes at120,000, with deterministic native source/id/ordinal ties.

The audit uses real accepted request times, Case-event/effect times, actual stored snooze events and the separately recorded native hold transitions. It distinguishes accepted request labels from completed changes. No current mutable hold is expanded into fictional old history. NET request membership is correctly tied to its exact DRAFT allocation state and selected Case instruction; the current 0559 acceptance stores the captured economic preparation revision (`0559_worker_reprepare_v1.sql:2932–2934,2971–2976`), matching the audit join at137–142 rather than the later execution revision.

The public preparation-page successor adds only the two named reader branches after existing service/admin/NEXT authority checks (lines1814–1821). Every new private helper remains owner-only; the existing public signature alone is granted to service_role (lines2094–2108). Candidate report balances come from native maintained principal/funded/recovered/writeoff/hold counters, not browser calculation or historical money summation. Final A4/native caller and Worker DTO review belongs to their separate reviewers.

Further exact interfield qualification (unsupported component membership and summary null semantics) is being checked against the existing Case row reader; no PASS is asserted while the corrections above remain open.

## Reviewed bytes

- `supabase/repeatable/11102026_2129_banking_pay_next_case_audit_v1.sql`: SHA256 `4556181dab7f4b280dc40ae47dca18dcaae83d24d3b6f0bcc2638bbfbc18bc48`.
- `supabase/migrations/11102026_2129_banking_pay_next_case_hold_audit.sql`: SHA256 `e6cb93b8f55ee253ff9fdfe9f3eb025448fa4128fcefbcee1c50d09fe48a8fd1`.
- `docs/bpay-next/design/FC-14-builder-design.md`: SHA256 `8211621f57ec1a1c17fdf2af107e7ba40ad0cd55bd2993de960976922638eb4a`.

Authority: complete FC14 card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1316–1337`, all current FC14 checklist rows, BD121, and the saved native investigation/design review. Required history is factual, bounded and auditable; future FC12/FC13 and FC02/EX50 delivery remains with its scheduled owner.

## Follow-up source check — 11 October 2026

Fresh SQL SHA256 `bcad42d95491d5f9b8f4194740bac208cec74872ef2f4b6a8fc6c8467b88a898` closes the original arbitrary key/null/source/string-ordinal shape problem, adds issued key/cursor byte refusal, preserves a deliberately null current edit Note, and gives native SHORTFALL truthful labels. CORRECTED already includes the actual nonzero approved_delta at line117; no correction was required.

Two additional concrete source conditions remain:

- **P2 — retain the edited Note through its genuine funding successor.** Report source currently joins edit.new_rule_id directly to Case.current_rule_id (line208). First Loan/Advance funding rotates that current rule to a new native funding rule (lines1634–1641,1672–1675). The amended Note, including an intentional null, then falls back to the creation Note. Follow the exact current rule's original_funding_event_id → actual event.instruction_id → captured instruction.rule_id before the edit point lookup, as the existing FC09 `case_edit_origin_v1` does at `1939:15–33`. Do not choose the latest history row.
- **P2 — automatic payout is not a Candidate NET input.** NET_REQUEST (lines133–142) includes native input_kind=CASE_PAYOUT. Original0559 admission stores entered_paye_net=null for that actual payout request (`2954–2960,2971–2976`). Qualify the audit event/detail wording by input_kind so it does not claim that a Candidate NET was entered for CASE_PAYOUT.

The cursor's newly explicit 8192-byte encoded-key refusal now prevents invalid continuation output. Root is aligning native text8192 and UTF-8 hex key16384, retaining the separate 16384-byte whole emitted-cursor bound; this alignment needs a fresh final check. Current shared read contract SHA256 `0fa9738b5c0ac7223c8c31e85bdf470ce6cd9e56696c0a0791ba235620af88d0` still has text4096/key8192, so final cross-layer closure is not yet asserted. The supporting migration remains unchanged at `e6cb93b8f55ee253ff9fdfe9f3eb025448fa4128fcefbcee1c50d09fe48a8fd1`.

## Fresh closure check — remaining aggregate contract condition

SQL SHA256 `885a05ca10580911f8ab2b43a073c8a02f97154cdc22a580f4439c141f273be3` closes the six earlier reader conditions. The Note follows the exact native funding event/instruction/approved rule and preserves a null edit value; CASE_PAYOUT has its actual request wording; displayed negative recovery amounts and AMOUNT sort now use the same scalar; text8192/hex16384 and whole issued-cursor16384 limits align; unsupported first-three component census produces nullable snooze facts, and the full-scope snoozed-items count remains null when any source status is unknown.

Static normalized-source comparison confirms five complete writer successors reproduce their exact current parents after removing only the six recorder calls (prepare1939:1, NET0559:2, cancel0559:1, post0410:1, release0814:1). No other predicate, calculation, replay return or lock behavior differs in those five bodies.

One final **P2** closed-contract condition was reported: the two full-scope report summary SUMs can exceed the maximum of a single native numeric(18,2) Case. The current shared CF_MONEY pattern allows only16 integer digits. Two legal outstanding amounts of9000000000000000 each yield18000000000000000, which SQL returns honestly but the Worker rejects as malformed. Admit these two exact native SUM fields with a separately bounded aggregate decimal validator, or refuse deliberately with the native named bound before returning; do not cap or calculate the sum in the browser. Shared contract at this check: SHA256 `a1c76f40b87f113497edc573551b375214e718d35ddc81a5605430f5787c1fc1`. Registry current signatures/owners all point to2129, SHA256 `337d9895e0579ea306c5279d8706b42209e372771e8014a094e9b65055c6b365`.

The earlier concern about component-dependent fields is closed for the actual output: maintained Case monetary counters remain known, pending payout uses the bounded original reader's nullable gap, and weekly/next-date are actual current rule metadata rather than an invented allocation due. No new money owner, history projection, snapshot certificate or trigger framework is required.

## Final source verdict — PASS (11 October 2026)

This section supersedes the earlier OPEN conditions. All reported essential reader defects are closed in the final bytes below. The separately bounded aggregateMoney validator permits36 integer digits for the two native report summary SUMs, while individual money remains at the original per-Case bound. No browser derives those totals.

The reader retains actual timestamps and unique source/id/ordinal identities; exact economic NET state/instruction membership; known HUMAN/SYSTEM/UNKNOWN actor distinctions; all native Case-event kinds including truthful SHORTFALL; nullable edited Notes through the real funding successor; and separately stored real hold transitions. Recovered display_amount and its AMOUNT sort use the same signed scalar. Literal source/target money remains unchanged. Candidate report uses maintained native Case money and the original outstanding semantics once per Case, current exact rule metadata and qualified native snooze bases. Unsupported membership produces unknown snooze tuple and nullable full-scope count rather than a prefix or zero.

Request limits, selected100-row DTO packing, response120,000-byte bound, string counts, typed/null-aware sort keys, source/section/ordinal shape, deterministic ties, context/scope requalification, and both input/issued16KiB cursor limits are present. Full native text is retained up to8192 bytes, hex scalar up to16384; a whole cursor that does not fit fails with the existing named bound. No truncation, guessed continuation, browser money calculation or new snapshot/proof owner is introduced.

Read access-path census: original Case event history index2013 covers full Case/time/id; creation uses its unique Case key; original Case/Candidate1020 and Work/Candidate995 paths cover report scope; original work-based snooze history1100 covers the Candidate's own Work histories and Case control-point0125 covers bounded component snooze points. Original pause-outcome2041 and new2129 edit/pause/effect/transition/Case-snooze and exact instruction+NET-state indices cover the named native sources. Write-off requests retain their original Candidate/command access and Case predicate; no unrelated agency inventory is introduced. The union's computed text/amount sorts require native scalar residual ordering; this is explicit in BD121 and is not claimed to have a single global index or constant cost. No broader audit projection or index framework is needed for source closure; actual timing remains unrun.

The five writer-copy comparisons remain exact normalized-parent matches after only six recorder calls are removed. The public signature and authority order remain as reviewed; private helpers/table are owner-only, immutable history has RLS and mutation/truncate guards, and no new public RPC/route/financial command/job/queue is added.

Final reviewed SHA256:

| File | SHA256 |
|---|---|
| supabase/repeatable/11102026_2129_banking_pay_next_case_audit_v1.sql | `885a05ca10580911f8ab2b43a073c8a02f97154cdc22a580f4439c141f273be3` |
| supabase/migrations/11102026_2129_banking_pay_next_case_hold_audit.sql | `e6cb93b8f55ee253ff9fdfe9f3eb025448fa4128fcefbcee1c50d09fe48a8fd1` |
| broker/src/bpay-next-case-audit-contract.js | `1798d3a1622b68efe4e89d2a1e3b527642ca92513f72e03c62ddce364e907fa1` |
| docs/bpay-next/final-definer-closures.json | `337d9895e0579ea306c5279d8706b42209e372771e8014a094e9b65055c6b365` |
| docs/bpay-next/design/FC-14-builder-design.md | `8211621f57ec1a1c17fdf2af107e7ba40ad0cd55bd2993de960976922638eb4a` |

SOURCE PASS only. SQL compile/catalogue and visual/Worker/native integration are separately owned checks; no runtime, business, timing, hosted or joined acceptance is asserted by this independent review.

## Immutable term detail amendment — SOURCE PASS retained

Fresh source read confirms creation details use only the actual immutable request's weekly amount, week count, start/due dates, threshold, floor override and saved Note. Edit details use the captured previous/new principal plus the saved native terms/input, including due_date. Nullable edit Notes say “No note recorded”, avoiding a claim that a prior Note was cleared or that an accepted request already completed. Both request branches retain their original accepted timestamp, human actor, source identity and pending/request wording; no current mutable terms or newly fetched financial values are substituted.

These are bounded text details within the existing row/response contract. No helper, object, public signature, money equation, hold/capacity owner, journal or command is added. Dates use the actual saved date in UK order. The existing page remains bounded and sorts DETAILS on the same native detail string. The thirteen final registry owners still resolve to their reviewed2129 bodies; this review itself did not compile or invoke them.

Final superseding SQL SHA256: `cdfd284bf1026875c30b8b472719dc31650e5a4f783e50ce9e241024407c84a8`.

Unchanged supporting hashes: migration `e6cb93b8f55ee253ff9fdfe9f3eb025448fa4128fcefbcee1c50d09fe48a8fd1`; registry `337d9895e0579ea306c5279d8706b42209e372771e8014a094e9b65055c6b365`; shared read contract `1798d3a1622b68efe4e89d2a1e3b527642ca92513f72e03c62ddce364e907fa1`. Earlier source findings remain closed. Native visual formatting and actual pixel acceptance remain with A4.
