# FC-11 investigation — sequence 132

Source-only investigation, 12 October 2026. Root alone authors product. No database/RPC, fixtures, tests, hosted actions or checklist changes were performed. All joined acceptance remains NOT_RUN. This note records the current sources rather than marking the card built.

## Owning scope and current parents

Read the complete owning card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:1238–1266` and every FC-11 checklist row (ITEM and D01–D15). D02/D11, D03/D12 and D04/D13 are duplicate ownership rows, not additional function families. D07/D10/D15 are context; D14 preserves the installed 1647 migration. The card owns one per-component all-or-amount write-off with required reason, EX-01 reauthentication, protected Draft information and terminal date/result. Existing Work collection behavior stays intact; non-taxable/Umbrella OVERPAYMENT producer support belongs to FC-15/CM-13a.

Registry `docs/bpay-next/final-definer-closures.json` resolves:

| Function | Current physical parent | FC-11 disposition |
| --- | --- | --- |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` | `10102026_1236_banking_pay_next_segment_authority_final_v1.sql:4461–4515` | Reuse unchanged; strict taxable PAYE Work debt qualification. |
| `private.bpay_next_accept_write_off_v1(uuid,uuid,uuid,bigint,text,text,text)` | `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql:24–91` | Named successor, typed nullable-origin branch and correct P. |
| `private.bpay_next_apply_write_off_v1(uuid,uuid,bigint)` | `11102026_1509_banking_pay_next_case_change_guard_v1.sql:296–388` | Preserve FC-16 protection addition; add same typed branch/fence/refresh. |
| `private.bpay_next_write_off_guard_v1()` | `04102026_1647…:136–242` | Named successor covering request, result, event and counter tuples. |
| receipt, owner, claim and deferred commit guards | `04102026_1647…:8–22,93–134,244–266` | Reuse unless a narrowly required terminal date output is added. |
| `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` | `12102026_1215_banking_pay_next_case_net_replacement_v1.sql:4344+` | Existing ordered WRITE_OFF/EX-01 contract; preserve complete latest body. |
| `private.bpay_next_reauth_scope_v1(uuid,text,jsonb,boolean)` | `11102026_1358_banking_pay_next_paye_override_reauth_v1.sql:97–135` | Necessary supporting WRITE_OFF branch: currently rejects every NULL origin. |
| `private.bpay_next_segment_write_off_membership_v1()` | `10102026_1236…:4788–4828` | Necessary exact NULL-origin early return; all non-NULL membership behavior unchanged. |

The old 1647 apply is not the current apply parent. Replacing it directly would lose FC-16's fresh post-component-lock WHOLE_CASE/UNRESERVED_AMOUNT reproof (`1509:353–366`).

## Two exclusive origin branches

For a non-NULL request origin, continue calling the exact current Work origin helper and preserve the original Work/Position locks, equality of Work collection/Case/component IDs, segment attribution and PAYE taxable OVERPAYMENT tuples. Do not relax origin failures into the new branch. The current helper explicitly proves ordinary/Source sealed native Work, paid position, current original rule and OVERPAYMENT recovery identity (`1236:4468–4513`).

For a NULL request origin, admit only positively typed RECOVERY components belonging to LOAN, ADVANCE or MANUAL_DEBT, exact Candidate/Case/rule identity and matching source counters. P is funded for LOAN/ADVANCE and approved for MANUAL_DEBT; component P is the corresponding funded/approved source value. A payout/CREDIT, malformed tuple, unsupported OVERPAYMENT or unfunded Advance must not acquire write-off authority from a missing origin.

The additive migration makes only `work_collection_id` nullable. Preserve its native FK, monetary domains, request immutability and state shape. Replace the audited exact original equality CHECK (`04102026_1647_banking_pay_next_work_collection_write_off.sql:13,36,40`) with `work_collection_id IS NULL OR (work_collection_id=case_id AND work_collection_id=case_component_id)`. The trigger, not SQL NULL permissiveness, must establish the typed no-origin branch.

## Complete P and event identity matrix

For each listed point, use the same branch-qualified P rather than changing only the first refusal:

| Required location | Original 1647 anchor | Current behavior needing the new branch |
| --- | --- | --- |
| Accepted immutable basis | 72–75,80–85 | Origin is compulsory; `basis_principal` stores approved Case P. |
| Request insert proof | 146–153 | Exact accepted Case/component tuple uses approved P. |
| Terminal remaining/protected and component equality | 169–175 | Remaining and Case/component tuple use approved P. |
| Case counter before tuple and resulting status | 223–230 | Exact request basis and WRITTEN_OFF status use approved P. |
| Component counter before tuple | 232–235 | Component approved P is compared to basis P. |
| Apply balance and tuple | 307–311; current 1509:335–340 | Remaining and Case/component equality use approved P. |
| Event insert and result proofs | 179,192,326–327 | Event item ID currently Work collection ID. |
| Terminal apply replay | 292; current 1509:320 | Exact event item ID currently Work collection ID. |

NULL-origin event `operation_item_id` must be the real Case component UUID at all four sites; use null-safe exact identity checks. Non-NULL events retain Work collection UUID. The request's immutable basis P then governs the existing ALL/AMOUNT event-capacity checks (`1647:198–201`). W is forgiveness only: retain every recovered/funded/paid counter, prior holds/results/effects, historical R and schedule; no restoration/recreated liability.

## Ordered ownership, protection and projection

Acceptance already validates active admin, module epoch, same bare command UUID mutex and exact replay operands before new acceptance, then locks Candidate control → Case → component (`1647:33–75`). Claim/owner qualify same Candidate command ordering, native lease nonce/epoch/deadline and REQUESTED request (`1647:93–134`). Keep these owners.

Apply must retain exact DONE replay first, then native lease proof (`1509:314–329`); call current `private.bpay_next_assert_candidate_financial_fence_v1(uuid,uuid,uuid,bigint)` from DR-37 `1215:1525+` before fresh financial mutation. Its contention is a native retry/defer, not a durable financial REVIEW. Existing non-NULL Work/Position prelocks remain before Case/component; the NULL branch omits only those inapplicable Work locks (`1509:330–333`).

Preserve existing ALL refusal for any protected whole Case and AMOUNT permission only inside unreserved recovery capacity, including accepted-but-not-yet-held input. Current FC-16 maps PENDING to INPUT_PENDING and protected slice to PROTECTED_CAPACITY (`1509:357–365`); do not silently lose the sixth current code because the original NX-043 card enumerates five older ones.

The current apply increments Candidate financial-view revision but does not call the Workbench Case projection (`1509:377–385`). Card 1258 explicitly requires WB-08 Case + summary refresh after applied W. Reuse current `private.bpay_next_workbench_refresh_case_v1(uuid)` (`12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql`) under the existing locks, with financial revision bumped before refreshing. Avoid unrelated Work producers/ORIGIN tasks.

## Reauthentication and transport

Current public intake uses closed WRITE_OFF operands (command/component UUID, component revision, ALL|AMOUNT, reason, amount only for AMOUNT), requires only `reauth_proof_id` at SQL, takes native scope locks, consumes the exact EX-01 proof, then calls acceptance (`1215:4421–4453,4514–4536`). Preserve consumption in this same transaction and identical expired-token replay against retained request operands.

The current EX-01 WRITE_OFF scope branch has immutable accepted replay but a fresh request requires Work origin and previews approved P (`1358:116–134`). Extend only that branch with the identical typed NULL-origin qualification, P, component ID and stable principal/protection facts. No new proof table, resolver, signing or auth protocol.

The real Worker owner exists at `broker/src/bpay-next-command.js:64,83–85,232–233,362–374,441–477`; it verifies the action reauth token, records a closed proof, checks consumed command ID and forwards only proof UUID to SQL. Existing `CASE_WRITE_OFF` job map/closed result in `bpay-next-job-step.js:45–46,79,243–249` and ID-only queue kind are reusable. Current public job dispatcher calls apply and proves exact stored request result (`1215:4162–4163,4196–4205,4328–4332`). No new action/job/queue envelope.

## Office and design boundary

There is no native NEXT per-component write-off window yet. FC-02 currently shows its More action through optional `options.caseActions.writeOff`, disabled when absent (`FE/js/bpay-next-case-list.js:88–97`). Legacy `main.js:20962+` still calls the retired finance endpoint and is not a substitute for native WRITE_OFF.

SCR-124 and refusal/result variants are the owning B9 designs (`DESIGN_INDEX.md:139; SCREEN_INVENTORY.md:200`). Required native window facts: precise component/source-channel identity, outstanding P−R−W, protected Draft facts, all/amount, reason, EX-01 auth, destructive Apply, terminal W/remaining/protected/date and known-code English. Reuse current frame/actor/NEXT namespace, existing bounded CASE_COMPONENTS/protected-slice reader and EX-01 launcher/controller; unknown outcomes replay identical original operands/command/proof before any fresh edit. No future FC-15 producer, restructure, source history or whole-Case financial action. Pack's 80/30/50 example does not authorize inferred Draft references.

## Finished narrow source checks

The native funded balance columns already exist; no money-column migration is necessary. `04102026_0300_banking_pay_next_case_instruction_core.sql:84–90,107–121` constrains RECOVERY identity and its R+W+H ceiling against component funded P for LOAN/ADVANCE, approved P otherwise. Case `bpay_next_case_principal_balance` does the same (`0300:476–481`). Funding updates both the payout and paired recovery component's funded source amount before increasing Case funded principal (`0410_banking_pay_next_case_transfer_post_v1.sql:456–485`). The reference recovery-capacity authority is `04102026_0390_banking_pay_next_case_paye_net_v1.sql:415–423`: component and Case P−R−W less disjoint active recovery holds. FC-11 can use these existing source-channel counters without valuation or a new calculator. Existing generic Case-event guard only adds allocation-result proof for FUNDED/RECOVERED/PAID/SHORTFALL (`0310:127–154`); its WRITTEN_OFF branch does not need rewriting.

**Two existing Worker equality checks are incompatible with the new branch.** `bpay-next-command.js:363` and `bpay-next-job-step.js:244` require Case UUID = component UUID, an automatic Work-collection invariant. Native manual repayment component IDs are independent UUIDs (`0300:68–104`). Remove only this cross-ID equality for generic typed WRITE_OFF results; retain exact requested component identity, result state/money/event/replay constraints and server Candidate/Case FK checks. Otherwise the new SQL mutation could commit and its valid response be rejected. Current sixth INPUT_PENDING enum is already present in both modules and the exact installed CHECK extension (`11102026_1509_banking_pay_next_case_protection_lookup_indexes.sql:14–19`); no blanket enum relaxation.

There is no separate public protected-slice page to invent. Existing `CASE_COMPONENTS` rows already contain `pause_state.protection_summary` (FC-16 recovery capacity, count, protected amount and bounded cursor) plus `pause_state.first_protection` (native Run/worker/channel/status and next action). Its current helper positively calls FC-16 FUTURE_ONLY and asks for one binding (`11102026_2041_banking_pay_next_case_pause_v1.sql:10–31`); component rows include this state alongside source counters (`2041:1861–1904; broker/src/bpay-next-preparation.js:381–384`). Read those facts, display unavailable protection honestly, and keep the apply-time guard as authority. Existing edit-state `protection` is also just the first binding (`1939:91–105`); it is not a complete amount list. Do not call a mutating private assert directly from Office.

**Terminal status and write-off date need no new reader.** The existing Candidate-scoped payment-history reader exposes one exact `WRITE_OFF` request, including all operands, status, applied/remaining/protected amounts, issue/event and `completed_at_utc` (`10102026_0653_banking_pay_next_ps_c16_indexed_history_v1.sql:115`). COMMAND parent authorization is exact `command_member(command_id,candidate_id)` (`0653:271–273`); the Work-collection join at 292–293 applies only when a Timesheet/Work scope was supplied. Thus a no-origin repayment remains readable through a Candidate scope without a new page-kind/route/table or CASE_COMPONENTS optional receipt.

Use existing `CloudTMSBpayNextPaymentHistory.createClient` with that Candidate scope: `manifest()`, then `section({section:'WRITE_OFF',parent_id:originalCommandId,context_id:null,component_key:null})` (`FE/js/bpay-next-payment-history.js:244–308`). Qualify the exact row's command/component/Case/Candidate and retained request operands before acknowledging the accepted journal. Format terminal `completed_at_utc` as London DD/MM/YYYY; do not substitute client time or today's Case history. REQUESTED has no completion date. This read also lets an accepted request survive close/reload without replaying fresh proposals or exposing browser financial STEP calls.

Current Office `financial`/`reauthAction` finite sets do not include WRITE_OFF (`bpay-next-office.js:726–727`); input/result schemas do not include it (242–291). Add only the existing native WRITE_OFF action to these closed contracts, exact token expectation, receipt/controller journal adoption and existing EX-01 preview launcher. Existing controller dispatch retains unknown original bytes and clears an initial definitive refusal only (`1595–1617`), which must remain unchanged. The exported `openBpayNextReauth` and `createController` seams are already real (`1738–1832,3201`). The full background financial mutation route already exists; no new route/job or queue body.

Workbench refresh itself updates the Case group and invokes the existing contribution delta/summary owner, so a second summary formula is unnecessary (`12102026_0207_banking_pay_next_origin_wait_recovery_v1.sql:611–617`). It observes the newly bumped Candidate financial revision and acquires the existing Candidate/Case locks (`457–468`). The named apply can append this one existing call after W and the financial bump within the same transaction.

## Remaining scope and dependencies

The approved chain places FC-11 immediately after DR-38 and before FC-12/FC-13/FC-15 (`08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:213–219,517`). FC-02, FC-16, EX-01, WB-08, F-16 and DR-37 provide the current native seams; preserve the latest parents rather than rebuilding them. FC-12/13 restructure and FC-15 non-taxable/Umbrella OVERPAYMENT creation are later ownership. Current FC-02 More → optional writeOff handler is a genuine required launcher seam; outstanding post-payment recovery can reuse the same native Case/component launcher from an existing Candidate Case/report context without constructing future execution trees.

Card 1248 separately names cross-basis partial-write-off comparison refusal. This investigation found no native current `CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED` writer or typed moved-comparison binding in this chain. Do not infer that reason solely from today's Candidate channel or invent a CM carry/compare framework. Preserve current strict non-NULL origin and document the later CM-02/FC-13 comparison handoff; any presently supported no-origin branch must still have exact coherent source counters. If Root chooses to expose the named future comparison reason now, it requires actual retained comparison evidence and corresponding finite request/Worker issue mapping, not a guessed blanket refusal.

## Source checkpoint

SHA-256: 1647 repeatable `77019761E500193DDD5D8C4147AA0DB5BF389C2D1DEB02D576407548C0ED4A87`; current FC-16 1509 `087C3C5D3AE9CFAA94B66EA58B338358D0236F2B4953BE5EE6ADF7E3DFC60FD4`; 1236 `F74EA457BF77F0D85CE7F3F21089BDECF9D9D9121D4005850DE430D50AA7379D`; EX-01 1358 `1EA45B445CF57E2F2111E231E53E6E334539D76CE84281460C9B5257340855AC`; current DR-37 1215 `23AC3D72354EAC877D66E61E249DE0DB94F1F820BB7F96A90373C5D5425D72BB`; history 0653 `A6C76D789C3FFAE22ED7B7749FA0943807A16C0477CF8CBE2CD56E878C33CEC3`; registry `98774829C1A81FE7C71700C6A0BFCD2E21134840ABB9D5C59D5A1900C777E166`.

Worker command `F18B320E8AB3111B87686F94122F39C5C6747B149E0A062BEC500FE5BC9B6F4A`; Worker job-step `05A0A880732EB5A51721453D0325E5855FAD830C20D5B5C217C14FF77645AA1D`; FE Office `34804A80191481FCC010D7598E734B2345EF802C19E6FE9CE99D0C6E9FA191E6`; FE Case list `81762F7BFB98263ACB56B0D1AD191E925DFD703493EABABC1E521AA4CA0A0647`. Source investigation is complete at these saved bytes; app acceptance and all business-first-use checks remain NOT_RUN.
