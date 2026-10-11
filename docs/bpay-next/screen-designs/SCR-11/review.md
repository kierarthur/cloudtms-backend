# SCR-11 / WB-26 independent A4 policy review

11 October 2026. Reviewer: independent screen-review agent. **APPROVED for the exact local WB-26 native Case-component choice/display slice: 39 PASS, 0 FAIL, 3 N/A.** No remaining finding blocks this scoped approval.

This is the final review of saved source and all 33 actual product PNGs, superseding the preparatory observations. It does not approve the broader Candidate/Case aggregate selection (WB-24), real estimate/allocation display (WB-27), Case management/snooze actions (WB-38), or full Draft/execution flows. Installed and App tested remain unticked; financial/application first-use acceptance remains NOT_RUN.

## Scope and method

Read the WB-26 card and its 17 checklist references, saved `design/WB-26-build-scope.md`, applicable workspace/repository rules and Banking Pay Bible. The supplied design pack was read-only: SCR-11 maps to root T02 in DESIGN_INDEX, not future T02b. Reviewed SCREEN_DESIGN_POLICY, SCREEN_INVENTORY, T02 spec and supplied dark image. No gen.cjs/render.cjs/check-wording.cjs, product write, new specification pipeline, test programme, fixture construction, database/hosted action, deployment or financial side effect was performed by this reviewer.

The owner-directed evidence is actual locally captured product UI using saved fixed read responses. It covers fresh ALL_MATCHING and remembered-excluded baselines, Ready, repayment detail, repayment choice, independent credit choice and paused Waiting in dark/device-light/phone, plus Overpayment detail in all three modes. Every final PNG was opened; eleven dark/device-light pairs have identical SHA256/byte size. Phone images are 390px viewport cards; desktop images are 1600px viewport tables. No business-data or runtime journey proof is inferred from those fixed responses.

Reviewed source bytes are authoritative saved disk contents. FE HEAD at review was `49ebb10c2452519b34f1d41ddcf9b1d546d03a2a` (the committed WB-26 frontend product); the final WB-26 bytes were present on disk. The hashes below identify the reviewed WB-26 bytes independently of a later commit.

## Finding closures

| Finding | Final result and evidence |
|---|---|
| P26-01 — paused CSS escaped the modal scope | CLOSED. New rule is `#modal.ctms-modern-modal .bpay-next-case-paused { opacity: .65; }` (CSS line 296); no unscoped rule remains. |
| P26-02 — unallocated repayment/recovery displayed £0.00 | CLOSED. Root identified and reviewer confirmed the misleading zero; outer rows use Show estimate when Case deduction has no accepted run (workbench line 1379), including paused Waiting. Final Ready/Waiting captures confirm this. Factual Due is read inside detail; no extra outer Due field was invented. |
| P26-03 — Case detail had a redundant nested table box | CLOSED. Root identified and reviewer confirmed. `renderCase` now appends its one native table directly (workbench lines 1246–1261); all final expanded captures show a single native detail table/card. |
| P26-04 — Weekly limit money alignment | CLOSED. Final repayment and Overpayment desktop captures show the £80.00 limit right-aligned; the existing money column styling is reused. |
| D26-01 — missing expanded Overpayment capture | CLOSED. Three fresh-overpayment-detail PNGs now show the changed Original/Outstanding detail in desktop, device-light and phone. |

No remaining A4 finding in this slice. The plain Show estimate text is a truthful unallocated state; this review does not claim an interactive estimate implementation before WB-27. Paused rows are grey, explicitly not collected and untickable. The current outer reader does not supply a Due fact: Due uses the existing factual detail reader and its positive obligation value, not a fabricated signed deduction. A full loan balance is not displayed under Payment Advance.

## All 42 policy checklist results

PASS notes for inherited shared frame/list behaviour assess whether this narrow slice preserves it, not whether later card capabilities have already been built. N/A decisions follow the owner-defined scope. User-eyes rows are explicit scoped yes answers; they do not convert NOT_RUN journeys into application acceptance.

| Item | Result | Evidence / scope note |
|---|---|---|
| 1 | PASS | The authorised WB-26 slice exposes native Case ROW and expanded component choices, repayment/recovery defaults, remembered exclusions and standalone Manual Credit. Parent Case/Candidate aggregate controls remain WB-24; estimate remains WB-27. |
| 2 | PASS | The card-cited Case selection, factual repayment detail, independently ticked Manual Credit and paused no-collection display are retained. Broader legacy Case management/estimate flows remain their named later cards. |
| 3 | PASS | Payment Advance repayment, Overpayment recovery, Manual Credit, Due, This Draft, Weekly limit and Paused – not collected use the required business wording. |
| 4 | PASS | Ticks change local Case intent and repaint matching controls, without recalculating money. Unallocated deduction cells say Show estimate; paused rows remain in Waiting and cannot be included. |
| 5 | PASS | Existing Candidate Banking frame identifies Amelia Hughes · TMS 10421; Close stays in the header. Desktop and phone preserve that context. |
| 6 | PASS | One Ready / Waiting / Action required / Blocked tab set; no new second tab level. |
| 7 | PASS | Summary then tabs then native list/detail. Case detail now appends one native table directly; the redundant nested table-wrapper was removed. The existing inline detail stripe remains. |
| 8 | PASS | Existing universal table/sticky-header structure is reused. Original, Outstanding, Due, row amount and Weekly limit use right-aligned tabular money; final pixels confirm Weekly limit alignment. |
| 9 | PASS | No new badge or tone vocabulary. Existing tab states remain textual; paused is grey with a written reason rather than colour alone. |
| 10 | PASS | No WB-26 footer/action proliferation. Existing Include all eligible payments remains the scoped selection action; local tick/Expand controls sit at the affected row. Later estimate/Create actions are not fabricated. |
| 11 | PASS | No new design framework, tokens or !important. Existing universal/native controls and choice labels are reused; new paused opacity is scoped to #modal.ctms-modern-modal. |
| 12 | PASS | Existing bounded continuous list/detail reads and Loading more… remain. No page number, Previous/Next or Load more was added. |
| 13 | PASS | Existing sortable data headings and phone Sort by remain; Payment ▼ and Sorted by Payment, descending state the active/default sort. Case factual detail is an inline evidence table rather than a new independent list. |
| 14 | PASS | Native component tick is first and uses component identity/defaults/exclusions; whole-Work three-state behaviour is retained. Case/Candidate aggregate select-all and mixed state are WB-24, outside this owner-approved component slice; no loaded-page count is invented. |
| 15 | PASS | Displayed prepared facts and any counts use named reader fields. The choice renderer does not sum visible rows or invent a full selected total; no per-tick money recalculation. |
| 16 | PASS | Manually reviewed final visible copy and changed inline messages against policy.json neverShow, chatter, formats, vocabulary and rewrites, per owner direction. No wording tool was run; copy is plain, brief and uses UK formats. |
| 17 | PASS | Changed choice/detail failures stay inline: Could not change this choice; refresh and try again. / Could not load the deduction; try again. No raw exception or internal code is printed. |
| 18 | N/A | This slice adds reversible choice/read controls, with no confirmation or irreversible command. Confirmation design belongs to its owning later flow. |
| 19 | PASS | Existing loading/empty/retry states are defined in source; unavailable factual money uses —. New unallocated deduction rows no longer present £0.00 as an allocation. Failure states were source-reviewed, not executed. |
| 20 | PASS | Final pixels use Payment Advance, never Loan, and show no UUID, rule id or internal code. Payment Advance detail omits Original/full/Outstanding balance, advance date and advance count. |
| 21 | PASS | Visible money is £100.00 / £35.00 / £80.00; week ending is 04/10/2026; missing Client is —. Existing UK formatter remains; no new clock/date format. |
| 22 | PASS | Native labelled checkbox, visible focus, stable focus keys, disabled guards, Space row toggle and Enter/arrow navigation remain. Existing aria-sort/roles are retained. Keyboard reachability is source-reviewed, not an executed journey. |
| 23 | PASS | All eleven phone images fit the viewport as labelled cards, show Sort by, retain header Close, and keep the applicable footer action reachable. Expanded Case detail uses the same card layout; body continues vertically. |
| 24 | PASS | Every one of the eleven final desktop dark/device-light PNG pairs is byte-identical by SHA256; all images were opened for review. |
| 25 | N/A | Owner requires actual local product captures/fixed read responses and forbids generation/rendering/copying the supplied specs. The supplied pack remains read-only; no new spec/page pipeline is claimed. |
| 26 | PASS | Visible values map to existing HEADER / CANDIDATE_GROUPS / GROUP_LINES / CASE_COMPONENTS fields listed below. Show estimate is an unallocated state, not fabricated money; readerAdditions is N/A under the actual-product review scope. |
| 27 | PASS | No pay date appears; the visible Timesheet week ending is work context. |
| 28 | PASS | Yes, for this choice task: business labels distinguish credit, recovery, repayment, Due and Weekly limit; Show estimate prevents an unallocated zero being mistaken for a result. |
| 29 | PASS | Yes. First-column ticks and inline Expand/Collapse sit beside the payment, with detail controls in the same expanded payment. |
| 30 | PASS | Yes, for this component slice. Tick/untick is the immediate action; the existing include-all footer is explicit. A real estimate/Create step awaits its owning card. |
| 31 | PASS | Yes, within scope. Candidate identity, prepared Ready figure and separate payment addition are clear; the repayment/recovery shows a Due fact and explicitly defers the recoverable amount. |
| 32 | PASS | Yes, within scope. Waiting repayment says Paused – not collected and has a disabled tick, so it cannot be collected here. The later management/remedy action belongs to WB-38 rather than being forced into this slice. |
| 33 | PASS | Yes. Fresh repayment untick and remembered repayment re-tick update both outer and expanded controls; Manual Credit toggles independently. Captures show the changed checks without changed prepared figures. |
| 34 | PASS | Yes. Full Payment Advance balance/Original/Outstanding and internal Case/rule identifiers are removed from that detail; Overpayment retains the factual Original/Outstanding needed for recovery. |
| 35 | PASS | Yes. The same payment names, Paused – not collected and Show estimate appear in desktop/phone, outer rows and expanded detail. |
| 36 | PASS | Yes. Choice changes are reversible; paused is disabled. No payment, allocation, provider or other irreversible command is newly reachable on a single tick. |
| 37 | PASS | Yes. Native ticks reverse immediately; Collapse and header Close remain available. No new modal trap or destructive action. |
| 38 | PASS | Yes. A first-column tick chooses a payment; Expand exposes its factual detail without a separate facts window. |
| 39 | PASS | Yes, within the factual boundary. Ready £100.00 and Payment addition £35.00 remain separate reader facts, not a browser-computed £135.00 selection; Due/Weekly limit £80.00 are obligation/control facts, while unallocated deductions say Show estimate. |
| 40 | N/A | Full confidence before Create Draft requires WB-27's actual estimate and the later Draft flow. This review neither approves recoverability nor claims what the final Draft will pay. |
| 41 | PASS | Yes at source/layout level. Existing bounded retained pages, read/busy/actor/revision guards and disabled controls are reused; 33 captures cover desktop/phone and fresh/remembered contexts. Large, slow, failed-read and full keyboard runtime journeys remain NOT_RUN. |
| 42 | PASS | Yes. Candidate title/subtitle, active tab, payment label, remembered checked/unchecked state, paused reason and unallocated Show estimate retain location and outstanding work context. |

## Named data mapping

| Visible fact or control | Existing field / authority |
|---|---|
| Candidate subtitle | Candidate display name and TMS reference from the Candidate address/header context. |
| Ready to pay / Payment addition | HEADER `amount` and separate `ready_net_add_amount`; no visible-row selected-money sum. |
| Payment / Client / Pay channel / deductions / factual outer amount | CANDIDATE_GROUPS `payment_label`, `client_display_name`, `target_pay_channel`, `has_deduction`, `group_amount`. |
| Paused – not collected | CANDIDATE_GROUPS `case_state=PAUSED` and GROUP_LINES `reason_code=CASE_PAUSED`; Waiting/non-eligible authority is preserved. |
| Initial and remembered Case tick | CANDIDATE_GROUPS / GROUP_LINES `case_component_id`, `selection_eligible`, `selection_basis`, `default_selected` plus the existing choice store's `intent.cases` and Candidate exclusion precedence. |
| Overpayment Original / Outstanding | CASE_COMPONENTS `case_principal_approved` / `outstanding_source_ex_vat`. Hidden entirely for `case_kind=LOAN/ADVANCE` Payment Advance detail. |
| Due / Weekly limit | CASE_COMPONENTS `amount_due` / `weekly_due_source_ex_vat`; optional minimum earnings/take-home labels use the existing threshold fields. |
| This Draft / unallocated outer deduction | Show estimate until its owning estimate card supplies real recoverability; CASE_COMPONENTS `recoverable_this_run` is null in this evidence and is not turned into £0.00. |
| Timesheet context retained around native Case controls | Existing Timesheet `week_ending_date`, `timesheet_reference`, client and prepared amount; no pay date. |

`readerAdditions`: N/A. Existing projection adds only the authorised native `case_state` display seam; other factual values come from the current closed readers. No new balance or estimate reader is requested by this A4 review.

## Reviewed source and reference identity

FE root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-frontend-20261009`

BE root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009`

All hashes below are SHA256 of saved file bytes.

| File (FE unless marked BE) | SHA256 |
|---|---|
| js/bpay-next-workbench.js | `05d1b9a71a82cf520d2fb9d1241a9a68c6d9c757defb7f330d8b1a27c78c3f1c` |
| js/bpay-next-office.js | `e372dbd1a9f1b091067f800a1e83d652d81b06313044f490449e63510173fc72` |
| js/bpay-next-ui.js | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| css/bpay-next-office.css | `0d89b064ca0d664ea7b18cbf2c67e0a882b3e3576007c4757de246b1628c689b` |
| index.html | `d224ff012269665ffb9dad00f90af4536d93f87c168ebcc93d54cb3d20375349` |
| docs/bpay-next/design/WB-26-build-scope.md | `11accc68c71daef79a9350aab24285f52a7bbab602a917c014fdaacee5ef7a48` |

The final row is the BE scope file. Policy/reference root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\cloudtms-backend\codex_outputs\claude-stage2-continuation-20260925\SCREEN_DESIGN_POLICY_20261010`.

| Read-only supplied file | SHA256 |
|---|---|
| SCREEN_DESIGN_POLICY.md | `8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5` |
| tools/policy.json | `45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d` |
| SCREEN_INVENTORY.md | `3b9b035de99847483c0111fd689dd2fc6d9f9589758f02744ec7b028c6727b2c` |
| DESIGN_INDEX.md | `cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe` |
| specs/T02_candidate_window.json | `7f6f14de9d0808a53e4065b9b807dc5e26a0aa4da796e27ba68d7ba5758302e7` |

## Actual capture identity

Files live beside this review. All 33 PNGs were inspected; `fixed-read-responses.json` was read for field/scenario identity only. It contains local fixed responses, not proof that a database or hosted application returned those values.

| File | Bytes | SHA256 |
|---|---:|---|
| fixed-read-responses.json | 39264 | `3cedc569822ba6c1dfff3e20a370d46629e3bd78400e4028f5b1431ef987e3c3` |
| fresh-credit-choice-dark.png | 292442 | `20c71ac6c48b8247a6b4cba3680851affec510605f62dfe045b4a95a8ee3e7e3` |
| fresh-credit-choice-device-light.png | 292442 | `20c71ac6c48b8247a6b4cba3680851affec510605f62dfe045b4a95a8ee3e7e3` |
| fresh-credit-choice-phone-dark.png | 91707 | `3e4993b151e86764e1c0a3119838e7abce029f2a3b453c3693cfbbbb0b5e332d` |
| fresh-overpayment-detail-dark.png | 273852 | `fbc554831b46cde764928a505970459682fc7a7dcb4c3683e915d90f303509c4` |
| fresh-overpayment-detail-device-light.png | 273852 | `fbc554831b46cde764928a505970459682fc7a7dcb4c3683e915d90f303509c4` |
| fresh-overpayment-detail-phone-dark.png | 95854 | `083b40cb6170260353e18fa8761281b90b7d1d1345d13e550449d03b3746b6e0` |
| fresh-paused-dark.png | 328381 | `1f8cd475eff75863ef0a8e5728e6fb407e1780ec40b0f92b442f32925b494ac4` |
| fresh-paused-device-light.png | 328381 | `1f8cd475eff75863ef0a8e5728e6fb407e1780ec40b0f92b442f32925b494ac4` |
| fresh-paused-phone-dark.png | 91544 | `4ca7ecbbe92b82b6c8bc22538cacb40dc48c717ca31ea79979a6f6f3a99f02c4` |
| fresh-ready-dark.png | 324648 | `3df466198210b3b53c4cfd133e17795eb3a0ca7e7155ee428b249e342ed028d0` |
| fresh-ready-device-light.png | 324648 | `3df466198210b3b53c4cfd133e17795eb3a0ca7e7155ee428b249e342ed028d0` |
| fresh-ready-phone-dark.png | 92947 | `bc6825b7568c12673e7e3b492952459347512794ca4a0e877bb7b7d68b923b0e` |
| fresh-repayment-choice-dark.png | 294574 | `0f17b96c088ffd3862ed7fae1b5a72c7a552af304034125f060fd3f80cfaf41c` |
| fresh-repayment-choice-device-light.png | 294574 | `0f17b96c088ffd3862ed7fae1b5a72c7a552af304034125f060fd3f80cfaf41c` |
| fresh-repayment-choice-phone-dark.png | 94084 | `f5a81504da7f743649c51b1ee133dcb8b3572d1e66d518daa1078336e55d8436` |
| fresh-repayment-detail-dark.png | 296970 | `e1200465febb40cc6f766aa46289b272d2094dceae730d5846cfbaee3a024fcd` |
| fresh-repayment-detail-device-light.png | 296970 | `e1200465febb40cc6f766aa46289b272d2094dceae730d5846cfbaee3a024fcd` |
| fresh-repayment-detail-phone-dark.png | 95290 | `a891dce0423bdd306a3edcc5efc925075a490e3737f0c5fed37ba0e5be2f79d2` |
| remembered-excluded-credit-choice-dark.png | 294956 | `5bfc2802e73e77d05b3885ab88eff4bb1bcf201f22036ec9174698546ec3a3ab` |
| remembered-excluded-credit-choice-device-light.png | 294956 | `5bfc2802e73e77d05b3885ab88eff4bb1bcf201f22036ec9174698546ec3a3ab` |
| remembered-excluded-credit-choice-phone-dark.png | 91898 | `94b6f26a9124b36a9c2ea028f1a4b6fcddf9e7432a60dbb9b1f9c0ac04403169` |
| remembered-excluded-paused-dark.png | 337529 | `34fd93446d9914ad17f855fd89d12e620b97205ac15b5ede0f67f5b2465c6660` |
| remembered-excluded-paused-device-light.png | 337529 | `34fd93446d9914ad17f855fd89d12e620b97205ac15b5ede0f67f5b2465c6660` |
| remembered-excluded-paused-phone-dark.png | 91544 | `4ca7ecbbe92b82b6c8bc22538cacb40dc48c717ca31ea79979a6f6f3a99f02c4` |
| remembered-excluded-ready-dark.png | 322512 | `714b05fc4ba30f6dc51b6fc00326413bc911870b3b98a1fef7bb54c0bada8a0a` |
| remembered-excluded-ready-device-light.png | 322512 | `714b05fc4ba30f6dc51b6fc00326413bc911870b3b98a1fef7bb54c0bada8a0a` |
| remembered-excluded-ready-phone-dark.png | 91622 | `a92b87704bc49e4d54257712afbcb05d0baef7224e575d99b1f02519a414afcf` |
| remembered-excluded-repayment-choice-dark.png | 294836 | `5eb803d3007eeb1f78734638b0329839db46dd3daa94e5aad92f5d03c622b563` |
| remembered-excluded-repayment-choice-device-light.png | 294836 | `5eb803d3007eeb1f78734638b0329839db46dd3daa94e5aad92f5d03c622b563` |
| remembered-excluded-repayment-choice-phone-dark.png | 94290 | `d7f976f24b316531afd87c81e5122a615f36a24f2091fcc8d76740e69e1a6dd0` |
| remembered-excluded-repayment-detail-dark.png | 293549 | `7bee955b6476614ea866dfab58d2b20a8b8c63da0c1986c60789347f6b6e69eb` |
| remembered-excluded-repayment-detail-device-light.png | 293549 | `7bee955b6476614ea866dfab58d2b20a8b8c63da0c1986c60789347f6b6e69eb` |
| remembered-excluded-repayment-detail-phone-dark.png | 94016 | `3aaff268d0e66527657b2d4d8e66e38ea07aadfd0917d480717271cccc471f36` |

## Acceptance boundary

The changed visual/source slice passes A4 with no unresolved policy failure. Prepared money is not recomputed on ticks, unallocated recovery is not presented as zero or collected, remembered choices repaint in place, and paused remains in Waiting without selection. This receipt authorises only the local screen review outcome. Full first-use acceptance, allocation correctness/capacity examples, application journeys, installed runtime and App tested are NOT_RUN and require their own authorised stage/evidence.
