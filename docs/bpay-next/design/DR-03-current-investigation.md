# DR-03 current investigation — sealed Case choices before allocation

Read-only investigation, 11 October 2026. No product, SQL, checklist or schema changes; no database execution, tests, fixtures, screenshots, hosted calls or commits. Runtime/Office acceptance is **NOT_RUN**. Policy X: no drift.

`BE` and `FE` mean the current backend/frontend bpay-next-build worktrees. `PLAN` is the ordinary backend `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3`; short SQL names below are under BE/supabase/repeatable. Current saved source and final-definer registry govern, not a historical installed-status label.

## Binding card and row scope

The complete card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:98–119` requires excluded repayments/deductions/credits to be removed before allocation, Case choices sealed by the existing WB-26 CASE_PAGE/CASE_SEAL path, re-inclusion to preserve history/frozen instructions, and removal of the three separate Case-selection controls from the Sheet. **Line 112 explicitly says DR-03 writes no SQL successor.** If exclusion occurred after allocation, record that as the first DR-11 gap; do not fix it out of chain here. Line 113 requires only verification that current Case actions/readers stay allow-listed. Design-first is NO; dependencies are DR-02, WB-26 and F-16; sequence80 is `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:164,465`.

All current checklist rows read: BE `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2365–2384`: D01 prepare owner, D02 append Case page, D03 seal Case selection, D05 current Worker maps, D06 retire Sheet controls, D08 native case-instruction schema; D04/D07 are shared-owner evidence. Their generic remaining-change wording does not authorize a second seal, a new amendment/producer framework or promotion of raw0549 under a card that explicitly writes no successor. Required completion notes must cite the current functions/lines; app acceptance remains separate.

The master chain is F-25 → CM-02a → WB-27 → DR-04 → DR-11 → DR-12 → FC-10 → WB-41 → CM-13a → CM-02 → CM-10 → CM-12 → CM-13. WB-27 places derivation/allocation in the single STABLE plan; later edits merge only their named branch and preserve current locks/writes. DR-03 is verification, not an allocation link. (`03:17–19`; current `BE/docs/bpay-next/design/case-allocation-chain.md:5` and “WB-27 plan boundary” section; controlling master F-04 `01_BUILDER_RULES_AND_FOUNDATION.md:452,488–490`.)

Owner OR-44/Rule B freezes original selected Work/revision, Case nonbalance/rule/control facts, Candidate channel and floor at accepted Create; later changes belong to the next Draft. Preserve original accepted command order and PIN; do not install parked2153 accepted_input/effective-cut objects from historical R2 prose. Current native balances remain live under original money locks. The historical R2 connection discussion is read as provenance, with the current owner Rule B supplement controlling implementation. (PLAN `09_OWNER_ANSWERS_BINDING.md:11–19`, `review/R2.md:90,226`, `addendum/R2_EXACT_CONNECTIONS.md:56–64`; current `BE/docs/bpay-next/design/WB-21-current-scope-investigation.md:7`.)

## Current source proves exclusion before headroom

Current final signatures/physical owners from `BE/docs/bpay-next/final-definer-closures.json`:

| Signature | Final physical owner |
| --- | --- |
| private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) | 11102026_0205_banking_pay_next_prepare_shared_case_plan_v1.sql |
| private.bpay_next_case_allocation_plan_v1(jsonb) | 11102026_0204_banking_pay_next_case_allocation_plan_v1.sql |
| private.bpay_next_append_case_selection_page_v1(uuid,uuid,bigint,bigint,uuid,uuid[],boolean[]) | 11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql |
| private.bpay_next_seal_case_selection_v1(uuid,uuid,bigint,bigint,bigint) | same0454 |
| private.bpay_next_seal_selection_v1(uuid,bigint,bigint) | 10102026_2306_banking_pay_next_actor_selection_memory_v1.sql |
| private.bpay_next_enqueue_preparation_v1(uuid,uuid) | 10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql |

In prepare0205, a Case header must already be SEALED (`125–128`). INSTRUCTION_CAPTURE selects only the exact run/Candidate/selection revision and **i.is_selected** before any Case/component/period locks, DERIVE call or instruction insertion (`251–269,311–320`). Capture continuation applies the same predicate and verifies selected instruction count before advancing to ALLOCATE (`328–336`). Only those frozen native instructions enter the ordered allocation page (`370–379`); headroom/worker take-home then advances from the plan's allocation result (`383–385`). An excluded component creates no instruction and cannot consume headroom. No DR-11 exclusion gap was found.

The shared plan ALLOCATE sorts the same immutable instruction tuple and uses the existing row kernel rather than a second formula (`0204:358–395`, original helper call384–388). Prepare orders `age_key,case_id,component_ordinal,case_component_id` (`0205:374–379`). No kind tier or new money estimate is required by DR-03.

The Candidate floor is the retained selection_candidate.min_take_home_wtd (`0205:146–150`). Prepare preserves Case → component → period lock order before the shared DERIVE call (`257–269`). Plan DERIVE consumes the accepted original Case LEAF status/due, rule ID and rule revision with exact native identity checks (`0204:46–84`); its legacy non-memory branch remains separately at86–87. Outstanding balances/capacity still come from current native Case/component/period facts under those original locks (`0204:175–220`). Current rule changes must not replace the accepted captured rule. The ordinary funding-only period qualification remains the existing plan branch, not a new DR-03 amendment mechanism.

## Current seal and history proof

CASE_SEAL serializes on run/header locks, checks expected page/item counts, returns genuine stored seal replay, checks the complete accepted Case member count and seals the original header once (`0454:456–487`). The run seal requires every accepted CASE_COMPONENT to have an actual item with the same selected flag in a SEALED Case header; otherwise it raises **BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE** (`2306:662–680`). This existing code satisfies the card's “CASE_SELECTION_UNSEALED or existing code” clause. SEAL_ENQUEUE calls that run seal before enqueue in the same transaction (`0454:655–659`). Preserve the DR-02 late-page/refusal and final-count successor.

Case page/items and frozen instructions/results are immutable under the existing U/D guards (`04102026_0310_banking_pay_next_case_guards_v1.sql:283–294`). The native Case event history separately has a BEFORE UPDATE OR DELETE trigger calling the immutable-effect guard (`03102026_1342_banking_pay_next_guards_v1.sql:188–199,259–262`). Current Workbench reticking changes local Case choice intent; it does not delete/modify case_event or an already captured instruction. Actual finance-resolution apply/clear owners remain WB-29/WB-30 scope, as card108 states; DR-03 does not claim those later surfaces implemented.

## Smallest complete remaining product work

**Remove only the retired Sheet Case selection surface.** Current `FE/js/bpay-next-office.js:1105–1113` still contains the legacy `!workbenchShell` branch with “Open Case components”, “Add selected / excluded Case page” and “Seal Case selection”, plus its manual Case checkbox/page controls. The normal unsealed Workbench path already returns before this branch, but leaving these separate Draft controls is not the explicit card114 removal. Remove that rendered manual Case-selection branch; preserve frozen readers, controller/journal compatibility and Candidate Workbench selection. Do not move it into another Office route.

The current Candidate Workbench has native component ticks and bounded Case-group intent handling, with PAUSED controls disabled (`FE/js/bpay-next-workbench.js:1414–1428,1496–1498`). The existing DR-02 sequencer uploads exactly the accepted CASE_COMPONENT members and seals each Candidate through its retained journal; keep it intact. DR-16 owns the later sealed-choice review.

**No Worker edit is needed from the observed current source.** CASE_PAGE/CASE_SEAL are already exact closed inputs/receipts, and CURRENT_CASES/CASE_COMPONENTS remain allow-listed (`BE/broker/src/bpay-next-preparation.js:21–22,43–45,59–60`). Keep them for WB-26/native read compatibility; removing Sheet controls does not remove server capabilities. No new action, reader, queue job, public trigger/table or schema successor is required.

Completion should record these source anchors and the narrow UI removal. Future acceptance remains the card's GBP100 pay/two GBP80 repayments example, first unticked before allocation, history retained on retick, and absence of the three Sheet controls. No financial fixture, runtime assertion or app pass is supplied by this investigation.

