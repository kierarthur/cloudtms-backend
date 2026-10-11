# DR-03 independent Case-choice source review

**SOURCE PASS — no essential defect found in the reviewed batch.** Read-only review, 11 October 2026. The product change is exactly nine removed physical lines in FE `js/bpay-next-office.js`: the obsolete `!workbenchShell` manual Case-selection branch formerly at1105–1113. No replacement controls, SQL successor or Worker change are required by this card. No tests, database/business-data operations, fixtures, hosted calls, screenshots or commits were performed by this reviewer. App acceptance and the full Worker/queue path are **NOT_RUN**.

`BE`/`FE` denote the current bpay-next-build backend/frontend worktrees; `PLAN` denotes the ordinary backend `codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3`. SQL filenames below are under BE `supabase/repeatable` unless identified as migration.

## Scope and actual edit

Read the complete owning card `PLAN/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:98–119`, all D01/D02/D03/D05/D06/D08 rows in current BE `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2370–2375`, the saved `design/DR-03-current-investigation.md`, and the current final-definer registry. Card112 explicitly assigns **no SQL successor** to DR-03; card113 retains existing Case commands/readers; card114 removes the separate Sheet controls. Generic checklist successor wording does not expand that boundary. Later actual resolution/clear and sealed-choice review remain WB-29/WB-30 and DR-16 respectively (card108,114).

The saved diff is `0 additions / 9 deletions` in the single Office file. It removes “Current Case facts”, “Open Case components”, manual component checkboxes/page controls, “Add selected / excluded Case page” and “Seal Case selection”. None of those four labels remains in the current Office source. The surrounding current Work facts and preparation review flow remain intact (`FE/js/bpay-next-office.js:1098–1108`). Retained controller/read contracts are preserved; no second ticking or seal route was introduced.

## Original Candidate choices and durable upload

Candidate Workbench keeps native component selection, bounded complete group selection, and disabled PAUSED controls (`FE/js/bpay-next-workbench.js:1414–1428,1496–1498`). Group selection validates exact context/native membership totals before one local intent change. Reticking changes selection intent and does not amend native Case history or frozen instructions.

The unchanged original submission sequencer scans only accepted native CASE_COMPONENT membership through existing WORK_CHOICES, uploads the stored selected flags through CASE_PAGE, seals each complete Candidate using retained server counters, and advances its cursor only after known upload/seal (`FE/js/bpay-next-office.js:691–715`). Reader validation closes the exact header/member/cursor schema and selected/excluded count relation (`430–444`). Unknown original request bytes remain journalled and retried unchanged (`538–550,583–618`). The DR-10 conditional Guard insertion remains an explicitly later seam (`717–719`), with no early guard work in DR-03.

## Exclusion precedes capture and allocation

Current final prepare owner is `11102026_0205_banking_pay_next_prepare_shared_case_plan_v1.sql`. It requires SEALED Case selection (`125–128`) and filters **i.is_selected** before Case/component/period locks, DERIVE or instruction insertion (`251–269,311–320`). The same predicate and captured-instruction count check govern completion (`328–336`). ALLOCATE receives only those frozen instructions, ordered by the original age/native identity tuple (`370–379`); headroom advances from their plan results (`383–385`). An unticked component therefore creates no instruction and consumes no headroom. No later-exclusion DR-11 gap was found.

`11102026_0204_banking_pay_next_case_allocation_plan_v1.sql:46–84` consumes accepted original Case status/due, native rule ID/revision and control basis. Prepare uses the retained Candidate floor (`0205:146–150`) and preserves Case → component → period locks (`257–262`). Native outstanding balances remain current under those original locks (`0204:179–182`); the legacy non-memory branch is retained (`86–87`). ALLOCATE invokes the existing row kernel rather than a second formula (`358–395`). This preserves Rule B and the current master chain; no parked0549/2153 objects or producer work is introduced.

## Seal, native identity and history

Current CASE_SEAL owner `11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:456–487` serializes on run/header, validates expected counters and complete accepted membership, returns genuine stored seal replay, and seals the original header once. Run seal in `10102026_2306_banking_pay_next_actor_selection_memory_v1.sql:662–680` requires every accepted Case component to match its selected flag in a SEALED native header; incomplete upload raises **BPAY_NEXT_CHOICE_UPLOAD_INCOMPLETE**. `0454:655–659` seals before enqueue. Existing refusal satisfies card112; a new error or seal is unnecessary.

Native migration `04102026_0300_banking_pay_next_case_instruction_core.sql:179–208` retains bounded page counts, scoped Case/component foreign keys, unique component/ordinal identity, stored selected flags and the selected-only page index. `04102026_0310_banking_pay_next_case_guards_v1.sql:283–294` retains immutable selection pages/items, rules, instructions/results. `03102026_1342_banking_pay_next_guards_v1.sql:188–199,259–262` preserves append-only native case_event history. No amendment, lifecycle or capacity owner is rewritten.

The unchanged Worker closed input/receipt maps still allow CASE_PAGE/CASE_SEAL and CURRENT_CASES/CASE_COMPONENTS (`BE/broker/src/bpay-next-preparation.js:21–22,43–45,59–60`). No Worker gap was found.

## Reviewed saved bytes and limits

- FE `js/bpay-next-office.js` SHA256: `AF49A6DF915410FB6C11DDC5C389D3F27F64FF018EEE6CE61D05B1115F513E51`.
- FE `js/bpay-next-workbench.js` SHA256: `F01B5540B281E9EE30B45098811F6EC7C8067DDAD93F9247FAA4BEA760990E3E`.

This is a source verdict only. Root's normal compile/source checks and separate minimal-response screen capture are not executed or asserted by this review. The card's GBP100/two GBP80 repayments, exclusion-before-allocation, history-preserving retick and native end-to-end journeys remain **NOT_RUN** until separately observed.
