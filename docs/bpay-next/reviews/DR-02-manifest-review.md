# DR-02 manifest/backend source review — 11 October 2026

**Verdict: source PASS for the current backend successor and Worker amendments. No essential source defect found.** This is an independent saved-source review, not a complete DR-02 delivery or runtime acceptance. Root alone edits product/checklist files. Reviewer executed no tests, SQL/database calls, fixtures, hosted operations or browser journeys.

Authority read: complete [DR-02 card](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:68>), all [DR-02 checklist rows and shared-owner evidence](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2325>), and [current investigation](../design/DR-02-current-investigation.md).

## Retention and exact scope

Static function-body comparison of [0454 successor](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/supabase/repeatable/11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:6>) against each latest physical predecessor confirms exactly nine definitions:

- Six1832 append/component/Case owners retain their complete bodies, each adding only an explicit `selection_state='SEALED'` refusal.
- The2232 EXCLUSION owner retains its complete body with the same explicit refusal inserted before its existing exclusions-closed branch.
- The2232 public intake adds a local `v_seal` result, retains the existing native seal return, and exposes its `selection_count` in SEAL_ENQUEUE.
- The0312 public Workbench reader retains its complete body, adding only `can_create_draft:true` to the existing HEADER enrichment.

No Create body, final seal owner, accepted Work acknowledgement, MATCHING owner, enqueue/enroller, claim/PIN, money kernel or prepare owner is replaced. In particular WB-40 Create0426 and WB-38 enroller0316 stay current. Original accepted descriptor/member/revision/query capture remains authoritative; the successor adds no live financial/settings/Case requalification or membership reconstruction. Original bounded page sizes, row locks, input validation, body conflict codes, receipt shapes apart from the one required count, ownership and ACLs remain intact.

A parsed metadata comparison with HEAD shows semantic registry changes only for these nine signatures. Each finalRoot/finalPhysical points to0454 and retains its former owner in allowedPhysical/predecessors. JSON serialization changes elsewhere do not change those entries' values. There is no parallel ownership registry or new command/table/helper.

## Replay and genuinely new late membership

Each append still locks the run and compares the retained page before the new late-seal check. Existing identical pages replay; conflicting content retains its existing finite conflict code. The new `BPAY_NEXT_SELECTION_ALREADY_SEALED` applies only after no stored page was found. The old ordering, component-choice/header-open, exclusions-closed and accepted-membership checks continue afterward.

| Current0454 owner | Exact new guard line | Retained behavior |
| --- | ---: | --- |
| append_selection_page_v1 |54 | Accepted descriptor dispatch still uses existing acknowledgement; legacy page replay/conflict precedes the guard. |
| append_work_choices_v1 |178 | Closed tuples and retained request/revision/mode replay comparison remain; no new Work freshness check. |
| append_component_selection_page_v1 |259 | Expected revision/mode and retained ordered component replay remain; a genuine new page cannot cross final seal. |
| seal_component_selection_v1 |323 | Expected page/count/revision conflict and genuine sealed-choice replay precede run closure. |
| append_case_selection_page_v1 |383 | Request ID, item count and ordered component/boolean replay remain before run closure. |
| seal_case_selection_v1 |466 | Expected count conflict and genuine sealed-header replay precede run closure; first-seal counter remains once-only. |
| exclusion_page_v1 |516 | Retained request/result replay precedes run closure; existing matching/exclusions completion remains distinct. |

Accepted Work v2 and MATCHING already own the required late code and are reused unchanged. Final seal2306 still serializes on the same run, verifies expected pages/count and complete retained Work/component/exclusion/Case upload before sealing, and returns its immutable native final count. Enqueue still binds the original command and refuses unfinished membership; earlier-command and enrolled-membership barriers remain unchanged.

## Receipt, permission and Worker

[SEAL_ENQUEUE](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/supabase/repeatable/11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:655>) captures the original seal result, then enqueues, preserving original run/command/agency-sequence/replay fields and adding `v_seal->>'selection_count'`. It does not recalculate from a browser list or later database membership. Both first seal and retained seal replay supply the native count.

[HEADER permission](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/supabase/repeatable/11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql:1458>) is returned only on successful HEADER after existing service-role710, active Office-admin736 and NEXT-owner738 gates. Failed or unavailable reads return the original error response instead. Mutations retain their own authority checks; the flag is current server permission, not authority inferred from money/count or a legacy capability. The existing SNOOZE_CHECK early branch returns its own kind and cannot create a HEADER success bypass.

[Worker preparation schema](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/broker/src/bpay-next-preparation.js:61>) requires the new count as a canonical bounded integer string. Line301 compares it exactly with the attempted `expected_count`; missing/extra/wrong values fail the closed response and retain unknown-mutation handling. Existing request/response/deadline limits, action list, exact run/command identity checks and accepted ENROLL wake are unchanged. [Worker HEADER schema](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/broker/src/bpay-next-workbench.js:15>) adds only the required boolean.

[safeError](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/broker/src/bpay-next-preparation.js:465>) exposes only four exact23514 manifest replay/conflict messages and exact55000 `BPAY_NEXT_SELECTION_ALREADY_SEALED`, as known409 refusals with false retry/unknown flags. Arbitrary database strings retain existing safe normalization. Existing before-acceptance/accepted-identity refusal handling, authentication and timeout/unknown semantics are preserved.

## Evidence boundary

Root reports336/883 `db:check` PASS; local PG17 load was pending at this review checkpoint. This reviewer confirms source retention and closed contracts only. DR-02 UI/journal/progress integration, historical journal restoration, normal Worker/queue path, page3 lost-reply/conflict, late EXCLUSION/CASE, exact40-Work/12-Candidate readback and close-while-preparing application checks remain separate pending acceptance. DR-10 Guard and DR-06 full progress retain their own later scope. No whole feature installed/app-tested box is justified by this review alone.

## Final backend amendment and source checkpoint

Fresh read confirms [typed constructor entry](<C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009/scripts/bpay-next-source-gates.mjs:565>) now names the exact current SEAL_ENQUEUE object with native `v_seal->>'selection_count'`. Only this exact constructor changes; no generic field/operation exemption or gate suppression is added. The0454 tail emits `notify pgrst, 'reload schema'` immediately before the existing commit, so schema reload remains transactional. Function bodies, permission gates, original accepted money/manifest owners and Worker validators reviewed above remain unchanged. **Final backend source PASS.**

| Final saved source | SHA-256 |
| --- | --- |
|11102026_0454_banking_pay_next_selection_manifest_guards_v1.sql | `d40cc95cb6675f09b1fe06a87801e42a6a28ca1f02a563534ae5e1a65263b5cb` |
|scripts/bpay-next-source-gates.mjs | `b28b0dd82d47d5c6922fae179d21aa22db2879468daf545347da342b03fbb4ec` |
|broker/src/bpay-next-preparation.js | `594e62ce0cc860853e8a9462a7d901afc456c9c7ebba732a40d580e00fb65760` |
|broker/src/bpay-next-workbench.js | `af15952439c88ff1f6f3e0df1bd5d4083da83dffa85c459c378873a96e586447` |

Root subsequently reports336/883 `db:check`, sole localPG17 four-closure schema load, contract seal/coupling, complete22082026_1302 catalogue DO and11/11 source gates PASS. Those are builder-reported checks, not executions performed by this reviewer. Screen review and joined runtime/hosted acceptance remain pending; this amendment grants no installed/app-tested claim.
