# DR-09 SQL/migration independent review

11 October 2026. Read-only source review of the uninstalled0814 working draft against `design/DR-09-build-scope.md`, full approved DR-09 card/checklist and current0559/0708/0746 parents. Only this review note is written. No SQL execution, database requests, tests, fixtures, browser/hosted actions, product edits, commits or checklist changes. This is an early findings record, not item/source PASS.

## Essential findings

1. **P1 — public intake source is not a complete SQL definition.** In `supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql:2085`, the new expected-revision regular expression ends without its `$`/closing quote or validation branch. The old body begins immediately on2086 and reaches its first `$function$;` at2189. The remainder of the new validation appears outside that definition at2190–2194, then duplicates the original body through another delimiter. This would prevent installation. Smallest correction: assemble one complete current0708 public intake definition, keeping the exact closed discard validation and one original body; remove the detached duplicated tail. Reported to root before load. Re-read current bytes after correction.

Root already identified and is correcting the separate early header cancelled-count timing assertion. The current read at1367 still compares run.cancelled_candidate_count to root.original_removed_count+root.removed_candidate_count during CANCELLING, before finish-member records that new root removed count. It must only assert that equality at a proved terminal transition; this is not a new independent finding.

## Scope already checked

The migration creates only the approved private root request and Candidate binding. The new lane retains economic/execution revisions and active preparation identity, uses original native Candidate/member order, and keeps due-expiry admission/type/deadline separate. Same-command acceptance replay precedes late run eligibility. Genuine sealed empty scope returns EMPTY without a command; accepted positive OPEN/RECEIVED original metadata has its separate retirement branch. Original WORK mutation is reused through the typed scope; Case arithmetic and native unpaid-week arithmetic remain in their existing owners with explicit discard binding branches. Full original Work/selected Case projection and the corrected checkpoint-only pending-ORIGIN clearing pattern are retained.

Further focused review continues on private guards, final counter/replay/lock ordering and completed packaging/ACL. No runtime or installation claim is made.

## Fresh reread after root reassembly

The public intake packaging finding is **closed in fresh saved source**: one full definition now contains the complete canonical integer regex, bigint ceiling, optional reason validation and original body. The detached duplicated tail is absent. The known header counter timing assertion is also moved behind request terminal status, so interim CANCELLING counter updates are no longer rejected for being ahead of finish-member's root count.

2. **P2 — a genuine empty OPEN Create is not a no-op.** Acceptance's EMPTY branch currently checks selected_candidate_count=0 **and** selection_state='SEALED' (`0814:84–88`). Current descriptor Create `11102026_0005_banking_pay_next_component_choice_validation_v1.sql:449–465` leaves an empty Create OPEN, records selected_candidate_count=0, and inserts original PREPARE/run_command only when native v_count>0. Discard currently creates an ordered command/request and changes this empty run to CANCELLED. That contradicts the card's empty-scope no-op. Smallest correction: return EMPTY for zero native selected Candidate count regardless of selection state, or use the exact no-original-command zero proof. Positive accepted OPEN Create already has selected_candidate_count>0 and the original PREPARE command, so its necessary metadata retirement stays reachable. Reported before load; awaiting fresh correction.

Current assembled source also includes private ACL revocations, service-role-only existing public grants and both new table guard triggers. No new public function membership is required. The terminal member guard checks no remaining ACTIVE Work/Case holds, and BLOCKED is limited to its original NEW-stage protected worker identity. Work/Case money arithmetic remains unchanged; new scope/collection/week admissions are exact typed branches. Original-member projection, <=100/<=50 bounds, retained pending ORIGIN checkpoint clearing and original revision0 Case selection handling follow current0746. Confirm now independently refuses every unconfirmed run with a discard request, retaining confirmed replay and the original real deadline checks.

The general enroller now uses NOWAIT on its original ENROLLING run SHARE, preserving the previously plain RECEIVED metadata read. The dedicated discard enroller's run SHARE was still plain at the last read; root is finishing that finite lock qualification. No additional money/retry-identity framework is needed.

3. **P1 — supported positive OPEN discard cannot be followed through PROGRESS.** Current descriptor Create0005:449–465 records a positive selected Candidate count while selection_state remains OPEN. Acceptance and enrollment explicitly support cancelling that accepted original PREPARE before upload/seal; discard never changes selection_state. The new private PROGRESS predicate (`0814:2797–2805` in the24-definition assembled read) admits discard OPEN only when CANCELLED and expected_candidate_count=0. Consequently, a supported positive OPEN run gets BPAY_NEXT_PROGRESS_NOT_ACCEPTED throughout CANCELLING and after complete discard. Smallest correction: admit this exact captured OPEN discard through its own native request/header/revision/count/command proof in progress and terminal states, keeping ordinary and expiry predicates intact. No new reader or money work. Reported before load; awaiting reread.

## Current 27-definition SQL and Worker reread

Fresh saved-source hashes: migration `e5005f5ceac57ac6d23f4a568c6a2c8979773f2c4a65612c963cde4605c63c00`; repeatable `69d37a9a548aca0a5f375d630a50fbe5ca1096d4e8f64b32e8562b29896497d0`. Findings 1–3 above are now **closed**. Empty native scope returns EMPTY for both OPEN and SEALED without creating a command (`0814:84–87`). The positive OPEN discard PROGRESS branch proves its original captured selection/page/Candidate counts, exact review revision increment, own command/current module, and current or terminal disposition (`0814:2802–2815`). Stored discard counters supply pending/preparing, removed/already removed and blocked without recalculating money (`0814:2839–2846`). Terminal PARTIAL is REVIEW with honest blocked count and no false all-returned claim.

The fresh final expiry admission rejects a different discard binding after its own original replay and before late admission. The due reader takes the original bounded native <=100 slice before filtering discard roots (`0814:3273–3286`), and advances its visited cursor across skipped rows. Existing dispatch stores that visited cursor, retains the fixed sweep boundary, and returns only the last eligible returned-item cursor (`0814:3440–3466`). This keeps PARTIAL roots out of expiry without an unbounded anti-join scan or a new expiry protocol. The Worker direct DUE validator now admits a strictly advancing visited tuple >= its last returned item, including an empty filtered page, while keeping a paired null cursor (`broker/src/bpay-next-preparation-expiry-rpc.js:171–183`). The runtime dispatch validator retains the last-returned-item contract.

Worker source adds only the exact PREPARATION_DISCARD lane to existing kind/page/claim maps. Its closed step receipt accepts RELEASE, DISCARDED or BLOCKED, limits native release rows to the requested page, rejects release money for missing worker or BLOCKED, and rejects replay release counts. A separate retained DISCARDED PREPARE schema preserves the ordinary PREPARE schema and requires done/replay/zero visited rows plus the original paired cursor and worker/issue identity. Queue delivery remains the original two-field native ID/kind wake, fresh CLAIM then one STEP, exact lease ownership, and awaited continuation before ACK. Unknown outcomes retain native lease-expiry recovery. Discard request operands and optional bounded reason are closed, canonical bigint revision is retained as a string, command/run receipt identity is compared with the request, and native terminal counters conserve original scope. No browser money, new producer, secret, route, or calculation owner is introduced.

One announced finite-lock correction remains pending in this exact hash: dedicated `bpay_next_enroll_preparation_discard_one_v1` still uses a plain run FOR SHARE (`0814:166`), while it is called with the original global enrollment clock/command locks held. The original ENROLLING run lock has already been qualified NOWAIT (`0814:1724`). Qualify the dedicated run point lock the same way so a discard page holding Run UPDATE cannot hold the singleton enroller through a long page/ORIGIN refresh wait. The existing 55P03 retry behavior can then defer this one member safely. This is a narrow metadata-lock correction; no business owner changes are needed.

No additional essential defect was identified in the reviewed whole-run release/collection/week/header guards, immutable economic/execution identity, bounded original membership projection, checkpoint-only pending ORIGIN clearing, terminal no-ACTIVE-hold proof, replay ordering or ACL packaging. **Whole-run source closure is conditional on that final finite-lock correction.** Per-Candidate Remove while PREPARING is an outstanding separately owned card requirement under investigation and is not approved by this whole-run review. UI and runtime/A4 remain unreviewed or NOT_RUN here; no item completion is asserted.

Reviewed Worker SHA-256:

- `bpay-next-financial-queue.js`: `dcceeab15dcaa39c513a87b438d240f27fb23d31a0a62fd55d2dc18dce0cf0d7`
- `bpay-next-job-rpc.js`: `28a8ef749a277a7fbcd10a6e5a9caabd50a6b94ab795df411c7bfdf36fda676f`
- `bpay-next-job-step.js`: `0ef630c4c9afc5b5be0cdc06ed2b1974280beacdd27ac944d168c591cf5979ed`
- `bpay-next-runtime-rpc.js`: `82dcd6a55eb521a94d1acf88c5c2fc17a2e71a42fdb213b27ac74a6a6e0a0078`
- `bpay-next-preparation.js`: `604be4c3ab603aee49c41e137222be4365cc0ca4a66243dcdc369a75384a8c2b`
- `bpay-next-preparation-expiry-rpc.js`: `f173c7e0a9b1f037f32c324b711581115b72dc64162fcfd1582571c680d8fec4`

## Running per-Candidate Remove support reread (32 definitions)

Fresh supporting migration hash `ee695d33bc9408260c9a0f438eadd9961d7c462e86bd7d73ffcc5514a599ca2f`; repeatable hash `0f5e700dd87559a8cc71e6aaf35f797ea6a6571a8828bf667fad798345fe08a6`. This supersedes the earlier statement that the per-Candidate PREPARING Remove source was pending.

The new exact existing WORKER_RELEASE path is source-consistent with the card: only an existing native worker UUID can be removed; original SEALED admission remains and RELEASE alone gains exact original ENROLLING command/member/input-job proof (`0814:3647–3655`). The locked current active PREPARE job must pass the original active-execution helper before its READY/LEASED/BLOCKED state is retired to DONE/DISCARDED. It retains all cursor, position and completed-prefix fields (`0814:3672–3677`). A separate pending action is still refused. Native economic Case revision and sealed input identity are unchanged; the existing manual action increments execution revision and stores its exact previous/input job association. The admission parent preparing delta is zero for an already PREPARING worker; original READY/REVIEW decrements are retained and the release terminal applies one preparing debit/cancelled increment (`0814:3700–3704`, `3850–3855`). No unelected or unenrolled worker is invented.

The retained old PREPARE receipt now has a separate exact previous-job RELEASE request/command/input-job/module/worker association (`0814:625–640`); the financial dispatcher routes only terminal DISCARDED previous jobs to that owner. This historical receipt preserves old cursor operands and requires no new input fetch. Ordinary PREPARE and expiry receipt schemas remain closed and distinct.

Release WORK pages prelock their bounded Work IDs and bump Candidate financial revision before refreshing. CASE pages are <=50 and obtain actual Work identities through frozen instruction/collection relationships, prelocking Work before the original bounded Case expansion and native release helper. FINAL proves captured count exhaustion and no remaining ACTIVE Work/Case holds, records worker cancellation and original week invalidation, then hands off to projection. Pending ORIGIN completion is retained on the existing worker-action request as <=100 UUIDs and checked against exact current native task tuples; active waits and completed-set clearing advance only the checkpoint. The latter uses its own step before a subsequent page can enqueue a new origin generation (`0814:3769–3791`).

The new release-only projection helper uses native original Work/selected Case membership seeks, each bounded <=100/<=50, includes actual Case collection Work IDs, and retains unfinished ORIGIN tasks. It changes the existing request/job to COMPLETE/DONE only after both native membership walks and all retained ORIGIN work finish (`0814:3544–3579`). Job cursor is the request checkpoint, while native manifest seeks live only in the two supporting private request columns; a waiting step strictly advances that checkpoint. Existing PREPARE CLAIM/STEP, command order, active execution fences, and service lease yield remain the dispatch. The release branch is isolated before the unchanged ordinary preparation projection body (`0814:3929–3932`). Request immutable-field guards permit only these native bounded operands and reject COMPLETE with pending ORIGIN work. Original amount mutation helpers/formulas are reused.

No new essential defect was found in these running per-Candidate support definitions or three existing request-column additions. The final whole SQL source verdict is still awaiting the previously identified dedicated discard enroller NOWAIT correction. This is source review only; installation, source gates, runtime, database fixtures, and UI/A4 are not performed or asserted here.

## Final current-source closure

Fresh reread confirms the dedicated discard enroller uses `FOR SHARE NOWAIT` at `0814:166`. The preceding 32-definition amendment carried this pending item forward from the older read; it was already corrected in the exact `0f5e700dd87559a8cc71e6aaf35f797ea6a6571a8828bf667fad798345fe08a6` source and is now independently verified closed. Migration remains `ee695d33bc9408260c9a0f438eadd9961d7c462e86bd7d73ffcc5514a599ca2f`.

**SOURCE PASS for the current 32-definition SQL/migration and the six Worker files hashed above.** All recorded essential findings are closed; no remaining essential SQL/Worker source defect was identified. This verdict covers whole-run discard and the added existing per-Candidate WORKER_RELEASE support during preparation. It does not certify local installation, tests, database/business calls, fixtures, hosted publication, UI/A4 or item delivery. Those remain root-owned verification and receipt work.

## Fresh bounded Case-array and source-gate receipt

**SOURCE PASS reaffirmed** after the per-Candidate CASE loop correction. Its DISTINCT `v_case_ids` is populated once from the <=50 native hold slice before Work/Case prelocks (`0814:3814–3826`); the loop now advances only release counters/cursor/visited count (`3817–3830`) and passes that original bounded distinct array to refresh (`3834`). It no longer appends duplicate Case IDs after the prelock budget calculation. This preserves native release money and fixes the literal mixed-page bound.

Fresh diff review of all six changed Worker source files, including `bpay-next-preparation-expiry-rpc.js`, found the same previously reviewed closed discard request/response, exact kind maps, separate retained PREPARE receipt and advancing visited DUE cursor; source line-ending preservation introduced no additional behavior.

The final-definer registry records all32 exact physical definitions in0814 with no unrelated final-owner replacement. Source-gate additions enumerate named current routine signatures and exact per-signature call edges. They preserve the original forbidden dynamic/history-reader checks; new helpers are included in routine checks rather than excluded. Typed public PROGRESS/WORKERS/financial-service constructors are updated to the exact new named scalar fields and the separately qualified DISCARDED/EXPIRED phase expression. The additional collection-guard history clauses are exact native original-effect/last-effect point predicates tied to their unchanged operation, worker/Work/component, transfer, Case instruction/hold/capacity tuples. They add no general table, whole-body, file or prefix exemption (`scripts/bpay-next-source-gates.mjs:1139–1309`, `1331–1334`). Existing final-definition resolution and monetary decimal text checks remain.

This is an independent read-only source receipt; no compilation, source-gate run, SQL/business execution, database/fixture, browser or hosted operation was performed. UI and item delivery remain outside this receipt.

Fresh SHA-256 after reread:

| Saved source | SHA-256 |
|---|---|
| `supabase/migrations/11102026_0814_banking_pay_next_preparation_discard.sql` | `ee695d33bc9408260c9a0f438eadd9961d7c462e86bd7d73ffcc5514a599ca2f` |
| `supabase/repeatable/11102026_0814_banking_pay_next_preparation_discard_v1.sql` | `29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c` |
| `broker/src/bpay-next-financial-queue.js` | `933990513dad238624473628ef20a774eb1ae2d766399e4049b1a37af79cfcb3` |
| `broker/src/bpay-next-job-rpc.js` | `28a8ef749a277a7fbcd10a6e5a9caabd50a6b94ab795df411c7bfdf36fda676f` |
| `broker/src/bpay-next-job-step.js` | `70a989b65ddf08df2b6f5f5133ac0521300708021382870730a9f58051907651` |
| `broker/src/bpay-next-runtime-rpc.js` | `62e0ea2e586fcee1c097984a8793e0084c6ff3869e55815df12e404624eae73c` |
| `broker/src/bpay-next-preparation.js` | `572dee1fba45c21c01ae01cd60f5f3675e0b42363450625363bb7473f71eed20` |
| `broker/src/bpay-next-preparation-expiry-rpc.js` | `f173c7e0a9b1f037f32c324b711581115b72dc64162fcfd1582571c680d8fec4` |
| `scripts/bpay-next-source-gates.mjs` | `bb681d698406bee1647f021571006f6983d29ca86fa73521e8484d4a70da4e35` |
| `docs/bpay-next/final-definer-closures.json` | `23d87d32ed2c3ada2bfd90bf116c927cf681500c3eaf605b14f597aa5c0c1d92` |

## Immutable0814 / alias-only0847 successor receipt

**SOURCE PASS reaffirmed.** Read-only definition-by-definition comparison of all10 bodies in `11102026_0847_banking_pay_next_discard_row_projection_v1.sql` with their0814 parents found only `SELECT *` -> `SELECT dr09_row.*` and the matching single native table alias, plus whitespace. After reversing only those two exact alias substitutions, each whole definition equals its parent. Predicates, strictness, null/replay handling, lock modes (including NOWAIT), row bounds, cursor/checkpoint updates, money mutation calls and ACL posture are unchanged. The registry advances exactly those10 physical definitions to0847; the other22 definitions remain0814 owners.

The source gate preserves the installed0814 predecessor with its exact SHA-256, then enumerates finite exact `(routine signature, ROWTYPE hazard, statement)` keys with original occurrence counts. Only that one physical file receives this map; corrected successors are checked normally. This is not a table/body/file exemption and cannot admit different installed0814 bytes. The added0559 historical preparation projection participates in the existing proof requiring exactly two `MIN/MAX(p.selection_no)` aggregates, the original native Work/Case selected expressions, and the two original bounded `order by s.selection_no limit p_limit` subqueries. The BIGINT alias type map is still applied only to the named original preparation-projection statement. Exact per-signature history/callee checks and typed transport checks remain in force.

Fresh reviewed SHA-256:

- Immutable0814 repeatable: `29f3099ebfb8a7e11d8c23a8996c5351eb9330316d8bdb6fd6d3a5136d3f379c`
- Alias-only0847 repeatable: `86343d45963a9335de8f3af0a3465cbbcdd5c4c2fd3eeef69c628060c8df1c47`
- Source gate: `850b238f09d112c65d54bec70a7abed022b1b739dce5a5c20e110c961225ed0e`
- Final-definer registry: `f0466b0131d0d669b466a2a230718e0b4df344823719e52dc3622aa17587640d`

No essential source defect identified in this amendment. Root reports local installation; this independent receipt does not execute or certify installation/runtime, tests, fixtures, hosted work, UI/A4 or item delivery.
