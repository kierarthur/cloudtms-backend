# DY-06 independent source review

11 October 2026; sequence 93. **SOURCE PASS for the saved 1107 source below.** No remaining essential defect found within the card/BD104 scope. This is not installation, compilation or application acceptance. Reviewer ran no SQL, tests, fixtures, business actions or hosted calls and edited no product/checklist. Root’s reported source-gate results are separate evidence; they were not executed by this reviewer.

## Sources and complete-parent comparison

Reviewed full DY-06 card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1008–1040`, checklist D01–D05 (`IMPLEMENTATION_CHECKLIST.md:2583–2599`), current chain, `design/DY-06-scope.md`, finite retention census and saved current owners.

Read-only complete-function comparison removed only the authored DY06 blocks and the direct wrapper’s one added row variable, then compared normalized-LF complete source. All three recovered parents matched exactly:

| Successor owner | Exact parent | Parent / successor lines | Result |
| --- | --- | --- | --- |
| private._timesheet_route_change_context_v1(uuid,text) | 08082026_2035_timesheet_route_version_rotate.sql:1476 | 562 / 567 | Exact original recovered |
| public.timesheet_route_version_rotate(uuid,uuid,text,uuid,boolean) | 10102026_0922_weekly_source_pay_query_identity_admission_v1.sql:2140 | 178 / 215 | Exact original recovered |
| public.timesheet_route_version_confirmed_v1(uuid,uuid,text,text,text,uuid,text,text,text,boolean,timestamptz) | same0922:2322 | 370 / 376 | Exact original recovered |

Thus original Source admission/managed-root/import checks, family/current-identity/financial/workflow locks, permission/feature/confirmation branches, signature/context reproof, workflow supersession/notifications, output contract and internal dispatch bodies are retained. No internal legacy/core replacement or QR feature rewire is present.

## Financial classification and native bounds

`11102026_1107_banking_pay_next_daily_route_retention_v1.sql:7–79` adds one STABLE SECURITY INVOKER private classification helper. It retains the actual booking family, guards the 33-row physical sentinel against maximum32 before use (22–31), and also refuses a 33-row Contract Week sentinel before native history fallback (33–41). Context passes the already-read native history; direct rotation calls the same existing native removal owner. It retains every existing blocker except TIMESHEET_STILL_AUTHORISED (44–46); the global sticky Archive marker and removal reader are untouched.

The helper reads exact native Work by immutable booking identity; work_choice, captured run_work and financial_effect EXISTS have no run/choice/job status narrowing. Accepted OPEN/PINNING Work choice is already capture, while skipped choice-members are not promoted to capture. Existing choice/selection/revision FKs and Work identity supply the association. This satisfies capture-before-runline Rule B without any parked2153 table/resolver or new accepted-input storage.

The twelve legacy roots match the original finite native capture/backfill meaning: all physical TF versions with retained payment/remittance fields; any batch item; force-include UUID[] overlap using existing GIN; any Timesheet snapshot; exact original settled/signature/snapshot/processing pay-state predicate; any pay-state history, adjustment, Case component, advance, correction, carry-forward or override. No current-only, active-only, item-kind or terminal-status shortcut drops retained facts. The exact native pay-state IS NOT NULL fields are preserved even where schema defaults are non-NULL. There is no money aggregation, ledger rewrite, financial formula or inventory reconstruction. Sources: original monolith `26052026_2100HRS_NEW_FUNCTIONS.sql:183474–183812`, finite `design/DY-06-retention-census.md`.

Context changes only genuine DAILY under active NEXT, after its original native history read. Original non-NEXT/non-Daily classification and import → financial → auth → workflow order remain. Current paid/invoiced checks stay independently in the original context. Approval marker alone therefore ceases to be a financial blocker, without deleting the marker.

## Mutation locks and original refusals

Confirmed takes exact native Work UPDATE NOWAIT immediately before its original fresh context call/signature reproof, after its existing native family/TF/workflow/request locks and before supersession. The conflict with accepted Create’s original Work SHARE (`0426:292–298`, native choice guard26) serializes accepted capture and destructive rotation. NOWAIT prevents a new blocking Work-after-Timesheet wait edge; no catch permits continuation without the lock. The later dispatcher call re-enters the same retained Work lock.

Direct rotation takes the same Work lock and financial-only refusal inside each original valid legacy/core branch. It retains the original feature/service choice and original confirmation-GUC refusal before core dispatch. Qualification uses the actual expected current physical row in the original booking family, accepts an older caller anchor, and leaves missing/stale/unknown inputs to original validation. Full Candidate context/permitted-action logic is not imposed on dormant legacy dispatch.

Initial review found that generic recognized action lists could mask core’s Daily paper refusal (`2035:164–175`) or WEEKLY-only SWITCH_TO_MANUAL (`408–410`). **Closed in the final saved source:** both direct fences are limited to ALLOW_ELECTRONIC_AGAIN, SWITCH_DAILY_TO_MANUAL and REVERT_TO_ELECTRONIC (`1107:812–814,843–845`). These are native Daily-capable actions; ALLOW_ELECTRONIC_AGAIN remains covered, preserving its existing Daily permitted route (`2035:1931–1935`). Incompatible paper/WEEKLY targets reach their original bodies/refusals. The original stale-preview checks remain before confirmed’s final permitted-action refusal, so changed financial context retains the existing context-changed outcome and warning details.

## Indexes, ACL and scope

The migration adds exactly three point indexes: private Work choice(work_id,run_id), public pay_advances(linked_timesheet_id) WHERE non-NULL, and public carry-forwards(timesheet_id) WHERE non-NULL. Their predicates retain every status and support only the helper’s actual key seeks. All other roots reuse existing key/GIN support listed in the census. No public trigger/table/column/grant, business rewrite or backfill is introduced. Installed-catalogue absence/type/index proof belongs to root’s evidence; reviewer source does not claim a live query plan.

Helper/context execute is revoked from PUBLIC/anon/authenticated/service_role; only existing public mutation wrappers regain service_role execute. Owner is the existing audited logical postgres owner. The helper accepts only trusted private/native context history, not an exposed browser field. Registry contains the helper and actual private2035 context ancestry plus current1107 public final owners. No new route/command/protocol/secret or money owner appears.

## Correct retained withdrawal and Office integration

Current0939 OLD withdrawal remains unchanged (`11102026_0939_banking_pay_next_contractless_daily_publication_v1.sql:874–915`): exact OLD physical UUID/version and NULL-safe Candidate/Contract binding, original Candidate/Work locks, only current offer WITHDRAWN/current revision NULL, financial-view bump then existing refresh. It creates no authorised zero and changes no captured Draft/hold/effect/history. Reuse is valid D01 evidence.

Native Office Worker remains `broker/src/candidate-app-backend.js:11057–11117,12427–12435` (permission, preview and ROUTE_CONFIRM with expected identity/context); FE controller102–149/API92–106 still previews, presents original warning, confirms and reconciles. Bridge645–672 maps genuine Daily to SWITCH_DAILY_TO_MANUAL; approved policy29–64 and warning W04/W05 are unchanged. Native withdrawal success remains `main.js:179321–179327`; NEXT Office retains WITHDRAWN at `bpay-next-office.js:283`. These are D02/D03 source integration facts, not app acceptance. No current rendered Daily QR restore/send control was found; the separate unused generic legacy QR endpoint is explicitly bounded in the scope/lock note and was not rewired under this card.

## Final source identities and acceptance boundary

- Current SQL repeatable1107: `F7964500C4C691390EB75A1150A634A7E73BD2918747EF57CB7CE81DE139411F`.
- Migration1107: `4CC1FCB40ABE6545465625E1FF0F34559DE76FBA5B151A617C0DE0557E6E5324`.
- Retained0939 withdrawal file: `4A21AB611B90ED39FED312A5D8AAA0754E72F980DA9C621EE815EC1F611C6C1A`.
- Worker Candidate Office: `162DC06CEAD09DE49FC9D7B60F1189EAC3C5420544F7872BB158A428F4E8379B`; index: `8DECF8F784118C153E4E5D2DB0848050812B8ADAB72E5B8B4A53A2E48F2358C0`.
- FE main: `A65011BEF9BB78BB83E4B8E2AB7481690DD724C83EF6B3716B79FF6C11292915`; NEXT Office: `0DCF5FC982195684FC2001985BB258A6C9397B89B7C34F0888612860C0F77C15`.
- FE controller: `189AEF8B0B2DA7994D601D28A14EAE509A4C7D4E7F924FA21D23A97C4BC847A3`; API: `BB1AE7966A5EBF6E5E719662E874DD2368D84420D92B3AC3C428362B57A8F21E`; bridge: `73520A0019D0C00CBB4920CC4A8CA9BD57802D3B37FC234A47A54151349A451B`; policy: `471F0A0A040FAE4718BCC59845823FBFB4CF60211EEF4DC8581D3AC5EA9C867C`; warnings: `D1B6A44DD6522ED46302F7D062D07A18349F172BAFE5D2DC41940BD56B669778`.

Final verdict: **SOURCE PASS**. Local compilation/install, required root gates, actual Office/native interactions, accepted-click race behavior and the card’s joined lifecycle acceptance remain distinct root verification; reviewer/application acceptance is **NOT_RUN**.

### Final saved-byte amendment

Final 1107 repeatable SHA256 is **F7964500C4C691390EB75A1150A634A7E73BD2918747EF57CB7CE81DE139411F**, superseding the initial review’s historical repeatable identity `E3A8B44472CED901AFE91308CD76A152DEBE7672B7E68883B8E515C1C40C83D7`. The only final helper delta removes ORDER BY from its two LIMIT33 native membership sentinel reads (physical family22–24, Contract Week33–35). Both >32 refusals remain before use; unordered UUID membership is equivalent for the helper’s ANY/overlap/EXISTS predicates and avoids sorting an oversized malformed family. Current complete-parent inverse comparison again returned exact equality for all three wrappers/context. Current ACL, three valid Daily direct actions, Work NOWAIT placement, financial predicates, indexes and retained Worker/Office/OLD withdrawal reuse remain as reviewed. **SOURCE PASS remains**; no execution or application acceptance was performed.
