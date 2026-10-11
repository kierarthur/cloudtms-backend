# DR-05 transport and review-row UI investigation

11 October 2026. Independent read-only investigation of the current saved backend and F-16 Office source. Read the full DR-05 card, all D01–D11 checklist entries (including shared-owner D07/D10), the retained-retry and worker-release design notes, current B4 SCR-43/43b/43c specs and PNGs, SCR-45/45b wording, DESIGN_INDEX and screen policy. Product, checklist, pack and business data unchanged. No execution, tests, fixtures, screenshots, hosted calls or Git mutation. Application acceptance and real product pixels: **NOT_RUN**.

## Narrow conclusion

Build exactly two authenticated preparation actions and two Candidate review-row actions in the existing NEXT Office host. Reuse native WORKERS and STATUS reads and the existing journal recovery. Keep the queue kind PREPARE for the separately bound release/retry phases requested by root; extend its closed phase/checkpoint schema rather than introduce another queue/reader/screen. The database must first supply a definite typed request/receipt and active-revision fence. A transport scaffold does not prove retained-input retry or released-hold conservation.

Current human E5 and the **current supplied** SCR-43b spec agree: Try again repeats the exact payments/items/figures captured at Create Draft, fetches nothing new and requires no preliminary user check. The older card/checklist “new intent/cutoff/newer figures” example is superseded. Do not call CREATE, replay a choice upload, refresh approval inputs or query new economic facts on Try again. E2 automatic release uses the same native release owner; Office never submits a SYSTEM actor or invents an administrator identity.

## Existing source and exact gap

| Owner | Current saved evidence | Indispensable delta |
| --- | --- | --- |
| Broker preparation route | `broker/src/bpay-next-preparation.js:11–25,61–63,473–529`: exact action/input and receipt maps; active admin guard; unknown mutation truth; only SEAL_ENQUEUE requests ENROLL | Add exactly WORKER_REPREPARE / WORKER_RELEASE request and receipt schemas, identity checks and closed refusal mapping. Both durable acceptances request the same ENROLL hint. Keep selection pages/reads non-waking. |
| RPC wrapper / index | `bpay-next-preparation-rpc.js:2–15` derives allowlist from preparation maps and calls existing public intake; `index.js:198855–198878` consumes the accepted handler header | Preserve wrapper authority and existing endpoint. No third RPC/route is needed. Update index comment to name the additional accepted actions; retain its same exact `{id:'1',kind:'ENROLL'}` hint/recovery semantics. |
| Office transport | `js/bpay-next-office.js:26–62,279–291,320–356`: closed INPUT/RECEIPT schemas and typed mutation errors | Mirror only the two new request/receipt schemas. Validate command/run/worker/revision against the clicked subject. Use only receipt fields actually supplied by the native owner. |
| Office review host | `office.js:1056–1064,1105–1112`: current WORKERS paging, REVIEW/Confirm owner and thin Frozen preparation progress table | Existing rows expose worker ID, Candidate ID, native worker status, frozen figures and issue. STATUS header adds preparation_revision, issue Work ID and channel. The table currently has Open worker, with generic Review required; neither required new action exists. Add the card’s row controls here, without building future DR-15 totals/tabs/Not-in-Draft screens. |
| Existing journal | `office.js:761–859,1091`: actor-keyed persisted exact pending bytes; save before dispatch; one pending mutation; explicit same-byte retry after unknown | Reuse this controller. On a genuinely new confirmed row intent generate one command UUID and retain exact expected revision; after unknown never generate another UUID or replace/rebind the subject. Readiness becomes invalid before dispatch and is restored only by native readback/REVIEW. |

WORKERS has no native PENDING enum, no per-row preparation_revision, and no Candidate display name/Timesheet reference/summary totals. PREPARING is the factual unfinished state; never manufacture a PENDING database status. Bind the clicked worker to the existing STATUS header to obtain its exact revision and recheck row eligibility before confirmation. An unclaimed Candidate with no worker ID cannot be made actionable by a fabricated worker. Required names/references can use already available factual parent/Candidate context; omit unavailable decoration under owner A2. UUIDs stay internal. Do not add a reader/field or query fresh financial inputs to imitate the 225-row sample.

### Required host handoff

The normal native nav opens this F-16 host at `js/main.js:7159`, but the new Create Draft journey uses **createChoiceSubmission’s separate journal**. At `office.js:999–1013` it opens `openBpayNextPreparationProgress` with the accepted run address and never binds/adopts the old createController’s sealed-run state. That opener (`912–923`) currently only reports Draft creation started and submission-readback recovery. The older WORKERS table renders only after its separate saved controller is selection_sealed (`1077–1083`). **Adding actions only to that old table would not satisfy the normal new-Draft D09 path.**

Add the narrow card-required accepted-run review handoff using the existing standard-window adapter, exact original accepted run address and WORKERS/STATUS. Expose that existing review owner from the current accepted-run opener, without claiming DR06’s unbuilt full progress or DR15’s future full review window. It must preserve any existing uncertain controller request; do not clear/reset an unrelated journal or fabricate sealed state to force the old renderer. Native REVIEW can establish the run’s factual readiness/review revision after exact accepted identity qualification. The supporting entrypoint is part of making D09 reachable, not authority for another screen programme.

The older B4 CRITIC paragraph recommending newer approval input is superseded by `CHANGES_20261010d.md` D1, current SCR43b and current policy SDP-60a: the Create Draft cutoff has no exception. Destructive-button placement follows current policy SDP-21 even where a supplied confirmation picture places its red button right; record this required policy difference in A4.

## PREPARE release phases and recovery

`bpay-next-job-step.js:34–36,49,83–94,162–198` already has two bounded checkpoint shapes: RESERVE/ALLOCATE carry a Work UUID + component cursor pair; FINANCE_ALLOCATE/WORKBENCH_WORK/WORKBENCH_CASE carry nullable positive integer checkpoint + limit 1–100. Use the existing **integer checkpoint** shape for a bounded release phase if that is the native release owner’s actual checkpoint. Add only the exact phase names chosen by that owner to input/CLAIM/result phase maps, checkpoint validation, terminal/done rules and continuation dispatch. Keep cursor values decimal strings; no SQL arithmetic in JavaScript.

`bpay-next-financial-queue.js:96–110` compares the exact phase-specific cursor tuple, then obtains a fresh CLAIM and one STEP per delivery. Extend the same-phase progress comparison for each added release phase so unchanged/behind-cursor replies cannot cause a drain. Preserve exact `{id,kind:'PREPARE'}` wakes, nonce/epoch/cursor held only in native state, null nonterminal STEP deadlines and publish-before-ACK. An unknown CLAIM does not grant adoption of an active lease; an unknown STEP recovers from native durable state, with original receipt identity retained.

The intake request must distinguish RELEASE from REPREPARE, bind original sealed command + worker + expected preparation revision, and preserve original run-command identity. E2 release completion and explicit Remove complete only after bounded Work/Case hold cleanup, once-only parent bucket debit and the existing bounded Workbench refresh. An acceptance receipt is not “removed”, “Ready” or “Confirmable”. These database semantics and the ACTIVE-prefix / released-hold retry identity gaps remain governed by the companion design notes; this investigation does not weaken them or reuse whole-run expiry admission unchanged.

## Exact screen/control purpose

Use read-only final B4 references: `batches/B4/png/SCR-43_remove_candidate_confirm_dark.png`, `SCR-43b_prepare_again_confirm_dark.png`, `SCR-43c_review_row_results_dark.png`; matching JSON specs; SCR-45/45b reason wording and SCR-44 host style. T07c/d are superseded per DESIGN_INDEX. The three SCR-43 PNGs were independently opened and inspected.

- **Try again**: quiet row action on an eligible unresolved pre-confirm Candidate; asks to repeat that Candidate’s retained inputs. Small info confirmation: “Try [Candidate] again?”, supplied same-payments consequence, Cancel and one primary Try again. No new-input promise or prerequisite checklist.
- **Remove from Draft**: quiet row action for the card’s pre-confirm release owner. Small destructive confirmation: “Remove [Candidate] from this Draft?”, payments return to Banking Pay for a next Draft, Keep in Draft and native red `btn-warn` Remove from Draft at footer left. Do not promise the payments are Ready when other native holds can remain.
- **Existing Refresh / Review and header Close**: obtain native state / exit; close keeps uncertain journal. No extra action suite, Discard-this-Draft, confirmed-Draft Cancel or future full review framework is justified by DR05.
- **Unknown receipt recovery**: existing explicit retry of the exact journal request, with plain factual outcome-unknown copy. Disable competing row actions and Confirm while pending; do not clear the journal after a later auth/refusal fails to establish the earlier outcome.
- **Results**: in the existing review parent, per SCR-43c; accepted retry shows Updating… / Trying again until native completion. Completed remove then shows a success notice; refusal leaves the row To review with one sentence. Use current SCR-45b’s two plain could-not-prepare sentences (including generic unknown failure); hide raw codes/UUIDs/phase names. Do not count E2 automatically excluded cases as unresolved review after actual cleanup.

Use the standard native frame/footer/notices/badges, one border for tables, right-aligned tabular money, em dash only for unknown value, no accidental zero. Owner 11 October action/caret/phone rules apply: no redundant Expand text or one-component duplicate row; only required controls; 390px targets at least 44px; phone-specific Open only where its existing card owns an Open action. Native `[hidden]` and live-copy markers must survive Office modernisation. Record every visible difference from supplied PNGs under A2/A3/A4 before later approval.

## Required actual screenshot states

After root implements the native contract, capture actual Office dark, device-light and 390px phone: unresolved review rows with both controls; each confirmation; retry accepted/updating and later READY or still-review truth; remove accepted/cleanup and completed removal; stale-revision refusal; unknown receipt + disabled competing actions; close/reopen retaining that exact request and successful explicit recovery. Also show nineteen Ready / one unresolved blocking Confirm, then the native completed exclusion or retry result enabling only the legitimate remaining set. E2 system release needs factual resolved output evidence, not an extra screen or browser-submitted system action.

No sample 227 totals, extra channel counts, expiry timestamps, candidate names, Timesheet list or gross/deduction totals may be invented where current readers do not supply them. Full DR-15 review, DR-06 progress, DR-09 whole-Draft discard and later confirmed-Draft actions remain their own cards. Screenshots can use root’s bounded local fixed responses when available, with separate provenance and no installed-app claim. A4 pixel approval remains pending until actual product captures exist.

## Read-only provenance

SHA256 of saved bytes inspected in this investigation, prior to root’s DR05 edits. These identify the baseline; they are not source/pixel approval of a later implementation.

| Source | SHA256 |
| --- | --- |
| BE `broker/src/bpay-next-preparation.js` | `594e62ce0cc860853e8a9462a7d901afc456c9c7ebba732a40d580e00fb65760` |
| BE `broker/src/bpay-next-preparation-rpc.js` | `642a7d989f526c821b36eb58b1bb00f32439da953bc260c35ac290601fd37c34` |
| BE `broker/src/bpay-next-job-step.js` | `1ae85f5636a7a401f3d73dc288c39502a7160b1727a5b248b37db22b7f0d954c` |
| BE `broker/src/bpay-next-financial-queue.js` | `1311b89eb9b07b961140f64ee07b710170fdae975ea83e1718b72d020c6fc948` |
| BE `broker/src/index.js` | `5bc63c0e68c95913535e0a8947c0bbe25bfb899005ef15b811b4d14b798bf40d` |
| FE `js/bpay-next-office.js` | `af49a6df915410fb6c11ddc5c389d3f27f64ff018eee6ce61d05b1115f513e51` |
| FE `js/bpay-next-ui.js` | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
| Pack `batches/B4/specs/SCR-43b_prepare_again_confirm.json` | `0db9295a6218b6a8a1d49359139763c1c6af8823a9ebf62ead72d2e63f49b2a5` |
| Pack `batches/B4/png/SCR-43_remove_candidate_confirm_dark.png` | `cb0b8feb89564d0f64b4eceee4b9f5179ae63c2088ddc112ddc60897aba491e0` |
| Pack `batches/B4/png/SCR-43b_prepare_again_confirm_dark.png` | `03cb14d0dcd3c426ad756f9168107a2a7b8e6b95cc5c244df80c0c90eee733b0` |
| Pack `batches/B4/png/SCR-43c_review_row_results_dark.png` | `29feabc65b6f23826b2b3fa5bbd6ca138a994a6b0e38516c4e5df145687926e5` |
