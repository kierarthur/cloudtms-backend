# F-25 owner rule B: dormant Office accepted pages and next plan item

10 October 2026. Source-only investigation following the owner's superseding rule B relayed in the current task. Stopped0146 root-helper review; no completion verdict was issued for it. No product changes, SQL/runtime/tests/fixtures or hosted calls. Root implements the finite correction below.

Controlling current scope: keep Worker JS and D03 refusal parsing; park F25 repeatables except2153/2256; accepted-page scaffolding remains dormant and Office must not call it. Do not change Worker routes,1610/1620,0280, enrollment/PIN guards or producer owners for this correction. Existing historical design/review files are evidence of their prior bounded work, not permission to resume the superseded protocol.

## Current Office source does invoke parked accepted readers

The current FE build `js/bpay-next-office.js` is **not yet dormant**. Its two concrete network-read functions are:

| Function / source line | Actual page request |
|---|---|
| loadAccepted,504–506 | controller.page('ACCEPTED_CANDIDATES',{run_id,after_candidate_id,limit:100}) |
| loadAcceptedWork,508–510 | controller.page('ACCEPTED_WORK',{run_id,candidate_id,after_work_id,limit:100}) |

Every actual UI/automatic edge to those functions:

- refresh,528: a sealed preparation, retained accepted_prepare or pending SEAL_ENQUEUE loads ACCEPTED_CANDIDATES before ordinary preparation progress. This also runs on reopening a saved run at624 and after exact pending retry at537.
- Run toolbar,546: “Read original accepted progress” calls loadAccepted directly.
- Prepare button,552: after successful SEAL_ENQUEUE resets local pages then automatically awaits loadAccepted.
- Accepted preparation panel,564–568: “Open accepted Work,” “Next accepted Candidate page” and “First accepted Candidate page” call those readers.
- Accepted Work panel,570–571: “Next accepted Work page” calls loadAcceptedWork.
- Visible2s progress timer,617: when acceptedCandidates exists, calls loadAccepted again instead of ordinary WORKERS progress.

The source search found no second ACCEPTED_CANDIDATES/ACCEPTED_WORK caller in other `js` files. Their schemas/input shapes/enums/decoder branches at53–54,65,71–72,112–118,278–314 are passive declarations. The controller page-adoption block456–461 is reachable only after transport.page and retains identity checks; that block is not itself a prohibition on calling the parked route.

## Required finite Office correction

Remove/disable **only these accepted-reader invocation edges**, without creating an owner switch/feature flag or a replacement accepted-cut implementation:

1. refresh must use existing WORKERS/STATUS/CHANNEL_* progress logic and never call loadAccepted merely because selection_sealed, accepted_prepare or a pending SEAL_ENQUEUE exists. Preserve exact pending request bytes and original saved run/command identity; no replacement command UUID.
2. After successful Prepare, retain the original SEAL_ENQUEUE receipt/journal adoption and local page reset; use ordinary existing progress/explicit Review behavior. Drop the automatic accepted-page read.
3. Omit the accepted-reader toolbar button and accepted Candidate/Work panels/buttons. Their dormant decoder/schema/helper code may stay for later separately authorized scope, but must have no exposed click/poll/open/refresh edge.
4. Timer must retain actor/owner/close/visibility/read-busy protections and select only existing STATUS or WORKERS flow, never acceptedCandidates. Preserve server-driven expiry; no browser drain/review/enrollment work.
5. As a narrow defensive Office seam, a local exact-kind refusal **before** transport in createController.page can guarantee no ACCEPTED_CANDIDATES/ACCEPTED_WORK request escapes from an overlooked caller. Use an existing local request refusal or another exact dormant refusal; do not fake a server response, broaden the kind gate or modify Worker routing. If root retains schema/decoder compatibility tests as dormant references, they do not authorize actual transport. A generic Office transport caller must likewise not bypass the dormant guard; choose the one earliest shared Office page seam used by all intended consumers.

Keep existing current-work selection, cases, preparation mutations/REVIEW, workers/status/channels, advice, history and ordinary financial UI untouched. Keep v4 journal migration and accepted_prepare receipt identity if desired: it is original successful SEAL_ENQUEUE command UUID/agency sequence, not a command to query parked accepted pages. Do not delete saved pending bytes or rewrite old journals. Preserve D03 typed refusals and definite-refusal versus unknown-result behavior. The accepted-identity refusal may direct the user to the original run's ordinary progress; it must not cause an automatic parked read.

This is a real source correction requirement; merely parking SQL files does not make the current Office caller dormant. No product edits were made by this reviewer.

## Exact next plan item is F-11, sequence16

Read the full F-11 card in plan01:701–721, current BUILD checklist390–410, master08:99–103 and generated IMPLEMENTATION_SEQUENCE. Their order is:

```text
15 F-25
16 F-11 Wire the NEXT background runtime (queue consumer, minute recovery tick, wakes, expiry)
17 F-08 Draft-channel/channel-page/cancel/history contract alignment
18 F-13 shared continuation queue safety at one-way activation
19 F-23 release-package/source gates
```

F-11's listed dependencies are F-09,F-10,F-12,F-25. It requires no new design-first card and has **no screen work**. Rule B now controls what the F25 dependency means in this task; do not restart parked accepted-cut/producers/helpers as an inferred prerequisite. Preserve the existing Draft freeze architecture and explicitly record the owner override rather than asserting the superseded F25 implementation is complete. F-26 is a separate later epoch/pause-resume card, not the next item. F-13 owns safe old-envelope retirement; F-11 module wiring must not silently claim it built.

The current F-11 checklist has six deliverables, all four boxes unchecked:

| Row | Exact owning scope under the saved card |
|---|---|
| F-11-D01 | Reuse existing04102026_0560 queue recovery service: enrollment/wake/expiry discovery; only necessary applicable integration. No new F25 resolver/PIN changes under rule B. |
| F-11-D02 | Required NEXT one-message queue dispatch, actual owner/epoch admission and durable recovery; old-envelope retirement stays F13. |
| F-11-D03 | Minute recovery tick with bounded ENROLL/READY/LEASE_EXPIRED/EXPIRY lanes and exact owner gating. Do not introduce F26 resume mappings now. |
| F-11-D04 | Preparation/command ENROLL wakes after durable original receipt, awaited queue sends and missed-wake recovery. |
| F-11-D05 | Existing PREPARE mutable-read gap reference. Rule B supersedes protocol expansion; no proposed accepted-cut/current-read rewrite here. |
| F-11-D06 | Preserve existing PREPARE claim/PIN/order owners. Current task expressly prohibits enrollment/PIN/0280/1610/1620 changes. |

The card reuses existing BANKING_PAY_CONTINUATION_QUEUE/test-cloudtms-banking-pay-continuation, max_batch_size1/DLQ, existing five-minute/minute crons; **no wrangler.toml change**. Job rows are durable authority; queue messages are wakes. Reuse exact NEXT background modules and typed recoverable job issues/bounded retries. Historical queue entry points are reference seams; do not rebuild/operate/test the old engine. Actual shared-queue/runtime installation still needs its separately authorized workflow; this source note does not perform one.

Later acceptance remains separate: browser-closed preparation reaches READY, REVIEW no longer remains ENROLLMENT_INCOMPLETE, server72h expiry and visible job blockers. Owner start sheet §3 prohibits constructing/running a local testing programme or fixtures now. Source compilation/checks do not tick app-tested outcomes. No checklist boxes were changed by this note.

## Saved source checkpoint

| Source | SHA-256 |
|---|---|
| FE build js/bpay-next-office.js at active accepted-call census | `ba5ed0beffb01f0d7c626e8a9d41cf148ed5605cd5602238c335eea482c70a9e` |
| BUILD checklist | `a18ba33d361215ed1697531ce5d90d8a54d6db0868e9fc50d439b5dc7dd515d9` |
| Plan01 full foundation/cards | `edf724bb3e6fee87169d685bbc68284c5561172fa66ab97ac7be154b6d83d924` |
| Plan08 exact master order | `64db4b6328c01336213f02da1caca0889db119c0fa06b803abbda0756c30fc14` |
| Plan IMPLEMENTATION_SEQUENCE | `700e642790bac3e28a283fffebc68c3a9ee7291d43b8b68e3293e5f83f750b3c` |

This records saved-source findings before root's concurrent correction/parking, not an installed release, runtime or full F25/F11 verdict.
