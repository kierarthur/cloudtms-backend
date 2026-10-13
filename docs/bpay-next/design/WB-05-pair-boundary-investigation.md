# WB-05 pair/publication boundary investigation

Read-only source investigation, 10 October 2026. Read the complete WB-05 card (`02_WORKBENCH.md:212–246`), all nine child checklist rows, complete current pair1540 body, publisher/Processing1346, relevant1252 projection locks/schema and1332 withdrawal hooks. No product edits, SQL, tests, runtime or business actions. Rule B and the parked F25 boundary remain unchanged.

## D09: projection locks already held; exact Client FK scope needs preholding

The pair remains the same owner-only `(uuid[],uuid[],uuid[])` interface, one or two exact works/revisions/events, one Candidate, sorted publication order and all-or-none exact replay. No new publisher or command identity is required.

`supabase/repeatable/03102026_1540_banking_pay_next_source_ordered_publish_v1.sql:57–69` proves one actual Candidate and acquires Candidate key share plus worker-control update lock. Lines74–99 acquire both exact WORKs, selected/current/applied revisions and physical Timesheets before the first command. Lines141–150 call the existing publisher in sorted WORK order. The genuine Source event, revision source kind and immutable replay conflict/partial-replay refusals remain at lines105–137.

The first publisher's1346 Processing call at lines216–221 precedes its receive_command clock. The helper reuses the already-held Candidate control and WORK (`1346:18–20`), creates/locks the same Candidate metadata (`1346:21`, `1252` repeatable15–27), then updates and retains the singleton Workbench revision lock (`1346:33–35`). Only afterward does it replace the exact group/item and call apply_delta (`1346:36–70`). The delta owner itself takes that same revision lock before candidate/scope summary inserts or updates (`1252` repeatable99,113–128).

Consequently, a second WORK group is a different row but does **not** introduce an independent contended projection writer: current refresh_work/refresh_case/mark_processing take the Candidate control before their exact group replacement; all summary mutation is through the revision-locked delta owner. Metadata is the same Candidate. The first publisher retains these locks to transaction end. A second mark_processing is necessary for its own contribution/barrier, but pre-running both helpers would duplicate invalidation and revision changes and is unnecessary.

There is one distinct narrow FK issue. The1252 migration gives group/item `client_id` and `contract_id` real public FKs (lines36–37,77–78), Candidate summary a Client FK (185), and scope summary a Client FK (223). Mark_processing reads the selected WORK's current Contract.client_id (`1346:24`) and writes those Client references. Pair1540 explicitly preholds Candidate/WORK/revision/physical identities, **not** the Client rows. Current stage_source (`1236:1377–1380`) key-shares its Timesheet/Contract but plain-reads the Client (`1236:1612–1614`); protected staging likewise plain-reads Client (`1236:2598`). The singleton projection revision does not serialize a public Client key change/delete. Thus the pair owner's present lock list does not itself establish the second member's Client FK lock before the first clock. Upstream Source locks must not be guessed as a substitute for that finite interface guarantee.

Minimal D09 support: after existing exact WORK/revision/physical prelocks and the complete all-or-none replay check, before the publication loop, prehold **only** the pair's selected Contract/Client identities in deterministic order. Hold selected Contracts with `FOR SHARE` if retaining the helper's current Contract.client_id read, then key-share their exact non-null Clients. The share lock is needed to keep that selected Client mapping stable; Contract key share alone permits a non-key client_id update. The existing selected physical/Candidate/WORK/revision key protection already covers the other non-null Processing references. No Case, approved-line, snooze, run or command reference is populated by this informational helper. These finite reads require no money, history, ancestry, query or source-discovery scan. Keep this at the pair seam; preserve original Source owners and all original replay/security/ACL behavior. Do not prehold unrelated summary keys or introduce a generic protocol.

This recommendation is the pair's concrete FK closure, not a claim that observed projection row contention demands a new global framework. Exact upstream owners can already hold some of these locks, in which case preholding is reentrant; the bounded pair still establishes its own guarantee for both members.

## Existing bypass approval/current-pointer hooks

A source-only census of the registered final physical function bodies found exactly three update owners setting WORK approval_state/current_revision_id: publisher1346, ordinary authorisation trigger1332, and Source withdrawal guard1332. The publisher already calls mark_processing before its command clock. Both actual bypass owners already have exact refresh hooks:

- Source withdrawal: `10102026_1332_banking_pay_next_withdrawal_projection_v1.sql:379–382` qualifies the real withdrawal transition under NEXT; lines391–402 acquire Candidate control then exact WORK; lines414–418 retain the original Source financial-dependency refusal; lines419–427 clear current approval, preserve applied facts, advance financial revision and refresh exactly that WORK. Its refresh_work owner handles real before/after WAIT and only the genuine linked origin Cases (`1332:57–59`,278–280). Do not add a duplicate withdrawal callback or remove its Source restriction.
- Ordinary withdrawal: the same1332 file352–360 clears current approval, advances exact Candidate financial revision and calls exact refresh_work. It is the companion census entry, not a new WB-05 Source producer replacement.

Stage_source1236 creates immutable ordinary-format Source/Protected revisions and leaves current publication to the pair; it is not an unhooked approval-pointer writer. First Authorise's final outer body preserves its actual successful-core/NEXT qualification, then stage→pair and metadata-only initial binding (`09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3594–3609`). Later receipt publication stages exact sorted members, acquires the existing native source-binding lockset, then calls the same pair (`06102026_1301_banking_pay_next_source_receipt_binding_v1.sql:35–63`). These observations support D01/D02/D07/D08/D09 reuse; they do not by themselves prove every later Final/correction/protected/move application path or SP-12's complete caller census.

D03/D04 retain current1236 Source capture/hold and their named later source-book/query/channel/boundary chains; this investigation supplies no reason to rewrite those money owners for pair projection locks. D05 retains final1332 work replacement and1346 final applied-pointer boundary rather than publishing partially applied earnings. D06 remains specification evidence, not installation/application acceptance. Worker/Office/producer/PIN/enroller changes are outside this pair support investigation. Greater-than100 related owner continuation remains WB44's existing open dependency.

## Source identities

| Source | SHA256 |
|---|---|
| Pair1540 | `f4af5a88d1ede025fdf227df2442a2c49b801104bf02a99a466366663378a4da` |
| Publisher/Processing1346 | `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f` |
| Withdrawal/refresh1332 | `9212749c48615fac296a1af071ad1b7bb187bfc19c92bcba5021bf4fdc602391` |
| Projection repeatable1252 | `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b` |
| Projection migration1252 | `aa9a42864893856c9ae02479edb1172cdf39c764a6ca2e9e77f2fd11253affab` |
| Current Source stage/capture/hold1236 | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| Receipt publication1301 | `24cf44354735d4a452eb3334847294e486ee17905bdc085f402e77b7ed1d75d9` |

Conclusion: shared projection locks need no duplicate prehold/invalidation scheme. The only identified new second-member lock scope is the exact public Client FK target, with stable Contract-to-Client mapping. Current bypass withdrawal hooks are already present. No whole-WB05 or application PASS is claimed.
