# WB-17 source identity investigation — 10 October 2026

**Static investigation complete. Narrow reader/navigation work remains; no product changes or runtime claims.** Current saved sources, not historical card line numbers, were inspected. Rule B remains authoritative; no parked F25 resolver/protocol or producer changes are needed.

## Exact scope and current winners

Full card `02_WORKBENCH.md:702–726`, R2:208–210, master sequence 65/dependencies WB15/WB21/F16, and all seven current checklist rows were read. WB17 requires exact physical Timesheet/version navigation, Source root/week navigation, a stacked child preserving Banking parent state, and explicit newer-information presentation after Create. `work.original_timesheet_id` is family provenance and must not be the opener when a newer approved revision exists. WITHDRAWN is the one explicit current-reader exception: use its exact applied revision, never an arbitrary older usable revision.

| Row | Actual object / required disposition |
|---|---|
| D01 CONTEXT | `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` — reuse current 1236 capture, no edit or accepted-protocol rewrite. |
| D02 DELIVERABLE | `public.bpay_next_frozen_detail_page_v1(uuid,text,uuid,uuid,uuid,integer,integer,text,text,text,integer)` — registry physical/root `04102026_0250_banking_pay_next_frozen_detail_service_v1.sql`; exact run-line/run-work authority is already correct. Preserve the function and SHIFTS/BREAKS/RATE_DETAILS/RATE_SCHEDULE domains. |
| D03 CONTEXT | Office thin shell / durable choice journal — preserve existing WB15/WB21 owners. |
| D04 SCREEN | Existing Timesheet or Source evidence child; return same parent/page/scroll/unticks. Requires native caller integration, not closing/recreating Banking. |
| D05 CONTEXT | Reviewed card, no independent product target. |
| D06 DELIVERABLE | `createBpayNextFrozenPageRpc` — existing fixed service RPC, actor supplied by authenticated broker, abort signal/raw Response retained. Source reuse, no new RPC. |
| D07 DELIVERABLE | `handleBpayNextFrozenRead` — existing admin/auth, closed fields, byte/row/time bounds and exact continuation retained. Source reuse, app acceptance remains NOT_RUN here. |

The current common reader and public preparation dispatcher are both physical/root `10102026_2026_banking_pay_next_current_group_details_v1.sql`; the work classifier remains 1946. Source staging remains 1236; its producer and maintained projection writers need no WB17 changes.

## Smallest current reader additions

`CANDIDATE_GROUPS` currently returns maintained `g.physical_timesheet_id` and `g.current_revision_id` (2026:120–161), while `ISSUE_DETAIL` returns `i.physical_timesheet_id` and `i.revision_id` (278–329). Both omit physical version and Source origin type/root/version/week. The classifier already chooses `CASE WHEN approval_state='WITHDRAWN' THEN applied_revision_id ELSE current_revision_id END`, scoped to Work, and stores its exact physical identity (1946:128–155). In a withdrawn group `current_revision_id` correctly remains NULL; do not rename/retag it as applied.

Add a finite typed factual suffix to these two existing row projections: `physical_revision_id` (UUID nullable), `physical_timesheet_version` (positive integer nullable), `source_kind` (existing revision enum nullable), `source_head_id` (UUID nullable), `source_root_timesheet_id` (UUID nullable), `source_root_timesheet_version` (positive integer nullable), and `source_week_ending_date` (DATE nullable). An existing `physical_timesheet_id` remains the exact physical opener. Root may also retain `source_event_id` if needed to explain the original authority; it must be the exact retained revision field, never today’s event.

For CANDIDATE_GROUPS preserve the existing ten-presentation-identity `LIMIT p_limit+1` seed at 2026:496–498, native sort tuple and FK parent join. After that seed, PK-join Work by g.work_id and Candidate; PK-join only the explicit current/withdrawn-applied revision with `r.work_id=w.id`, checking `r.physical_timesheet_id=g.physical_timesheet_id`. A row mismatch is unavailable evidence, not a fallback. Standalone Case rows remain legitimately NULL for Timesheet/Source navigation unless they independently carry a genuine native source origin; do not infer one from the Candidate.

For ISSUE_DETAIL preserve its own scoped item/reason/group seed, order and limit before PK factual enrichment. Its exact `i.revision_id` must agree with the Work’s explicit current/withdrawn-applied choice and physical ID. Keep the original affected-Candidate count, unchanged cursor, row count, group/reason membership and byte envelope. Do not filter out an owning issue because optional origin enrichment is absent. Keep existing ten read kinds; no new table or index is needed for these <=100 PK joins. Mirror the suffix through closed Worker/FE row validators; otherwise the actual transport will correctly reject the extra fields. No producer write is required.

## Genuine Source head NULL versus missing evidence

Core revision schema 1324:132–176 retains immutable `source_kind`, nullable `source_head_id`, unique nullable `source_event_id`, non-null physical Timesheet/version and revision week. Native heads carry Candidate/Contract/week/root/physical root version (1534 schema:2666–2678). The following branches are real, distinct authorities:

1. **Head-backed SOURCE or PROTECTED.** Join exact `h.id=r.source_head_id`, and compare `h.root_timesheet_id=r.physical_timesheet_id`, root version, `h.candidate_id=w.candidate_id`, `h.contract_id=w.contract_id`, and head/revision/Work week. The stage owner verifies these at 1236:1434–1450 and stores that exact root in the immutable revision at 1644–1658. Historical identity does not require the head still to be COMMITTED_CURRENT; never join the current head pointer or newest family head. Missing/mismatching exact head makes Source identity unavailable.
2. **Initial SOURCE authorisation, head NULL.** This is intentional. 1236:1381–1385 defines first snapshot by expected head NULL; 1414–1431 verifies the exact root authorisation, `p_source_event_id=actual authorisation.id`, and no former head; 1644–1658 stores that actual supplied root/version. Therefore `r.physical_timesheet_id/version` are genuine root identity, not a guessed original Timesheet. For a bounded independent proof join `weekly_source_root_authorisations.id=r.source_event_id`, compare its stored root/version/family with the revision/Work. Do not require current_head=NULL or not-withdrawn today: those are mutable lifecycle fields, while retained original identity is valid. Root authorisation has no own week column; the sealed revision’s DATE is the captured Source week. Never manufacture a head to fill the NULL.
3. **Initial PROTECTED approval, head NULL.** 1236:2552–2558 reads actual `weekly_exceptional_payment_approvals.id=p_approval_id` and generation; 2571–2594 validates actual supplied physical root, Candidate/Contract/week and protected Source route; 2602–2612 stores physical root/version, `source_kind='PROTECTED'`, head NULL, and `source_event_id=p_approval_id`. Its physical revision is genuine Source root identity. Exact approval join by event proves Candidate/Contract/week; approval `evidence_timesheet_id` and signed-submission ID are evidence origins, not replacement root IDs. If original Source cycle/week evidence is exposed, join only `approval.source_cycle_id` explicitly; do not locate a current cycle by week. Do not apply this branch to a normal head-backed protected revision.
4. **ORDINARY/EXPENSE/ADJUSTMENT or unknown original event.** Physical revision remains exact if present, but Source root/week stays NULL. A NULL head alone does not authorize calling the row Source. No `COALESCE(head root, work.original_timesheet_id, newest physical)` fallback. A Source-typed revision with missing required original event must show unavailable rather than invent a valid origin.

The current source producers already establish the immutable root. Their proof can support initial root projection directly from sealed revision identity; optional exact event joins add independent bounded validation without adding current lifecycle authority. No financial snapshot, rate, head publication or approval-body rewrite belongs here.

## After Create: retain the captured physical origin

`bpay_next_run_work` has non-null captured_revision_id and composite FK `(work_id,captured_revision_id)` (1324:505–519). Existing `private.bpay_next_simple_revision_comparison_page_v1(uuid,uuid,integer)` in 2359:206–249 bounds native run-work rows before lookup; its exact captured revision is correct. It deliberately exposes `work.original_timesheet_id` as family identity and a separately informational current revision. Current 2026:779–783 forwards those six fields; **that original Timesheet field alone is insufficient for WB17’s post-Create opener.**

Smallest supporting addition is in the existing public preparation REVISION_COMPARISON projection: PK-join the returned captured revision by both work_id/captured_revision_id and add its physical_timesheet_id/version plus the same finite Source origin suffix. Label any joined current revision physical ID/version separately as newer informational evidence; only expose when the actual current revision differs and exists. Keep the private comparison body, its run/worker readiness guard, native Work keyset and current-vs-captured fact unchanged. Keep the frozen 0250 service and its owners unchanged. Mirror the exact typed additions in `broker/src/bpay-next-preparation.js` and Office decoder. If capture is still pending after accepted Create, do not use current Work physical identity as a substitute: retain the accepted Rule-B expected revision/choice identity and display genuine pending until the exact captured native run-work is available.

## Native navigation and later joins

Current main caller at `js/main.js:7165–7169` opens `openTimesheet({timesheet_id: issue.physical_timesheet_id})` after actor/NEXT checks. `openTimesheet` at 367499+ accepts an exact physical ID; it must not receive family original when the reader has exact revision identity. Existing `createStandardWindowAdapter` (`bpay-next-ui.js:31–70`) retains native stack/geometry/Close/Escape and exposes frame alive separately from active. Banking parent can be stashed/disconnected while alive; do not invalidate it on `host.isConnected` alone. WB16 already corrected that distinction.

Required WB17 work: route exact current/captured physical identity through Candidate group/detail and issue actions; stack actual Timesheet child, retain existing selection store, scope, continuous page cache/scroll anchor and unticks; on child return preserve these exactly. Capture actor, parent managed frame and immutable opener identity, and guard late replies. A later financial_view_revision comparison may flag “This Candidate has newer information — Refresh”; no automatic reload/recalculation or hidden replay-intent change. A Candidate aggregate with multiple Work groups cannot guess a single physical Timesheet; choose from bounded native groups.

`openWeeklySourceApprovedHoursModal` at main:178886 is an action-capable Manage Approved Hours dialog requiring a current view model, not an exact retained read-only Source evidence opener. Do not fabricate that VM from new root fields or invoke its mutation controls as a navigation shortcut. Either reuse existing exact-physical Timesheet’s factual Source evidence view with explicit root/week context, or wire the actual existing Weekly Source read view using the supplied native identity. If no compatible exact-evidence view is currently callable, record that screen join as required; do not invent a business RPC or replace current Source owners.

Later separate joins: WB19 explicit newer-information/Refresh and WB21 selection/accepted-run parent preservation; future Draft-specific screen ownership remains its named card. These are not permission to modify accepted Rule-B capture, money owners, F26 resume, parked F25 objects, producers, or frozen run facts. App acceptance journeys and actual child-close timing remain NOT_RUN by this investigation.

## Saved-source raw SHA-256

| Source | Raw SHA-256 |
|---|---|
| BUILD-BE/supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql | `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a` |
| BUILD-BE/supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql | `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37` |
| BUILD-BE/supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| BUILD-BE/supabase/migrations/03102026_1324_banking_pay_next_core.sql | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| BUILD-BE/supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql | `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d` |
| BUILD-BE/supabase/repeatable/03102026_2359_banking_pay_next_simple_review_confirm_v1.sql | `e69026ef2315b9dd4d6905b8a63fdde07d17152b56e1b7d99674e10e05ade561` |
| BUILD-BE/supabase/repeatable/04102026_0250_banking_pay_next_frozen_detail_service_v1.sql | `ca6e50fcb6324e9e3a39e840a664ddfa53a7a7d96d9d0fb9859d65743614e091` |
| BUILD-BE/broker/src/bpay-next-workbench.js | `bc063d8205be961e18703dfa6d85dbc431a176f4bde7b37bcb484b4d49992232` |
| BUILD-BE/broker/src/bpay-next-preparation.js | `da8b67652178bbbaa0e0b418d6ef97800e916416c1521e989c631effd02d8fb9` |
| BUILD-BE/broker/src/bpay-next-frozen-rpc.js | `6ddbc1ecb64873714e9a9c9a05bc4fcb2dae5d7fca95236a4f75976594a4e83c` |
| BUILD-BE/broker/src/bpay-next-frozen-read.js | `3c7d75b485db6824a38000d580eb96c37864c01a61fdaf114d5551637e10c3b2` |
| BUILD-FE/js/bpay-next-workbench.js | `2820c2682ea8cfd75668639b7795a894511528ef31d0e3ac926c60459e02c3e4` |
| BUILD-FE/js/bpay-next-office.js | `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0` |
| BUILD-FE/js/main.js | `2b085d88fabc4de2094a54ade45e73ebe2876910f38eb49d5145ada91512b16f` |
| BUILD-FE/js/bpay-next-ui.js | `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5` |
