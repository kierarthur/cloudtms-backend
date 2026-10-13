# PS-14 remaining reservation scope investigation — 10 October 2026

Read-only source/design investigation only. No product edits, SQL/runtime/tests/app/fixtures/hosted/network execution. No installation or in-app acceptance claim.

## Finding and required scope

PS-14 is real missing reader/caller/display work. Current STATUS exposes the accepted projection and bounded transfer rows, but has **no remaining confirmed reservation contract**. The current four 2025 destination execution/posting definitions correctly bind and conserve original legs; they are not a remaining-reservation reader. Neither current 0580 STATUS nor current 2025 implements the card's cited 'missing transfer member per run_line' shortcut. Therefore do not fabricate a replacement hunk or rewrite correct posting authority: add the genuine original-obligation read classification, then explicitly consume it in STATUS/Office.

The minimum complete scope is a source-owned **STABLE, write-free private reservation classifier** in a scoped 2025 successor (necessary read support), a scoped successor of `private.bpay_next_worker_status_page_v1` in 0580 to include those server facts, exact typed decoder additions in BE preparation.js and FE office.js, and a finite future joined acceptance checklist. Existing public preparation wrapper/CHANNEL_STATUS delegates can reuse the same helper identity unchanged. Preserve the four execution/guard/posting functions, their exact original semantics, and installed schema unless an independently identified defect truly requires a hunk. No new ledger/counter, bank/provider owner, settlement/cancellation writer, public API, Timesheet reprice or early Summary layout.

## Binding card/rows

Normal plan 06:1616–1634, BUILD-BE checklist:846–870 and file08 sequence34 require dependencies **PS-03 / F-08**. The four deliverables are:

| Row | Actual required object / scope |
| --- | --- |
| PS-14-D01 | Scoped `04102026_2025_banking_pay_next_destination_execution_v1.sql` successor; exact `private.bpay_next_destination_execution_leg_v1(uuid)`, destination_posting_guard_v1(), destination_posted_leg_guard_v1(), complete_destination_leg_v1(uuid) reviewed/reused. Add minimal private read support and existing F08/STATUS stored-fact integration; preserve these correct money owners. |
| PS-14-D04 | Future joined checklist: partial own/external/Case-only materialisation, execution/cancellation; transfer across many Clients counted once; parent Draft never doubles cash. All actual app/Worker/queue/minute-tick work remains NOT_RUN. |
| PS-14-D05 | `broker/src/bpay-next-preparation.js` STATUS remaining confirmed reservation decoder; CHANNEL_STATUS shares it. |
| PS-14-D06 | FE `js/bpay-next-office.js` display the server-computed reservation separately from actual transfers/payments; no browser subtraction or allocation across Clients. |

D02/D03 are shared installed 1324/2025 evidence, not permission to reinstall/overwrite those originals or to infer present connected acceptance. Card does not approve Summary policy/layout/filter/export APIs.

## Exact current final objects

Final registry currently points these four D01 signatures to the original 2025 physical file. `private.bpay_next_worker_status_page_v1(uuid,uuid,integer,integer)` is current0580. `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` is final2018. F08 channel readers are final0236; its channel_status_page calls worker_status_page after exact run/channel/worker membership checks. The public2018 STATUS/CHANNEL_STATUS branches already delegate to these private readers, so keeping identities avoids needless public-body duplication.

Other preserved winners: simple build is PS01 final0533; case transfer begin/build0410; destination groups/net-state1827; destination cancel2102; internal zero0490; bank settlement intake/post2018. PS03's typed member projection and captured-origin links remain the work-identification source, not an amount allocator.

## Original obligation authority already retained

| Actual fact / key | Relevant real fields and role |
| --- | --- |
| pay_run + run_worker | confirmed_at_utc / run status / selection state; exact run_id/Candidate identity; worker preparation/selection/current NET revisions and pending state. Confirmation is required before any reservation can be called confirmed. |
| net_projection exact id | Immutable accepted cash_amount, gross/treatment/accepted recovery/addition fields, request_command_id and projection_no. Original transfers retain this id; current worker projection pointer is not a substitute after materialisation. |
| completed net_destination_state | Exact projection_id, state_id, run_worker_id, own_amount, external_amount, external_leg_count, cash_amount, completed_at_utc; CHECK own+external=cash. Gives genuine disjoint original cash vector without splitting payroll NET by WORK/Client. |
| net_destination_amount | Exact destination_id/state_id, native amount and first_origin_command_id; unique(state_id,bank_details_hash). This records original expected external obligations before transfer rows exist. Read/output numeric IDs only; no bank details/hash disclosure. |
| destination_group | Exact anchor transfer/worker/Candidate/projection/draft_state/net_state/revision identity; stage, expected/created/sealed leg counts and sealed_cash_total. COMPLETE requires all materialisation/seal conservation. |
| destination_group_leg | Exact original transfer id/anchor/destination and OWN/ONEOFF kind. OWN has NULL destination; ONEOFF points to one retained original destination amount. Its identity is a true obligation link, not a guessed member absence. |
| transfer | Immutable original cash_amount, projection_id and worker/Candidate; status/execution_kind; original_transfer_id/return_cash_id distinguish reissue. Unique worker/transfer_no supports current bounded STATUS page. |
| destination_posted_leg / posting | Original_transfer_id primary key, anchor/job/posting_no immutable receipt and stored completion count. Original cash is posted once; reissue is deliberately skipped by complete_destination_leg. |
| hold + run_line | Actual source/target reserved tuple and ACTIVE/REALISED/RELEASED status; exact line→run_work/captured revision identity. No conversion of these source/gross amounts into arbitrary shares of Candidate NET. |
| case_hold + allocation/instruction | Genuine positive Case reservation purpose/result/channel/week, actual worker/Candidate, exact DRAFT/NET allocation state/result and capacity use. Case-only may have no WORK rows and still have genuine financial obligations. |
| outcome / internal receipt / return / cancellation | Immutable accepted whole-transfer occurrence/request plus current posting progress; internal zero is separate actual receipt; cancellation releases reservation, not a return; return/reissue retains prior paid origin separately. |

Core1324:561–609 holds frozen run_line source/target identity + work hold status; 612–627 genuine Case holds; 629–685 accepted projection/original-transfer shape. Case engine0300 supplies typed Case instruction/result/purpose and allocation state. 1827 migration40–76 contains immutable completed NET destination vector/amount/credit;80–134 group/leg/member identities. 2025 lifecycle8–28 original posted-leg receipts and31–42 cancellation checkpoints. No new numeric counter is required to retain these facts.

## Finite classification strategy

Build the original obligations from the exact accepted **original** projection and its original vector. For ordinary/non-group work this is one Candidate obligation. For a genuine destination group this is the exact OWN obligation and retained per-destination external obligations. Keep internal zero as a real zero-cash obligation with an explicit state/count; zero is not NULL or 'no evidence'. Do not multiply accepted projection cash by number of captured Timesheets, Clients or transfer members.

An existing completed NET destination vector is the authoritative source for an unmaterialised external destination. Left-join the exact destination_group_leg by destination_id and exact anchor; one destination without a materialised leg is genuinely unmaterialised only after its original vector identity/completion and hold/allocation scope have been verified. Absence of WORK transfer_member, a line page or a Case membership row cannot establish an unpaid cash obligation. Before any transfer exists, only confirmed/qualified current accepted projection may be used; during a replacement the pending flag remains truthful and old accepted projection is labelled separately. Once an original transfer/group exists, resolve via its exact retained projection id even if current worker projection differs or is later retired.

### Important bounded existing invariant

Current2025 execution_leg refuses execution unless group.stage COMPLETE, created/sealed counts equal expected, sealed total equals original projection.cash_amount, completed NET vector matches worker/projection, and original anchor build command/job is COMPLETE/DONE/MEMBERS_READY. Therefore **partial legal group materialisation cannot execute**. Its known remaining reservation is the original accepted obligation vector, not parent cash plus already-created legs or a missing-member sum. Single Case/simple builders likewise remain BUILDING until native full member/conservation checks complete; member pages are not bank execution facts.

After complete materialisation, classify each exact original leg once using its own accepted cash amount, original outcome/internal receipt, posting/cancel proof and active-hold scope. A correctly recorded SETTLED outcome is paid even when its effect/member posting continuation is pending; active holds alone must not recount it as reservation. Current2018 settlement intake immediately writes transfer.status SETTLED (2285) and retains its immutable outcome before posting. RETURNED remains a former paid transfer plus its separate returned-cash obligation; it does not recreate a parent Draft reservation. Reissue original_transfer_id is excluded from parent original-reservation calculation and remains the separate returned-cash lifecycle.

Cancellation audit is retained but never labelled RETURNED unless a genuine RETURNED outcome exists. During partial cancellation, derive only surviving original obligations from actual release/cancel/leg facts; do not subtract partially released source/gross holds from PAYE NET to manufacture a remaining bank amount. If a transient combination cannot establish an exact original cash obligation, expose a finite pending/unavailable classification and NULL amount, then recover on the next bounded factual read. This is not permission to leave ordinary/complete-group/Case-only positive cases unsupported. Known valid0 must remain decimal0 with explicit internal-zero/pending state rather than NULL.

SUBMITTED/UNKNOWN or contradictory receipt/status evidence cannot be affirmed as confirmed **unexecuted** cash. Preserve a closed unresolved/pending state and exact original id; don't silently classify uncertain provider execution as cash available. REFUSED has not paid but its remaining-reservation authority still depends on actual active reservation/cancellation facts, not the word REFUSED alone. No new provider owner is warranted.

## Reader output and boundedness

Extend the existing STATUS contract with one finite server-computed reservation object or equivalent typed fields (final names are builder choices): actual original projection id, classification/state, original native obligation cash, genuinely remaining native cash (nullable only for a truthful unavailable/pending classification), and remaining original obligation count including internal zero. Display each actual transfer's native cash once. Keep the existing accepted projection header explicitly informational; never sum that header into its child legs.

Any server aggregation must operate on the exact current original obligation/leg set under one SQL statement snapshot and native retained amounts, with exact identity and receipt predicates. It must not reconstruct a transfer total from all member pages, scan historic WORK revisions, price live finance, count return/reissue as a second original, or add a duplicate retained numeric counter. If choosing paged per-obligation facts instead, do not publish a whole-header remaining total from a truncated page. Complete meaning and byte/time bounds must be explicit; SQL returns a truthful refusal for missing/contradictory basis instead of optimistic0. Existing bounded STATUS transfer keyset and requested limit remain intact.

The classifier must be STABLE/write-free, take only the existing exact run/worker scope and use finite indexed identity relations. **Do not call destination_execution_leg_v1 from this reader**: it rejects incomplete groups and performs current external bank-detail FOR SHARE safety validation. History/reservation source facts must not acquire those business locks, inspect/return bank details or re-admit money commands. No current finance settings or WORK.current_revision_id selector is needed.

## Caller/display changes actually required

BE preparation.js:90–93 currently defines the exact STATUS_HEADER without reservation fields;312–319 verifies accepted projection/null/NET semantics;74 transfer rows are already one native cash amount per transfer. STATUS and CHANNEL_STATUS both use this decoder (270–272). Add exact reservation-field types and semantic null/zero/identity checks, preserving accepted four-field command responses, retries/journal and background wake unchanged. No command/route expansion needed.

FE office.js:66–68 duplicates the strict header shape;354–357 validates STATUS/CHANNEL_STATUS and accepted projection;583 renders accepted worker cash;597 renders bounded native transfer rows. Add the same exact fields and a small 'Remaining confirmed reservation' display next to existing worker status, separate from accepted bank cash/transfer rows. Preserve decimal text, current channel scope, actual native transfer IDs, pagination, uncertain request journal and existing date handling. No browser/member arithmetic, Client cash distribution, report totals, Summary layout or new money action.

Update only the existing finite typed-projection source-gate literal for worker_status_page when its exact SQL header projection changes; retain explicit fields/decimal TEXT. Existing targets in scripts/bpay-next-source-gates.mjs include worker_status_page and public preparation page. Reuse existing tests/bpay-next-preparation.test.js, tests/bpay-next-preparation-rpc.test.js and FE tests/unit/bpay-next-office.test.cjs for normal source/caller checks once authored; no fixture/scenario programme. These were inspected by source names only, not executed here.

## Future joined receipt and retained boundaries

D04 must state actual NOT_RUN clauses for: confirmed pre-materialisation; partial own/external materialisation; Case-only0WORK; internal-zero receipt; one own leg paid while external remains (and reverse); cancellation/release progress and retained audit; returned original + cash-only reissue separate; pending NET replacement; GBP300 across two Clients retains one whole native transfer amount and original members identify both without assigning Client NET; parent Draft and legs never both included as payment/reservation cash. Include unknown/missing evidence refusal, exact foreign scope denial and bounded continuation. Reuse existing joined acceptance where it covers clauses, not duplicate scenarios.

No independent mutation of 2025 posting guards/writers, settlement intake, PS03 member origins, Rule-B accepted authority, 0718 frozen rows or future Summary/Contract move owner is justified. Source current originals remain installed baseline/context; successor source, release/readback and real app acceptance are separate boxes.

## Fresh current raw source SHA256

| Repository-relative path | SHA256 |
| --- | --- |
| BE `supabase/repeatable/04102026_2025_banking_pay_next_destination_execution_v1.sql` | `5ed7e53c884b507444bda67c7ce6061a82c6b8690a3461ffa80846aaa98c5d1b` |
| BE `supabase/repeatable/04102026_0580_banking_pay_next_office_status_v1.sql` | `ab95db05d07150d131ba027951878b9833e4e5b5e1cb9f4873cfa55fddc3a1ba` |
| BE `supabase/repeatable/06102026_0236_banking_pay_next_channel_draft_readers_v1.sql` | `a90b3b2fe01e079e7177bedde583d5e0b66dd037b2344272a6a8fb288d008719` |
| BE `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| BE `supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql` | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| BE `supabase/repeatable/04102026_1827_banking_pay_next_candidate_destination_group_v1.sql` | `33891038c2865684c091f7bd3df4ce188bb42e026129163216e3bc341feb198c` |
| BE `supabase/repeatable/04102026_2102_banking_pay_next_destination_cancel_v1.sql` | `2f9b25165d1cca79ace54ef8a5f794f69b46a1f2e8e3b5a58ca211ba1e62dd6a` |
| BE `supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` | `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c` |
| BE `supabase/repeatable/10102026_0533_banking_pay_next_ps_c01_simple_transfer_bridge_v1.sql` | `a7175c21cac86b372ad59c3a114dce69c9298708154898955848877b020fb314` |
| BE `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| BE `supabase/migrations/04102026_1827_banking_pay_next_candidate_destination_group.sql` | `af9849f6e83784b0064ee58179aabc740d396764c3639ef7132967f394c9512d` |
| BE `supabase/migrations/04102026_2025_banking_pay_next_destination_lifecycle.sql` | `c36df5188713a13e5ee23796466eef4ce2227b9c0405f9958c415de8422e633d` |
| BE `supabase/migrations/04102026_0480_banking_pay_next_internal_zero_execution.sql` | `032fcf4d3456b66c67c49a6399ee6a13207e223cb37b5d5fff0de3439e70f70b` |
| BE `broker/src/bpay-next-preparation.js` | `9271ec0086bffdd9779707bf44407eba0923b1392317eb142e46c8f8e0338749` |
| BE `docs/bpay-next/final-definer-closures.json` | `a0cf5d67e0e46568831bb54550680bcbac083af7b7263862dba9ac9af35d1bfd` |
| FE `js/bpay-next-office.js` | `31752685c20b61df2d9e4cecdf218ed7f26ba730c565d0de6437b2a5a90df356` |
