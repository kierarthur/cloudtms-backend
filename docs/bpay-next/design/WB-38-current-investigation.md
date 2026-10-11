# WB-38 current source investigation

11 October 2026. Read-only investigation for sequence 75. Root alone writes product. No product edits, SQL execution, database calls, tests, fixtures, hosted operations or commits. App journeys remain NOT_RUN.

Read the full approved WB-38 card (`02_WORKBENCH.md:1359–1399`), all eight own/shared checklist rows, R2-F05 (`review/R2.md:112–118`), WB-38 review (`:292–294`), and relevant exact-connections segment ownership. The current mirrored checklist places D06 at `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:2210`; shared-owner rows are context, while the card's actual two many actions and all UI behaviors remain required.

## Binding complete scope

One shared NEXT snooze window must open from native Workbench row/expand, whole-Timesheet group and Timesheet window. Preserve indefinite unticked, blank dated input with London-today minimum, optional note, create/amend wording, active-only amend, discard confirmation, confirmed clear/cancel, visible failures, exact affected-view adoption and no typing-time calls/polling.

All expenses includes the full currently supported expense **and mileage** membership across pages, frozen once with native worker-pay basis and exact physical/revision identities. Confirm once. Submit and show at most 100 native lines per result page; report success/stale/refused individually; retain successes and retry failed lines only. Do not turn this into an all-page browser array, claim-authorisation operation, money estimate or producer rewrite. Distinguish existing EXPENSES_ALL persisted control from deduplicated/exclusive per-line controls.

## Current reused source versus gaps

| Object | Current source and disposition |
| --- | --- |
| Private snooze context/action/at-cut | `supabase/repeatable/10102026_1100_banking_pay_next_snooze_v1.sql`; genuine built native single-scope authority. |
| Current metadata/receipt guards | `supabase/repeatable/11102026_0255_banking_pay_next_due_snooze_guards_v1.sql`; WB-37 successor must survive. |
| Public W2 command | `supabase/repeatable/10102026_2150_banking_pay_next_workbench_retry_v1.sql:132–154`; currently accepts only RETRY_WORK_JOB. Snooze dispatch/readback and MANY actions are absent. |
| W2 Worker transport/handler | `broker/src/bpay-next-workbench.js:777–838`; closed RETRY_WORK_JOB contract only. Reuse this fixed route/auth/transport seam; add only card-needed action schemas. |
| Current public Workbench reader | `supabase/repeatable/11102026_0207_banking_pay_next_candidate_estimate_reader_v1.sql`; current final successor must survive. No published SNOOZE_CHECK branch is present. |
| Workbench row facts/renderer | BE `broker/src/bpay-next-workbench.js` and FE `js/bpay-next-workbench.js`; rows already expose native snooze ID/version/date/scope/stale facts. They do not implement the NEXT snooze window or commands. |
| Mileage scope | Private context `...1100...sql:101–111` expressly admits EXPENSE_LINE for approved component kinds EXPENSE and MILEAGE, and EXPENSES_ALL includes both. This is existing schema/owner support, not a missing mileage financial model. |

The private owner already separates SNOOZE_CHECK/SNOOZE_CONTEXT context validation from explicit SNOOZE_WARNING_ACK_ISSUE and single SNOOZE_SET/SNOOZE_CLEAR (`...1100...sql:174–185`). SET checks native version/pay basis and consumes the exact warning acknowledgement under original controls. These contracts should be exposed through the current W1/W2 seams, not replaced with a new owner or a read which secretly issues an acknowledgement.

Current native single commands retain their exact manual receipt and existing accepted control sequence; charge-only compatibility, frozen prior Drafts, current scope refresh and due wake guards remain their original owners. Preserve the latest WB-37 guards/enrollment support rather than copy older 1100 guard bodies over them.

## Exact existing single contracts and required exposure

`...1100...sql:29–58` closes context input to request/candidate/work/revision/scope/native segment or component or Case identifiers, kind, proposal date/indefinite/note, expected version, existing worker-pay basis, acknowledgement token, phase and Case revision. It rejects foreign scope fields. Current approved Work/revision is a native point validation (`:79–87`); an EXPENSE_LINE is the exact component key in that revision of kind EXPENSE or MILEAGE (`:101–107`). No display amount authorises a snooze.

Context returns exact physical Timesheet, native scope identity and member IDs, existing pay basis, active snooze ID/version, native financial revision, owner epoch, conflict, dominance and proposal/warning context (`:146–157`). Two small context gaps matter for the new dialog: it returns proposed date/note rather than saved active date/note; PRE_OPEN still requires `indefinite = (until_date IS NULL)` (`:54–55`), so a new blank-date/unchecked-indefinite dialog cannot send a truthful valid initial shape. Give PRE_OPEN a read-only saved-control/default presentation shape; retain strict proposal validity for PRE_SAVE and SET. London today comes from existing official-date context (`:132–138`). Typing changes local draft only; Check reads and explicit warning confirmation issues the W2 acknowledgement.

Single CLEAR closes input to request ID, snooze ID, candidate and expected version (`:196–204`), remains available on stale financial facts (`:238–250`) and changes only native control active/version/sequence/audit fields. Single SET validates the exact current native version/pay basis under locks (`:257–265`) and consumes the exact actor/scope/epoch/version/basis/warning acknowledgement (`:276–286`). Native result is only snooze ID, version, active, sequence and indefinite dominance (`:318–319`); the public wrapper needs exact affected-view facts/adoption from existing native readers. At the new public boundary stringify BIGINT version/sequence/epoch fields, currently emitted as JSON numbers, without relaxing their native type or bounds.

Current public W2 `...2150...retry_v1.sql:132–154`, Worker `bpay-next-workbench.js:777–836`, and FE `js/bpay-next-workbench.js:717–790` accept RETRY_WORK_JOB only. Required single exposure is the existing W1 SNOOZE_CHECK branch and fixed W2 ACK/SET/CLEAR dispatch/readback, with action-specific closed input/results and existing Office admin/owner/actor qualification. No separate browser database endpoint or new command route is needed.

## Full expense/mileage native membership

Use the sealed `private.bpay_next_approved_line` membership at the accepted exact Work revision, with `component_kind IN ('EXPENSE','MILEAGE')`. The core schema has unique `(revision_id,line_no)` and `(revision_id,component_key)` (`supabase/migrations/03102026_1324_banking_pay_next_core.sql:188–208`). Existing factual COMPONENTS paging takes Work/revision/after_line_no/limit, validates current approved/sealed revision, orders native line_no and admits at most 100 rows (`...11102026_0113...case_choice_readers_v1.sql:123–128,149–184`; Worker `bpay-next-preparation.js:30,69–70,202`). It includes all supported component kinds; filter expense/mileage natively rather than hardcoding reimbursement names or treating ADDITIONAL work as an expense.

Existing `private.bpay_next_line_pay_basis_v1(uuid)` deliberately binds Work/component/channel/umbrella/tax/quantity/nonzero unit rates/source pay and native rate-detail facts, excluding charge facts (`...1236...segment_authority_final_v1.sql:29–39`). Existing sealed `bpay_next_revision_worker_basis.expense_basis` covers **all** expense/mileage line bases (`:92–96`). Reuse this stored native operand; do not create another selector hash, money model, Source inventory or approval producer.

Visible Workbench GROUP_LINES is a projection of current contributions and can repeat a component across R/H/A portions. It is not a complete approved-reimbursement inventory. A Ready subset, current page, browser gathered array or displayed residual cannot freeze all-expense membership. Reimbursement snooze remains independent of claim authorisation and changes no approved, held or realised money. Frozen Draft choices retain their original at-cut controls (`...1100...sql:453–477`).

## MANY support: smallest original-owner extension

The two card-named W2 actions remain SNOOZE_SET_MANY and SNOOZE_CLEAR_MANY. The narrow parent proposal is a private frozen many request plus native per-line receipt/progress records, using persisted child request UUIDs to invoke **the existing single ACK/SET/CLEAR owner**. This avoids new many-shaped mutations to original snooze/event/ack tables. The support records are specific to these two actions and their frozen expense membership, not a generic operation protocol.

Freeze one exact actor/owner epoch/Candidate/Work/revision/physical Timesheet identity, existing expense basis, proposed date/indefinite/note, existing warning context and accepted original control cut. The sealed revision defines membership across all later pages without a full-member JSON array. Persist the root and each child request ID/body before attempting a child. Capture/reconstruct exact original active native controls/version at that cut, including aggregate and per-line controls. Do not substitute new revision membership or current control versions silently on resumed pages.

Each page processes at most 100 native lines and retains a closed per-line outcome: exact frozen native line identity, success/stale/refused status, named reason, and child request/result linkage. An unknown child outcome reads its original receipt before resending identical bytes. Failed-only retry is authorised by retained original failed line records; a caller-supplied new line or previously successful line cannot join a retry. A repeated page returns its original retained outcomes. Success is durable and never replayed as another amendment. Subsequent native pages and failure-result pages use indexed native cursors; the UI retains only the root, current bounded page/pending bytes and bounded failure view.

`private.bpay_next_snooze_request` is genuinely reusable for **single child** replay: schema `supabase/migrations/10102026_1100_banking_pay_next_snooze.sql:79–84`, owner exact replay `...1100...sql:187–195,219–224,321–323`. Its action constraint permits only ACK/SET/CLEAR and its current result/guards describe one snooze. It is not already a many root/progress store. `bpay_next_workbench_command_receipt` is not a bulk snooze substitute: `...2150...retry_receipts.sql:15–29` requires Work and job IDs and is immutable. Never fabricate a job or broaden retry-job semantics.

### Lock and command ordering

Original single lock order is Candidate worker-control row, exact Work row, then module/agency intake (`...1100...sql:208–216`), followed by acknowledgement/native snooze row locks. MANY must retain this order. Candidate/Work are fixed across a root; native lines are processed in deterministic line order. Native child request UUIDs and bodies must be retained across any page exception/retry. `receive_command_v1` already accepts a bounded command-kind string and exact request/epoch replay (`03102026_1404...command_intake_v1.sql:27–58`); it needs no new kind registry or money-owner rewrite.

A MANY parent owns its accepted command/receipt and completes with zero financial members/jobs only after its bounded page outcomes are durable. Extend the **current** enrollment owner `11102026_0255...due_enrollment_v1.sql:32–47` with a separate narrowly qualified MANY parent receipt branch: exact next sequence, current epoch, COMPLETE/sealed, expected/enrolled member count zero, no command members/jobs, exact actor/action/request/root/page receipt. Keep original single/manual and DUE branches unchanged. Failed/refused line outcomes are complete page results, not financial jobs.

If all native controls are changed through original single child commands, current `11102026_0255...due_snooze_guards_v1.sql` remains unchanged. Directly stamping MANY on snooze/event/ack rows would instead require changing the hard SET/CLEAR checks at `:76–92,:131`, ACK SET checks at `:144,:221–230`, singular receipt/event result checks at `:196–205,:240–260`, and receipt action constraint. That larger alternative is unnecessary for the original-child design.

### Aggregate overlap and warning consent

Existing EXPENSES_ALL is one persisted control over the revision expense basis. Existing EXPENSE_LINE controls remain separate native scope identities. Current context only implements whole-Timesheet/segment exclusion (`...1100...sql:116–126`); it **does not enforce aggregate/per-line exclusion**. Its indefinite dominance check sees valid whole and all-expense controls (`:140–145`), and at-cut read deduplicates applicable controls (`:465–474`). Do not describe that as built many-action exclusivity.

Freeze any active aggregate control in the original scope. For SET, update that existing aggregate exactly once through its original child SET and reuse that shared result for every covered line; do not create duplicate per-line controls for its covered members. If per-line controls already coexist, preserve truthful coverage/result facts and avoid introducing another aggregate merely to bypass their versions. Where there is no aggregate, native line SETs use exact frozen per-line versions/bases. For CLEAR, clear the frozen aggregate once, then clear all frozen active per-line expense/mileage controls; otherwise a second control could keep a supposedly unsnoozed line excluded. WHOLE_TIMESHEET controls are outside this operation and must remain unchanged; report remaining dominance rather than claiming that clearing expenses readmits Work.

One shared mutation failure must be reflected on every line it covers, including later pages; a successful shared result must not cause another aggregate amendment. Original acknowledgement tokens bind exact scope/version (`...1100...sql:279–284`), so an EXPENSES_ALL token cannot be passed to EXPENSE_LINE SET. Explicit MANY consent may authorise bounded original child ACK issuance in W2, with each exact child warning/context retained. Re-check expiry/current date/configuration/control/pay facts; a changed warning cannot be silently replaced by a resumed page. W1 Check must remain read-only.

### Original-cut point lookup caveat

The existing index set is active scope `(candidate_id,scope_kind,scope_identity)`, active Work `(work_id,scope_kind,segment_key,component_key)`, scope history `(work_id,scope_kind,scope_identity,id)` and event cut `(snooze_id,control_sequence DESC)` (`1100` migration `:45–60`). These do not establish a single bounded latest-event seek across cleared/recreated snooze IDs for one Work/component scope at an older cut. Current at-cut helper scans candidate scope history and performs a latest event lookup per snooze (`...1100...sql:459–474`). A many implementation cannot assume today's active row is the original active row after later clear/recreate.

Required support is either exact original control IDs/versions retained through finite freeze pages before mutation, or a narrowly indexed original native scope/control-sequence point query. Preserve original event payload/history and command cut; do not create a digest, inventory rebuild or generic proof scheme. A new index is justified only for that exact point query if the chosen implementation needs it.

## FE dialog, journal and Timesheet connection

The existing Workbench retry journal is a useful transport pattern, not an existing snooze bulk journal. It persists exact request bytes before network, namespaces by agency/actor/owner, uses readback before retry after uncertainty, repeats original bytes only when absent, and refuses actor changes (`FE js/bpay-next-workbench.js:741–789`). Its key is a job ID and closed version-1 body is RETRY_WORK_JOB only. Add closed snooze-specific actions/receipt/result shapes with a bounded root/page journal; preserve old pending retry-job compatibility. Do not reuse Office preparation journals to hold all expense IDs or clear pending work merely because the dialog was reopened.

Shared Timesheet wiring still reaches legacy endpoints: `FE js/main.js:362747–362803` opens the old payment-snooze modal and POSTs `/api/banking/pay/snooze/validate`; `:363651–363668` always invokes it and refreshes Overview. Current Workbench Timesheet opening passes exact physical Timesheet/version/Candidate but does not carry the native Work/revision pair (`:7166–7177`). The card requires a fresh NEXT-gated branch at this shared entry, standard frame and the new shared NEXT window; keep unrelated legacy handling behind the existing owner boundary.

For an exact Workbench-origin open, carry its existing native Work/revision pair. For a standalone physical Timesheet open, resolve the exact native Work/current revision server-side through the same context seam, with exact candidate/physical version checks and an ambiguity refusal. The core already has unique original_timesheet_id (`03102026_1324...core.sql:115–129`); current revision has its exact physical ID/version (`:142–146`). Do not walk all Candidate Work pages or choose an arbitrary historical physical row. A different physical revision needs the actual qualified original/current mapping; original uniqueness alone does not prove that arbitrary revision-ID lookup is indexed.

The reusable owner client `FE js/bpay-next-owner.js:20–62` performs one bounded read per qualified window scope and invalidates on current module/actor changes. Use it for the Timesheet NEXT branch. Reuse standard dialog/confirmation adapters, native segment/single-expense facts from WB-16 and whole-Timesheet group facts from WB-15; no compatibility financial formulas are required.

After accepted changes, adopt only affected Candidate/Work/Timesheet/current native detail views using existing scope refresh/read points. Original snooze update trigger (`10102026_1429...current_continuation_v1.sql:7–29`) already updates due metadata and queues exact scope refresh. Return/display honest Processing where that original refresh has not completed. The exact card toasts use DD/MM/YYYY and indefinite management destination; no automatic polling, whole-list rebuild, financial Draft recomputation or provider action belongs here. Keep WB-39 indefinite-row suppression at its later owner turn.

## Remaining implementation inventory

1. Small PRE_OPEN context presentation support and closed W1 Check exposure; retain current WB-27 reader branches and WB-37 successors.
2. Fixed W2 single exposure/readback and two card MANY actions with native frozen root/line outcome support, original single children, bounded receipt/result paging and failure-only retry.
3. Exact zero-job MANY parent enrollment qualification; no change to accepted financial-job or money formulas.
4. Shared NEXT dialog and native Workbench row/expand/group/single-expense/all-expense controls, plus fresh owner-gated Timesheet entry and scoped adoption.

This is source investigation/design support only. No database compilation, source execution, test, app journey or installed behavior is asserted. App journeys remain NOT_RUN.
