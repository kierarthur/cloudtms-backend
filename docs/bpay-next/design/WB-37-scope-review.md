# WB-37 narrow scope review

11 October 2026. Independent source-only review. Root alone writes product. No SQL/database, tests, fixtures, browser, hosted operation, checklist change or commit was performed. This is a supporting review, not a new design-first requirement or installation/app verdict.

## Recommendation

**Proceed with the two card-named service pages and their existing minute recovery lanes, using the native snooze/event/accepted-command owners. The qualified zero-job enrolment supplement is genuinely required.** Preserve original money owners, immutable Create cuts, native contribution refresh, manual snooze authority and queue consumption. No new command kind, table, system administrator, calculator or general sweep/proof framework is needed.

Read `design/WB-37-current-investigation.md`, the complete approved `02_WORKBENCH.md:1332–1357`, all nine checklist entries and shared-owner qualifications (`IMPLEMENTATION_CHECKLIST.md:2167–2190`), R2-F01 and R2 WB-37 (`review/R2.md:82–94/288–290`), WB-36 current closure, and the actual current native definitions below. The Postgres review skill was used only for source-level bounded discovery and lock-order review.

## Smallest native DUE transition

Use the existing active dated `(until_date,id)` partial index and the strict London date condition `until_date < current London calendar date`; today remains snoozed. Capture at most 200 native identities before joins/deduplication and revalidate the same snooze version/active/date/scope after its controls are held. Indefinite, amended, already inactive and moved rows cannot be cleared using the old discovery tuple.

Prehold distinct Candidate controls in identity order, then exact Works, Cases and native snoozes in the established order. Obtain the existing accepted SNOOZE_CLEAR sequence after Candidate/scope preholds and before projection/summary writes. Do not take the global command clock before later Candidate locks: the actual manual owner already acquires Candidate/Work/component controls before `receive_command` (`1100:208–215`). One successful page may share one existing accepted CLEAR command; mint no command for an empty/wholly skipped page. Failure handling must not leave a RECEIVED command or falsely COMPLETE command with no qualifying native result.

Conserve every native identity, scope, revision, basis, due/note and economic field. Change only inactive/version+1/later control sequence/update timestamp/last native command ID plus `cleared_reason='DUE'`; retain the original nonnull administrator provenance on the snooze. Append the existing WOKEN event with `actor_id=NULL`, reason DUE, exact before/after and that accepted command/sequence. The nullable event actor already exists; do not fabricate a manual request receipt or borrow an administrator.

The existing `private.bpay_next_workbench_snooze_due_trigger_v1()` already updates the version-bound expiry mirror and either queues exact Work ORIGIN/Processing or refreshes the exact Case (`1429:7–29`). Reuse it once. A second explicit full refresh from the new snooze page is unnecessary. Work ORIGIN scheduling removes current FUTURE_WEEK contributions into Processing immediately (`1429:268–278`; current Processing owner), so successful scopes do not remain indefinitely at the discovery head.

## Necessary native guard changes

Current metadata guard rejects a service actor, maps all inactive events to CLEARED and requires exact actor/updater equality (`1100:343–394`). Current deferred receipt guard requires a manual request row/nonnull actor (`1100:479–565`). Both need one narrowly qualified DUE branch, while every existing manual branch stays intact.

The DUE branch must require service-role/NEXT/current epoch, existing CLEAR kind, old active dated-and-due state, exact conserved business tuple, version+1, later accepted sequence, null WOKEN actor and exact native event/command binding. Deferred completion requires COMPLETE, expected/enrolled member counts both zero, nonnull seal, no command members/jobs and the exact bounded WOKEN/DUE result set. Do not accept one arbitrary WOKEN event as permission for unbounded page effects; qualify every affected native row/event.

Add only the card's nullable `cleared_reason` to the existing snooze relation, with active state restricted to null and DUE admitted only by the qualified transition. Historical events must remain immutable. Normalize an absent old nullable key compatibly when comparing prior event `after_state` with new `before_state` (`1100:386–392`); do not rewrite old events or strip unrelated tuple fields. Validate the reason independently if excluding it from the conserved-business comparison.

Page-result/enrolment lookup by command ID needs indexed native event access. The existing event indexes are only the PK, `(snooze_id,version)` uniqueness and `(snooze_id,control_sequence DESC)` (`1100` migration 51–60). If the chosen page proof enumerates WOKEN rows by `request_id`, add one narrow request-ID index on this existing event relation and enforce the page bound before aggregate validation. Otherwise that proof scans unbounded audit history despite a bounded page. This is receipt access, not a new discovery table or duplicate due index.

## Zero-job enrolment is a concrete required fix

Manual `bpay_next_snooze_action_v1` already sets its command COMPLETE with zero expected members (`1100:321–323`). Current `bpay_next_enroll_one_v1` selects only `last_enrolled_sequence+1` (`1832:1313–1316`), handles COMPLETE only inside PREPARE, and then returns NULL for a non-SEALED snooze command without advancing (`1832:1438–1440`). One genuine completed manual snooze can therefore block later ordered commands; new due commands would reproduce it. This is confirmed source control flow, not a hypothetical new feature.

Extend only that current enroller with an exact current-epoch SNOOZE_WARNING_ACK_ISSUE/SNOOZE_SET/SNOOZE_CLEAR COMPLETE branch. Require original matching manual receipt or the strictly qualified WOKEN/DUE result set, exact zero counts, seal and no members/jobs. Advance one exact next sequence and return no job. Do not require the original manual administrator to remain active now, and do not turn this into a generic arbitrary-COMPLETE skip. Preserve PREPARE's special completion/expiry checks and every financial branch.

The existing public enrolment service already reports advancement without a job (`0560:7–35`), and its runtime result schema explicitly admits `progressed=true` with null job fields (`bpay-next-runtime-rpc.js:219–229`). Neither needs a new job kind, producer or response shape for this fix.

## Rollover and fairness

The rollover page consumes the maintained indexed FUTURE_WEEK `eligible_from_date <= London date`, not a guessed Sunday/pay-date rule. Capture no more than 200 item identities first; deduplicate exact Work/Case scopes within that page. Use the card's current Work wrapper `private.bpay_next_workbench_refresh_work_v1` (`1438:318–350`) and preserve already supported Case due promotion through the current Case refresh (`1946:472–505`). Removing the old combined WORKBENCH_DUE call without keeping Case coverage is a regression. Projection owners replace contributions; the new page does not directly alter financial data or contribution deltas.

Return honest visited/progress/completion and original native seek tuples. Advance over visited busy/refused rows, keep them retryable, and isolate expected scope failures so one Candidate cannot repeatedly prevent another's turn. Retain higher-priority Processing, frozen IN_PREPARATION, resolution, PAUSED, pay hold and query classifications.

**Fairness condition to settle before implementation:** an in-request `(date,id)` seek with finite N pages, reset to null every minute, can still starve rows behind more than N×page-size permanently busy/refused rows. The current discovery relations contain no retained sweep cursor. For that case, retain only the two native seek tuples using an existing service-owned control/singleton seam, or name an equivalent genuine durable advancement mechanism. No new table/inventory/framework is necessary. Any existing singleton cursor update must respect Candidate→scope→projection lock order; locking a global projection row first and then waiting for Candidates would introduce the reverse order. Wrap after a genuine end so earlier/newly inserted due rows are revisited. A volatile Worker variable is not missed-tick/restart recovery evidence.

The Worker adds fixed service adapters for just the two named page RPCs and independently caught SNOOZE_DUE/WEEK_ROLLOVER lanes. Keep finite N, existing deadlines/byte caps/closed schemas/unknown-outcome classification, IDs only, NEXT gate, ENROLL/financial lanes and existing Workbench wake consumer. Replace the duplicate combined due dispatch only after both native categories remain covered. An uncertain mutation stops that lane's current walk; later native indexed recovery supplies retry rather than assumed failure or inline financial work.

## Exact change boundary

| Existing/new object | Minimal reason |
| --- | --- |
| Existing native snooze relation | Nullable card-owned cleared_reason and exact DUE constraint. |
| Existing native event relation | Existing WOKEN/null actor retained; narrow request-ID index if required by bounded page/enrolment receipt lookup. |
| Current snooze metadata and deferred receipt guards | Truthful service DUE branch and old nullable-key compatibility. |
| Current `private.bpay_next_enroll_one_v1()` | One qualified zero-job completed snooze sequence advancement. |
| Two card-named public page functions | Bounded native due and rollover discovery/refresh. |
| Existing service control/singleton seam, only if needed | Two native fair-seek tuples; no new table. |
| Runtime RPC and background modules | Fixed adapters and independently capped minute lanes. |
| Final-definer registry/closure, ACL/schema reload and existing source gates | Register actual new/final owners and preserve service-only exposure; no broad suppressions. |

The original at-cut helper (`1100:455–475`), accepted Work skip (`1832:64–109`), current snooze action/context, command-kind intake, financial job/kernel/producer owners and Work/Case projection formulas need no redesign. The snooze trigger and refresh owners can remain their existing definitions. Public enrolment service/response and browser/Office controls need no change.

**Scope approval is conditional on exact native DUE/receipt guards, qualified zero-job advancement and genuine bounded fairness.** No money rewrite, new command/table/job, parked resolver, browser control or additional proof framework is indicated. Compilation, deployment, contention, queue timing and all card app journeys remain separate and NOT_RUN by this reviewer.

## Fairness decision and concrete existing row

Root has approved retaining only the two native seek tuples in an existing service-owned singleton where no current advancement seam exists. The concrete smallest fit is **`private.bpay_next_workbench_revision WHERE id=1`**, created by the current 1252 migration (6–15), already owner-only/RLS-protected (286–291) and already extended by the current 1429 migration. Add four nullable native fields, for example `snooze_due_after_date`, `snooze_due_after_id`, `week_rollover_after_date`, `week_rollover_after_id`, with each date/ID pair both null or both nonnull. Do not add foreign keys: a successful visited row can leave the discovery set or be replaced. These are advancement hints, not financial/context proof operands.

Read the existing tuple without locking, discover/prehold native Candidate/Work/Case scopes, perform their qualified work, and only then update that tuple with an exact comparison to the tuple initially read. A concurrent page cannot overwrite a different cursor with its older value. Cursor-only updates do not need to increment `workbench_revision`, adoption counters, financial revision or user-visible metadata. The current reader consumes explicitly named context fields rather than echoing this row's JSON, so no public response change is indicated. At a genuine end, wrap the retained hint so earlier/new due rows and skipped failures get another turn.

Do **not** put these mutable hints on `bpay_next_module_control`. The native service/command boundary takes FOR SHARE there. A later UPDATE would require upgrading that shared lock while another module-share holder can be waiting on a Candidate already held by the due page. Likewise do not lock `workbench_revision` first and subsequently acquire Candidate/scope locks. The existing Workbench singleton is suitable when updated at the established projection end of the lock order, not as a new global admission lock. Reusing SEED/ORIGIN/METADATA task cursors would mix an unrelated task's generation/phase/lease ownership; no such reuse is needed.

## Exact points still to verify in the implementation

1. The new DUE branch must be selected only for the snooze/WOKEN tables and actual DUE tuple. It must not bypass manual ACK/request actor or receipt validation merely because a request uses service_role. The before-state must prove old active, inclusive-date expiry and conserved tuple, with independently checked new reason/version/sequence/null event actor.
2. Historical compatibility should add a default null key only where it was absent; existing explicit values remain significant. No old event update is permitted. The original `snooze_at_cut` body and accepted Work skip stay unchanged.
3. A page command is completed only with 1–200 exact native WOKEN/DUE events; a wholly failed/skipped page leaves no accepted orphan/empty command. Qualify every event and refuse mixed manual/due results, command members or jobs. Enumerate through the narrow native request-ID index with an explicit 201-row refusal boundary. Zero-job enrolment uses that same native result qualification or the original immutable manual receipt, and advances exactly one current-epoch next sequence.
4. Expected per-Candidate busy/moved/refused failures must advance the retained discovery hint while leaving that scope retryable. Do not report that all due records are cleared merely because the current suffix ended. Preserve visited/progress/failure distinctions in the closed ordinary page receipt. Unknown replies stop that minute's lane; native rows plus the committed hint recover the next attempt.
5. Every rollover Work still uses the current wrapper and every existing FUTURE_WEEK Case still uses the current Case owner. Snooze updates still invoke the original installed scope trigger once. Input discovery remains at most 200 rows before scope deduplication, not 200 groups after an unbounded join.

**The minimal fairness scope is now settled. Supporting native cursor columns and the qualified enroller are justified; exact guard/source closure remains the next review.** No additional object family or framework is recommended.
