# WB-37 current source investigation

11 October 2026. Read-only investigation for sequence 74. Root alone writes product. No product edits, database calls, SQL execution, tests, fixtures, hosted operations, checklist changes or commits were performed. App journeys are NOT_RUN.

Read the complete approved card `02_WORKBENCH.md:1332–1357`, all nine WB-37 checklist rows and their shared-owner entries, R2-F01 and the WB-37 review, the master sequence, and the current WB-36 closure/design. The card owns two named bounded service page RPCs and their minute recovery lanes: `public.bpay_next_snooze_due_page_v1(jsonb)` and `public.bpay_next_workbench_week_rollover_page_v1(jsonb)`. Scope is native control expiry and stored eligibility refresh; it does not authorise financial revaluation, new financial jobs, command kinds, producers or a protocol redesign.

## Binding date and scope rules

- A dated snooze remains effective through its `until_date`. It is due only when `until_date < (clock_timestamp() AT TIME ZONE 'Europe/London')::date`.
- A FUTURE_WEEK contribution is due when its maintained `eligible_from_date <=` that same London calendar date. Reuse the configured DR-01 eligibility/week owner; do not derive eligibility from a guessed reporting Sunday or the selected pay date.
- Refresh the exact existing Work/Case scope and its stored contributions. Preserve money amounts and frozen Create membership. A missed minute tick must remain recoverable from native indexed rows.

## Current final owners and existing runtime

The saved `final-definer-closures.json` identifies these current owners:

| Object | Current source |
| --- | --- |
| `public.bpay_next_job_wake_page_v1(text,integer)` | `supabase/repeatable/04102026_0560_banking_pay_next_queue_recovery_service_v1.sql` |
| `private.bpay_next_receive_command_v1(uuid,text)` | `supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql` |
| Snooze context/action/metadata guard/at-cut/receipt guard | `supabase/repeatable/10102026_1100_banking_pay_next_snooze_v1.sql` |
| `private.bpay_next_workbench_require_next_v1()` | `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` |
| `private.bpay_next_workbench_refresh_work_v1(uuid)` | `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` |
| `private.bpay_next_workbench_refresh_work_core_v1(uuid)` and `private.bpay_next_workbench_refresh_case_v1(uuid)` | `supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` |
| `private.bpay_next_workbench_queue_origin_v1(uuid)`, snooze due trigger and public Workbench runtime service | `supabase/repeatable/10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` |
| `private.bpay_next_workbench_mark_processing_v1(uuid)` | `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql` |
| `private.bpay_next_enroll_one_v1()` | `supabase/repeatable/10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql` |

`broker/src/bpay-next-background.js:127` has the existing independent ENROLL, READY, LEASE_EXPIRED, EXPIRY, CLEANUP, WORKBENCH_DUE and WORKBENCH wake lanes. Each failure is contained to its lane. The Workbench queue carries only native task IDs and lease/phase/cursor metadata, with 100-row pages; no financial amounts are transported. `broker/src/bpay-next-runtime-rpc.js` owns the closed service adapters and Workbench due/wake/claim/step schemas. Neither card-named WB-37 page RPC nor SNOOZE_DUE/WEEK_ROLLOVER minute lane is present in the current source.

## Existing combined due lane is partial support

The current `public.bpay_next_workbench_runtime_service_v1` DUE_PAGE branch (`...1429...sql:570–612`) already uses London date, bounded indexed snooze expiry mirrors and maintained FUTURE_WEEK items, ordered Candidate → Work → Case preholds, exact Work origin scheduling and exact Case refresh. It marks the private expiry mirror nonpending only after its captured snooze version still matches.

It does **not** clear the native snooze or append a WOKEN/DUE audit event. It also gives one combined page to both categories, without the separately capped card-owned minute lanes or a completion receipt. Treat this branch as current support to reuse or narrowly supersede, not proof that WB-37 is complete.

Correction to the initial hypothesis: queued Work does not remain FUTURE_WEEK. `...1429...sql:268–278` coalesces its existing ORIGIN task and invokes the current `...1346...sql:36–68` Processing owner, which deletes the exact Work contributions and replaces them with nonselectable Processing evidence. This removes successful scheduling from the FUTURE_WEEK due index immediately. The current `...1438...sql:318–350` refresh wrapper handles pending origin work and bounded linked Case propagation; D07 should call that named wrapper as the card requires, preserving its synchronous/coalesced behavior.

## Native schema and guard gaps

`supabase/migrations/10102026_1100_banking_pay_next_snooze.sql:50` already has the native partial due index `(until_date,id) WHERE active AND until_date IS NOT NULL`. `...1252_banking_pay_next_workbench_summary.sql:180` already has `(eligible_from_date,id) WHERE reason_code='FUTURE_WEEK'`. The existing `...1429...sql` migration supplies the separate version-bound snooze expiry mirror and its partial due index. No new discovery table or duplicated due index is required.

The native snooze has no `cleared_reason` column; the card explicitly requires `DUE`. The event enum already includes WOKEN (`...1100...migration:54`), and event actor is nullable, but current guards admit only manual active-admin SET/CLEAR transitions. `...1100...repeatable:343–363,383–394` requires active admin identity, maps every inactive update to SNOOZE_CLEAR and every inactive event to CLEARED, and equates event actor with the retained snooze updater. Its deferred receipt guard (`:479–565`) additionally requires the exact existing manual snooze request receipt, whose actor is non-null. A service cron cannot truthfully impersonate the prior administrator or bypass these guards.

The smallest necessary supplement must therefore remain within the existing native snooze schema/metadata/receipt owners and existing command acceptance/control sequence. Preserve the manual paths and the immutable historical at-cut owner (`:450–475`). A due event must receive a later accepted control sequence so earlier accepted Create scope still resolves its original historical snooze state. Do not introduce a new command kind or system-user identity to evade this boundary.

Adding `cleared_reason` also affects historical row-JSON comparisons. Existing audit `after_state` predates the new nullable key, while `to_jsonb(old)` will include it. The metadata guard's prior-event comparison (`...1100...sql:386–392`) must compare the existing retained business tuple with a compatible nullable-key normalization, rather than reject all retained snoozes or rewrite immutable old events. Restrict the added field to the actual DUE transition; preserve the existing manual SET/CLEAR argument and receipt shapes.

## Exact accepted order and unavoidable enrollment support

`private.bpay_next_receive_command_v1` (`...1404...sql:32–58`) holds the NEXT epoch, accepts one command-clock sequence under the original short lock, and returns the original sequence for an exact command-ID/kind retry. The existing native CLEAR kind is sufficient; a new SNOOZE_DUE command kind is unnecessary. Finish a successful due clear as an original zero-member COMPLETE command and retain the exact native WOKEN event as its durable result. A later event/control sequence cannot replace a historical event below an earlier run's retained snooze cut. The Work historical predicate remains `...1100...sql:450–475` and the original current accepted selection skip remains `...1832...sql:89–97`.

There is a concrete current ordering gap to close if the page accepts these native CLEAR commands: `...1100...sql:323` completes all manual snooze commands with zero members, but `...1832...sql:1313–1316` selects the next exact agency sequence and handles COMPLETE only inside PREPARE. At `:1438–1440`, a completed snooze returns NULL without advancing enrollment. The new service must not create additional commands that permanently block later PREPARE/financial enrollment.

The minimal named supplement is the current `private.bpay_next_enroll_one_v1()` owner, advancing **one** exact completed, current-epoch native SNOOZE_WARNING_ACK_ISSUE/SNOOZE_SET/SNOOZE_CLEAR command only after its original matching manual receipt or strictly qualified WOKEN/DUE event is present, with `expected_member_count=0`, `enrolled_member_count=0`, nonnull seal and no command members/jobs. Preserve PREPARE's special completion checks and every financial branch. Return no job. This is necessary ordered metadata closure, not a new producer, job family or financial protocol. Do not add a generic skip for arbitrary COMPLETE commands.

For an atomic bounded due page, one accepted existing CLEAR command can bind that page's exact WOKEN events; it need not mint one global ordered command per visited snooze. This keeps the existing one-command enrollment service from accumulating hundreds of zero-job sequences ahead of the next financial command. If root chooses this minimal page binding, the native due guards must validate every affected event/row, rather than treat a single arbitrary event as proof for unbounded scope.

A truthful service transition needs no new administrator: preserve retained manual provenance on the snooze, use the existing nullable event actor for WOKEN, and make the existing guards explicitly admit only a service-role NEXT/current-epoch, old-active, dated-and-due CLEAR. Conserve all scope/revision/basis/economic fields, require version+1 and accepted control sequence, and bind before/after/event/native command exactly. A narrow due-specific deferred check can use this native event/command instead of fabricating a non-null manual request actor. Retain every original manual receipt/permission check.

## Native eligibility owner and scope refresh

DR-01's current `private.bpay_next_append_work_skip_v1` is in `...1832...sql:64–109`. Its concrete week check at `:81–82` compares native `week_ending_date-6` with the Monday of the retained London business week; it does not compare with official pay date. The current projection (`...1946...sql:156,251–253`) maintains that Work eligibility date directly. D07 can consume the maintained indexed `eligible_from_date`; it needs no independently inferred week or official-date calculation.

Current Case projections also have FUTURE_WEEK native rows: `...1946...sql:503–505` maintains the genuine rule next-due/case due date. The already built combined DUE_PAGE refreshes them. If the Worker replaces that old lane with the two new lanes, retain these already supported Case due promotions using the same exact Case owner; otherwise removing the old call would silently drop existing behavior. No new Case formula is needed. The current Case priority checks preserve frozen IN_PREPARATION, resolution and PAUSED branches (`:472–505`).

Native snooze updates already run the installed `...1429...sql:7–29` trigger: its exact Work origin task supplies a same-transaction Processing barrier, or its exact Case refresh replaces current Case contributions, and its expiry mirror records the new version/nonpending state. The snooze page should use this current refresh seam once rather than separately repeat a second full scope replacement. Refresh owners alone own the existing natural-key contribution deltas; neither new due page writes those deltas directly.

## Minimal remaining product seams

| Seam | Required finite delta |
| --- | --- |
| D06 named snooze due page | Service-only/NEXT/current epoch; closed bounded input; indexed active dated native discovery ordered `(until_date,id)`, maximum 200; Candidate → Work → Case/native-snooze lock order with revalidation after waits; original accepted CLEAR sequence, native inactive/version/control plus `cleared_reason='DUE'`, WOKEN event and strict deferred native receipt; exact existing scope trigger; no duplicate expiry/audit after a lost reply. |
| Existing native snooze schema/guards | Add the card's reason field and narrowly admit the truthful due transition with compatible historical JSON comparison. Keep manual arguments, actors, warnings, receipt replay, scope and economic invariants intact. |
| Existing enrollment owner | Advance only qualified native zero-job completed snooze commands one sequence at a time, including the due event path. No job-kind or producer change. |
| D07 named rollover page | Service-only/NEXT/current epoch; indexed FUTURE_WEEK `(eligible_from_date,id)` discovery, maximum 200 native items; capture the bounded item identities before any refresh replaces them, deduplicate actual Work/Case scope IDs within that page, prehold native controls, call current Work wrapper/Case refresh once per affected scope, revalidate current facts. Reclassification preserves higher-priority blockers and existing financial facts. |
| `broker/src/bpay-next-runtime-rpc.js` | Add only fixed adapters for these two named SQL endpoints and closed bounded request/result schemas, preserving current 8s deadline, request/response byte caps, exact-key validation, bigint text, service role, fixed route-class header and unknown-mutation recovery. No Office route/action. |
| `broker/src/bpay-next-background.js:127–166` | Two independently caught SNOOZE_DUE/WEEK_ROLLOVER lanes, each finite N pages until its genuine completion/empty receipt; retained rows provide missed-tick recovery. Keep existing ENROLL/financial lanes and existing Workbench task wake/consumer. Stop the minute's duplicate combined WORKBENCH_DUE call once the new lanes preserve both Work and Case coverage. |
| Source registry/schema reload | Register only actual changed final owners and the two named pages; service-only EXECUTE, private support ACLs retained; required `notify pgrst`. |

Successful snooze clears leave the active due index. Successful rollover refresh removes/reclassifies due rows, or the existing queued origin transition removes them into Processing before the reply. These native commit boundaries supply normal idempotent advancement without a new discovery table. Pages must still count every visited native item, enforce the bound before joins/aggregation, handle busy/failed scopes without replaying the same head indefinitely within one tick, and keep all failed scopes retryable. A minimal internal native `(date,id)` seek/resume, if needed for skipped rows, must remain inside the named page adapter; do not introduce a generalized sweep/proof framework, browser cursor, history scan or financial admission shortcut. Per-Candidate failures must not undo unrelated successful native scopes or starve the other minute lanes.

## Existing scheduled ownership and completion limits

`broker/src/index.js:1315–1319` selects only `* * * * *`, avoiding the separate five-minute cron. At `:201515–201530` it obtains fresh owner status and invokes recovery only for NEXT; an unavailable owner is not silently LEGACY. The existing dependency factory (`:40998–41015`) uses the fixed runtime RPC and `BANKING_PAY_CONTINUATION_QUEUE`. Saved TEST Wrangler configuration already includes the minute schedule and queue binding (`wrangler.toml:251,266`). This investigation does not verify a hosted deployment or change configuration.

No screen, money owner, Source producer, parked F25 resolver, accepted-selector redesign, new command kind, new financial job, new table or new testing programme is indicated. No compile, gate, installation, queue or in-app execution was performed here. Root should keep build/source/installation/app boxes separate; all named later app journeys remain NOT_RUN.
