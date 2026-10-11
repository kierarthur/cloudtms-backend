# SP-14 current investigation — sequence 110

Read-only investigation, 11 October 2026. No product, checklist, SQL execution, business data, hosted calls, fixture, test, rendering or deployment action was performed. This is not an item delivery or runtime acceptance receipt.

## Controlling scope and immediate conflicts

Full SP-14 card was read in the ordinary approved plan, `05_SOURCE_AND_PROTECTED_PAY.md:484–515`, along with every current checklist row D01–D05 and shared evidence D06 (`docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:3251–3271`), review/R5.json R5-D065–D069, master sequence110 and capture editor chain (`08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:65,194`), and R2 exact connections §4.

**BLOCKED — contradictory policy text, explicitly recorded rather than silently selected.** Card05:487–492 and its acceptance clause still require whole-Candidate REVIEW/DONE for Source query BLOCKED/UNAVAILABLE. The latest direct owner direction supplied in this task instead requires only the affected exact shift to be excluded for `HEALTHROSTER_NOT_FINALISED`, `SOURCE_MISSING_OR_NOT_AUTHORISED`, and `SOURCE_HOURS_DIFFER`, through OR47 segment identity; the same applies at first Authorise, with publication's `CANDIDATE_TIMESHEET_MISSING` exception retained. Current capture0559:1337–1363 reads a Work-level retained disposition and exits the Candidate before its segment handling. Those instructions cannot all describe one result. Root must record the owner's controlling amendment and the precise remaining true query/evidence-refusal behavior before implementation. No guessed sibling identity or blanket Candidate-Timesheet hold is acceptable.

**SUPERSEDED — R2 §4 producer/protocol instructions.** `BUILD_LOG.md:189–193` records BD019 owner Rule B: original Create's exact Workbench rows/expected revisions are the cut; post-click arrivals belong to next Draft; post-click revision-change refusal remains; Case balances stay live under their money locks. The accepted-input/effective-cut resolver, producer hooks and parked2153/2256 are not SP-14 deliverables. Preserve the actual existing raw Source gate capture, original selection, retained pin and replay. Do not treat the old R2 protocol names as current installed authority.

**BLOCKED — D05 transport wording versus later approved DR04.** Card/D05 asks queue retry to retain STEP nonce/epoch/cursor, while current approved DR04 intentionally has ID-only `{id,kind}` messages, fresh metadata CLAIM then one STEP, and exact own successful page yield. An unknown reply waits for native lease expiry and obtains a fresh claim of the durable cursor, never adopts an active token. Record that supersession explicitly; preserve the exact retry/replay contract for direct STEP callers and known rollback mapping. Do not restore full lease queue messages.

## Current exact owners positively located

| Owner | Current saved source / decisive point |
| --- | --- |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | `supabase/repeatable/11102026_0559_banking_pay_next_worker_reprepare_v1.sql:727`; direct eligible NEW/PINNING admission before module SHARE:760–777; original locked validation/replay:781–809; selected expected revision qualification:846–876; **captured** raw gate, not later live gate:989–1068; run_work, pin and cursor commit in same page. |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | Same0559 final registry owner; genuine-positive Source/PROTECTED WORK predicate and exact original pin/run/worker/revision/module/PAY_QUERY_STATE_V3 qualification:1332–1359; missing companion=EVIDENCE_UNAVAILABLE, blocked=QUERY_OPEN, Candidate exit:1361–1363. Zero/negative/ordinary/expense/additional/already-unresolved shapes remain outside this gate. |
| `private.bpay_next_run_work_query_pin_guard_v2()` | Same0559 final registry owner, declaration5473. Append-only retained identity still needs full final census below; no new companion table is proposed. |
| `private.weekly_source_first_authorise_core_v1(uuid,text,uuid,jsonb)` | `10102026_0806_banking_pay_next_first_authorise_covered_hours_gate_v1.sql:4` final registry owner. Original actor/canonical-family/root guards precede query check. Original family advisory lock:108–109; protected permission:111; OPEN manual query:112–120; actual relevant OPEN incidents:122–135. **Only** comparison `CANDIDATE_TIMESHEET_MISSING` is excepted at130; remaining uncovered incident at131 blocks the whole Authorise. Original generic owner called once at142–146. This is the actual latest core, not provisional0755. |
| Canonical query facts/gate | `private.weekly_source_pay_query_facts_v1(uuid)` final root0308 / physical `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc`; `private.weekly_source_pay_query_gate_v1(uuid)` wrapper0308. Named missing-Timesheet exception and qualified WAIT evidence do not imply other families are cleared. |
| Native Source admission key | `private.weekly_source_pay_query_admit_v2()` in `05102026_0044_bpay_next_source_pay_query_admission_v2.sql`. Admission controls concurrency, not a Candidate-Timesheet business hold; full exact census follows. |
| Worker known rollback decoder | `broker/src/bpay-next-job-step.js:370–396`: only enumerated55000 cases have their own recovery; generic55000 is NOT_ELIGIBLE; 40001/40P01/55P03/57014 give retryable/known RETRY_CALL; timeout/unknown STEP remains outcome_unknown with RETRY_SAME_STEP. Named Source55P03 telemetry at412 does not change financial authority. |
| Current queue | `broker/src/bpay-next-financial-queue.js:143–173`: ID delivery fresh CLAIM then one STEP; original token only invocation-local. Original background publishes ID notifications and uses that consumer. |
| Existing Source Worker decoder | `broker/src/weekly-source/routes.js:956–999`: exact Source admission55P03+message returns rollback-only retry with automatic_retry:false/workflow NOT_ESTABLISHED; retained protected-action errors remain unknown unless exact native refusal proves otherwise. General route errors remain separate. |

## Smallest permitted continuation

Complete the exact existing first-Authorise/publication/Source decoder and pin/segment census before choosing successors. Reuse exact native approved segment identity; preserve original Create gate snapshot and immutable pin identity. Retain genuine positive-only gate and missing-companion refusal for any **remaining** actual query blocker, without promoting the three owner-named shift exclusions into whole-Candidate failure. No new command, table, producer, digest/witness, clock, resolver, calculator, Source entitlement rewrite, Additional exemption or blanket Candidate-Timesheet hold is justified by this card.

D04 is currently source-positive for known rollback versus unknown outcomes. D06's bespoke fixture/hash proof programme is superseded; future real OPEN/PIN contention, ordering, replay, bounded pages, liveness and actual Office outcomes remain NOT_RUN. Screen wording belongs to SP15; Source component/channel changes remain SP20's later chain.

## Initial byte receipt (not final source closure)

-0559 SQL: `514fb6a069dfe860d06c76d98302554bf3ed93e8e027cdbac460bf8cea7da9a6`.
-0806 core: `8024efb3a366ed6b2247df94c910a5176a5c9721a75d5688997b07b1c845bfe5`.
- job-step.js: `d1a1349423ff00c7ed2713ea053e7d334c2015eeb250e8f2a33aa8aa42b07608`.
- financial-queue.js: `19f08cbdd3ba40b9439b99cc274d7fa48b98daed82bbd31207ca58c5c638cd68`.

Status: investigation OPEN; concrete policy/transport contradictions BLOCKED pending explicit controlling amendment, not an implicit product choice. Remaining exact census will append here.

## Completed current census and minimal shift-only seam

The three owner names are already the exact native comparison `issue_family` values, not Worker aliases. In `qualified_coverage.inc:160–166`, each factual `hours_incidents` entry contains `id,source_group_id,source_cycle_id,work_event_id,candidate_id,client_id,state,work_date,current_comparison_revision_id,comparison_contract_id,issue_family,material_comparison_fingerprint,latest_qualified_exception`. `manual_reviews` entries at30–43 contain exact review/event/Candidate/Client/Contract/date/state identity; they contain **no issue_family**. Do not classify a manual review as one of the three named incident families by date or prose. Qualified WAIT is a real exact exception; `CANDIDATE_TIMESHEET_MISSING` is the existing sole named nonblocking incident at157–158. Every relevant incident still participates in the factual state/digest. Missing or contradictory evidence at71–80/168–170 remains unavailable, not a fabricated clear answer.

The raw gate wrapper0308:12–17 deliberately removes both arrays. Current original Create1358:486–529 calls that five-key gate and skips a whole Work for blocked/unavailable before creating its selection. Its snapshot is written only for a admitted original Work. Later PIN0559:989–1068 consumes that snapshot. Current append skip0426:111–119 first returns the original accepted `run_choice_member.skip_code`; it does not retrofit a live gate into accepted descriptor membership. The fresh fallback0426:152–160 separately calls the gate. Both fresh original Create and any genuinely reachable unaccepted compatibility append require the same narrow classification; accepted replay stays before new reads.

Current Workbench owner is `private.bpay_next_workbench_refresh_work_core_v1(uuid)`, physical0426 classification. It already visits exact native member/segment identity:124–129; query classification is confined to the fresh AVAILABLE positive portion:180–249, while saved base H/R classifications remain unchanged. However, its one cached root gate currently gives every affected Work member the same OPEN answer. This named owner is required support for honest individual shift eligibility; no new browser eligibility/money calculation or reader is needed.

**Exact OR47 identity exists.** Source stage1236:1773–1779 uses the native `weekly_source.work_event_id` / `work_event_id` as canonical UUID-text segment_key; protected stage1236:2643–2648 does likewise. Match the factual work_event_id to the **same original expected revision's approved segment** and its own Work/component. Do not choose another shift by date, reference, schedule, ordinal, display name, or a current replacement revision. A missing/ambiguous identity does not authorise sibling exclusion. A relevant named incident with actual qualified WAIT is not an additional query hold.

**Small native extension is defensible; original-cut retention is mandatory.** Root's considered `QUERY_HELD` disposition plus a closed three-code reason on existing `selected_segment` / `run_work_segment` is narrower and more truthful than falsely relabelling a query as a Snooze or untick. Existing1100 migration has selected dispositions INCLUDED/SNOOZED, run dispositions INCLUDED/SNOOZED/NOT_SELECTED/REMOVED, exact Snooze-null and removed-approved-ID constraints. Any addition must retain those old tuples and make query reason present iff QUERY_HELD; preserve both independent Snooze and query factual authority according to an explicit precedence. It does not justify a new command, table, producer, certificate, query protocol or financial owner.

The effective reason **cannot first be discovered at SEGMENT_PAGE**. That page is after original Create and can arrive after query open/resolve. Capture finite typed exact event/reason facts in the existing immutable `bpay_next_run_work_query_snapshot` during the original Create transaction, alongside its existing raw five-key gate (small supporting column or private stored member, not a new wire/proof framework). Snapshot0151's existing FK requires the same original run_selection and Work/expected revision. Append1236:254–261 returns exact request replay first. Append/INSERT guard must then use only those captured facts; validating today's Sourcefacts at INSERT would violate Rule B. Current segment fact guard1236:3884–3915 enforces selected and captured dispositions, so both guard and owner need the same finite extension. No UPDATE of old frozen selections is permitted.

Capture0559:6263–6293 already routes non-INCLUDED fresh positive amount into excluded counters while preserving original approved amount, realised amount, recovery and held overhang. Hold0559:1607–1608 consumes the exact included segment total once. Reuse those maintained native counters and original segment shares; retain current completed-prefix/retry identity and money locks. A disposition-only extension must also keep its matching guard's positive/excluded formula and Source book/collection conservation consistent. Removing the whole-Candidate gate alone would otherwise offer the queried shift.

**Do not zero Source entitlement.** Initial stage1236:1457–1497 requires its real TSFIN/HEAD effective inventory and approved basis. Its existing `exclude_from_pay` sets approved component amount to zero at1584/1678–1679 and writes that same amount/pay_excluded to shift detail1795–1796. Original initial binder0018:99 verifies exactly that zero. Mutating this flag for a query on an already approved/paid shift can lower entitlement and manufacture collection debt. Preserve current inventory, approved/pay/charge amounts, native financial/Source/head/publisher facts. Query-only acceptance changes fresh offer exclusion, not approved entitlement, invoice movement, paid history, recovery principal, or held money. No Source publisher or binding formula rewrite is required.

### First Authorise and ordinary admission

Final first-Authorise outer is `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)`, physical2018:3528–3614. It acquires Source admission before original Candidate/family locks3569–3573, calls actual0806 core once3592, and only on success stages/publishes actual Source revision and binds initial TSFIN3601–3609. Keep all of that. The complete current generic Authorise0939 contains admission at81 and exact busy rethrow650 but **no separate live whole-query business check**. Source entitlement core0556 has existing admission329/2208/2227 and no separate whole-incident query blocker. Thus the named whole-incident business check requiring amendment is0806:122–135. Exclude only the three direct owner-named families (and retain current missing-Timesheet exception130); keep actual manual query blockers112–120, other unsupported/reference/evidence refusals, actor/canonical/generation/signature/permission safety and qualified WAIT semantics. Do not inject new query flags into initial publisher/inventory.

**Native Source applicability for a lock-free ordinary-key exemption is positively verified:** `public.weekly_source_office_authorise_scope_v1(jsonb)`, physical05102026_1210:38–57, plain STABLE reads. Applicable is current/unarchived/unrevoked WEEKLY HOURS Timesheet with a Contract, and exactly one `public.weekly_source_group_clients` membership for Contract.client_id whose `public.weekly_source_groups.active` is true and Timesheet.week_ending_date lies in membership.valid_from..coalesce(valid_to,infinity). Zero membership gives false; ambiguous/missing scope refuses. First Authorise needs no existing root_authorisation. A private/generic pre-read may reuse those exact native predicates; do not call the public permission wrapper solely as an ordinary classifier. Existing Source roots/revoked canonical scope are not ordinary by absence of a current query scope. Missing target/Contract/client/week, conflicting membership, unreadable configuration, or uncertain routing takes the existing key. Preserve admission on revoked canonical restoration: SP12 census304 names that material mutation expressly. Never first acquire the key after the ordinary Timesheet locks. Current0952 approval_route rejects revoked and also uses self_bill / the imported-family classifier, so it is not by itself a complete revoked/first-Authorise exemption.

Source Worker actual first-Authorise route is index168668–168744 (bulk reaches the same owner), using `weekly-source/authorise-routing.mjs:67–115`. Native public1210 probe returns exactly contract/applicable; malformed/unknown applicability refuses rather than guessing ordinary. The finite known-refusal decoder at128–148 preserves actual QueryOpen/ScopeUnavailable/EvidenceUnavailable refusals. Index168292–168307 uses it before generic fallback; original Source command dispatcher also has positional FIRST_AUTHORISE routes:106. No decoder changes are established as required if the success/refusal shape remains original. Generic busy map is exact SQLSTATE55P03 plus named message, not text-only proof; it never claims wider workflow rollback.

### Current jobs, replay and later E2

The **current** service is `public.bpay_next_financial_job_v1(text,uuid,text,jsonb)` in2041:972, not the original0280 literal or its0559 predecessor. Its validation precedes discovery/key, exact eligible NEW/PINNING key acquisition is1120–1126 before module SHARE1131, terminal/stale/advanced discovery defers fresh admission, and successful page yields only its exact own nonce/epoch1471–1479. Current direct PIN0559 preserves original order and retained replay. No service successor is presently shown necessary solely for query-held segment economics if dispatch/result shapes remain unchanged.

Current E2 System RELEASE acceptance already names actual SOURCE_PAY_QUERY_OPEN / EVIDENCE_UNAVAILABLE at0559:105–110; it is not a new REVIEW policy to reintroduce. A remaining genuine whole-worker refusal must keep the current release/projection mechanics and immutable accepted inputs. The three exact shift holds must not enter that whole-worker branch. Record the card's old REVIEW wording amendment explicitly, including later owner E2, instead of silently changing the reported outcome.

D04 static decoder findings are source-positive, with full joined behavior NOT_RUN. D05 transport's required supersession is the already approved DR04 lease recovery; no restore of parked queue tokens is proposed. D06 remains superseded; no bespoke proof programme was created.

## Completed census byte receipt

These are saved-source observations, not installed catalogue/behavior measurements:

| Source | SHA-256 |
| --- | --- |
| guarded Create1358 | `6d13cd0a482be586e49aaeb2fb67bca82613aa092657cbcc208e6f012acbc0a6` |
| segment authority1236 | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| canonical coverage include1417 | `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e` |
| query admission0044 | `01405bc854fb7b2e23cd740154f0302a8956cdd144b44c1f3e4eda67db80dcdd` |
| current financial service2041 | `80675b1f77af852977da427d0ce0702bfb2a2da6fafcea2a5e05b731810de44b` |
| Workbench classification0426 | `055b9c2fe70b085e7122dde2ef205da89cc0c2e96ffe6f2a48044b410f84f663` |
| accepted skip0426 | `aec4c904cee22e748ea1ab3006e8cfdbf50bcdb9dd567b1fb1b2bc9da7391b92` |
| generic Authorise0939 | `3a7fb9b31fd418a16cb56a3a9980233a96ddd2ee66af64f59cadd1f807e02977` |
| Source entitlement core0556 | `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933` |
| Source authorise routing | `587f0ff02e2da0b29e6ef996c0b2ef94fd83cca49287e9d9a1f3edc47ef21de9` |
| Source routes | `3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11` |
| job RPC | `65454073073b13c740db5557a9a84a3cd6f64ef71f8493b2ef8a1fbfae7f5b26` |
| background | `0524712d1bf52209eea3a7f8846491bac4fc452b2ea593978c85162476049514` |
| final-definer registry | `337d9895e0579ea306c5279d8706b42209e372771e8014a094e9b65055c6b365` |

Investigation census complete for the requested seams. Implementation remains unbuilt/unreviewed here. The precise plan/owner amendments and root's final chosen native retention shape require an explicit recorded resolution; the original contradictions above are not erased by this investigation.

## Important qualification — native Source predicate, not group absence

The Office-frontdoor membership predicate above is a genuine verified native routing condition, but **zero group membership is not by itself proof of ordinary Source-query scope**. Current `private.weekly_source_pay_query_scope_v1(uuid)`, physical0057:44–50, reads the actual Contract and always calls the installed `private._candidate_expense_source_family_v1(uuid)`; Source domain is Contract.self_bill=true **OR** that result=true. No existing root-authorisation is required. Therefore a generic ordinary-key bypass cannot use group count0, absent root_authorisation or NULL full query scope as its sole negative proof.

The actual installed classifier is STABLE SECURITY DEFINER and lock-free, final `26092026_0202_banking_pay_stage2_source_authorisation_v1.sql:178–225`. Its plain EXISTS predicate is exact WEEKLY/HOURS/nonadjustment, resolved CONTRACT_SETTINGS_AUTHORITY_V1, actual import_authoritative=true or captured settings self_bill=true or exact booking `weekly_source_row_timesheet_lineages`, plus exact primary contract_week. It deliberately does not erase imported ownership on withdrawal/archive. It raises SOURCE_ROOT_HAS_SEPARATE_CLAIM_COMPONENT for an incompatible live claim. A plain pre-read may use the same current native classifier/Contract predicates; classifier failure is unknown and therefore requires the original admission key, not an ordinary answer or swallowed integrity permission. Revoked canonical restoration still requires the key. Preserve original errors and locked validation; never acquire the key for the first time after downstream locks.

For the narrow captured query extension, derive the raw five-key gate and exact query-held identities/reasons from the **same Source facts read** under original Create admission. Do not separately reinterpret a raw digest using later facts; do not require a browser assertion. This is direct retained native input, not a new digest/witness protocol. Any honest native size/identity failure must remain explicit and fail closed; no truncation or date-based partial matching. Current query-pin/money owners and genuine nonpositive/held/recovery exceptions remain.

This qualification supersedes any reading of the earlier frontdoor paragraph that would allow group absence alone to bypass admission.



