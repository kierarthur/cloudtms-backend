# HealthRoster WP-0 — journey feasibility

13 October 2026. This lane covers IMPLEMENTATION_START_PLAN §2 items 6, 7, 9 and 10, plus the refreshed complete MyTMS source. It changes only this report and WP0_JOURNEY_LEDGER.json.

## Decision at the WP-0 stop

Existing owners support ordinary ELECTRONIC adjustment-week enumeration and the current Daily detail projection outside the published Rota window. Genuine submission and manager-signature evidence are available from existing workflow/component owners. These are source feasibility findings, not successful correction journeys.

The new signed-correction journey cannot presently be composed unchanged. There is no committed WHOLE/request owner. Two current Timesheets cannot share one physical booking family; generic Daily reset resolves by booking; generic CANCEL rotates rather than completely retires correction-only records; manual adjustment deletion rejects correction identities. Initial P0/request approval before R0, all-member A0 approval and correction-scoped app drafts need explicit adjacent extensions. They are recorded once and left for an approved build stage.

The mail pipeline also has three definite source gaps: Mode A skips query-only follow-up without auto-authorise targets; retry rejects all PENDING operations; and new reminders cannot safely pass the frozen original action-set/COMPLETE receipt using mutable sent_count as replay identity. No mail was queued or sent by this lane.

C09 has a concrete existing-owner conflict. After invoice issue, the exact query evidence and catalogue refuse the query-email action. D8 remains answered: show the reason and correct only when Office judges the signed Timesheet wrong. No invoice-lock exception or mandatory correction is inferred.

The initial MyTMS main observation was safely resolved. Main 18f07fcc is an ancestor of the complete published approved-hours branch 3d26ed7. Parent verified that remote identity and fast-forwarded its own clean named worktree to 3d26ed7. All app, root contracts, clients, locks, build and Google inputs are present together; no merge or source rewrite was needed.

The ledger contains **22 assertions: 6 Source/static PASS, 9 existing-owner FAIL, 7 UNPROVED**. A FAIL describes the expected journey versus the current owner, not permission to fix it. Installed catalogue evidence supplied by the parent confirms the booking constraints and C09 catalogue refusal. No API, document, browser or phone acceptance is passed here.

## Authority and source identities

Authority is SEND_2_TO_HEALTHROSTER_IMPLEMENTATION_CHAT_WP0.md, SEND_2B_WP0_LIMITS.md and the superseding SEND_2C_NO_LOCAL_DB_USE_TEST.md. D1–D8 remain in force. D2 requires STOP/report before feature work. D5 requires exact TEST setup approval; D3 release/promotion and D6 native build authority are separate. D1 continues to hold only the disputed shift: the five × £40 / Tuesday dispute oracle remains £40 held and £160 payable.

Sources:

| Repository | Named worktree | Reviewed identity |
|---|---|---|
| Backend | .codex-worktrees/healthroster-v2-backend-13102026 | 77ffc9d7d15d19b6453ae25441eee8c1bff58c90 |
| Office | .codex-worktrees/healthroster-v2-office-13102026 | 887e7af7cd15d9f4034ad73806b44344f3eb0791 |
| Complete MyTMS | .codex-worktrees/healthroster-v2-mytms-13102026 | 3d26ed757bc5fed8208e3534abb7ad50a8bd45d3; initially main18f07fcc |

Workspace and all three worktree AGENTS were read. No nested AGENTS were found in the named worktrees. The separate Banking builder/worktree/chat was neither edited nor messaged. The backend named worktree's committed definitions are read-only evidence.

Paths below are relative to the named backend worktree unless prefixed Office/MyTMS. SQL shorthand refers to supabase/repeatable; baseline refers to supabase/baseline. Exact declaration/root mappings are in the JSON and the parent's WP0_SOURCE_OWNER_INVENTORY.json. Canonical ordering uses the repository's sqlFiles/sqlDateKey and recursive, in-position include traversal. Name-level declarations do not resolve overloads or later dynamic pg_get_functiondef patches.

## Existing owners and minimum extensions

### Adjustment listing, outside-window Daily and submission

- **J01 / J02 — static PASS:** candidate_app_timesheet_page_v1 in 24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql:345 enumerates candidate_weeks and sorts by week/Contract/additional_seq/id; additional adjustments are not generally filtered out. Its last installing root is 28092026_1234_stage2_hosted_final_authority_reassert_v1.sql. Page v2 at 03102026_1540_candidate_approved_hours_visible.sql:93 delegates to it. Final transition at 10102026_1347_import_expense_qr_approval_v1.sql:79 restricts additional_seq=0/not-adjustment inside the IMPORT_AUTHORITATIVE allowance (:956–1010), while ELECTRONIC remains subject to ordinary current/capability checks (:1017–1051). No blanket route-family/listing successor is justified from that evidence. A real request-shaped P0 first-use is still unproved.
- **J03 — static PASS:** final detail v1 at 10102026_1347:5432 delegates a current DAILY record to private._candidate_daily_read_projection_v1. Final projection is 28082026_2236_candidate_daily_receipt_display_v1.sql:5, not the older 2000 body. It takes hospital/ward/job/scheduled start/end from the stored Timesheet (:79–85). Receipt context at 28082026_1912:4 checks exact current/owned Daily record (:40–87), not current Rota membership. A new distinct-family P0 must acquire its own lawful request ownership and frozen booking display; this pass does not grant never-submitted outside-window entry.
- **J04 — static PASS:** explicit submit freezes immutable_submission_json/sha256 and worker_submitted_at_utc (10102026_1347:2280–2297). Electronic finalisation requires APPROVED exact request generation/review manifest, immutable HOURS component, candidate and manager signature witnesses and ready final render (:4984–5039). A proposal schedule or device draft counts as none of these. Existing _import_review_query_evidence_core_v1 additionally uses authorised_at_server (21072026_1820_00:659–668), so it cannot alone express signed-but-not-authorised A0 Checklist membership.

The smallest addition is one current submitted/signature/request selection contract with separate facts for worker submission, manager-signed finalisation and Office authorisation. Readers use the same exact physical/version/generation witnesses; no parallel submission store or actual-schedule proxy is introduced.

### Physical WHOLE identity, request-first approval and cancellation

**J05 — FAIL, source and installed constraint agree.** Structural finishing baseline :9806 has timesheets_booking_id_current_uidx, UNIQUE booking_id WHERE is_current; :9809 has UNIQUE booking_id/version. T0 and a waiting current P0 cannot both live in the same physical booking family. Existing Daily receipt reset, 28082026_1928:41–53, resolves current by the requested booking and verifies the expected Timesheet ID.

The viable minimum design is a distinct server-owned physical correction booking family, with immutable original/root business booking, original physical ID/version, P0 physical ID/version and durable request links. Business booking/date lineage stays the same; physical family allocation does not overwrite T0. A stable request/P0 adapter must precede ordinary booking aliases in read, draft, transition and retirement. Do not relax uniqueness, choose “latest” from two current members, or cancel by original booking/date alone. The exact first-use/family model remains a design freeze and TEST proof gate.

**J06 — FAIL.** Generic CANCEL at 10102026_1347:3480–3614 retires manager requests/components then invokes weekly or Daily reset. It does not prove removal of every correction-only successor and week. Candidate cancel v2 is physically at 24092026_2247:2347 and reinstalled by 28092026_1234; its opportunistic cleanup (:2267–2341) is expense-only. Manual weekly adjustment delete preview at 09022026:715 resolves the booking family and refuses correction_id/kind (:852–865), with a linked additional weekly Contract week required (:868–889). Final guarded apply is 17092026_0900:705. The later actor-predicate patch in 06092026_2355:18–45 changes the final preview body after its last CREATE declaration.

A history reader and generic archive transition are not complete retirement proof. The minimal extension is an exact-request retirement-only adapter beside the existing workflow/removal owners. It must keep audit/signature evidence, retire approval/manager/component authority while exact P0 exists, then legally retire every correction-only successor/week, and only then commit CANCELLED. T0 financial/document/current/version witnesses remain byte-identical. A failed or partial result rolls back; replay returns one receipt; Cancel/Authorise has one terminal winner. Generic submitted/delete/protected-root guards and T0 reset remain unchanged.

**J07 — UNPROVED.** No committed SIGNED_TIMESHEET_CORRECTION, AGENCY_INVOICE_CORRECTION, WHOLE_TIMESHEET_REVERSAL or weekly_source_agency_invoice_whole_correction_v1 was found in SQL/application source. Existing _ctms_expand_lifecycle_items_v1, correction chain and pair owners recognise import-classified CHANGED_HOURS/CANCELLATION units. One new justified WHOLE owner must reconcile unsigned agency after-issue admission with signed all-state request admission. The exact current physical family and durable origin govern; no copied phase-3 scope is assumed.

Office's dated weekly scope probe (05102026_1210:44–65) considers every matching active group. The Authorise route then uses first-authorisation wrapper (broker/src/index.js:169855–169907). Consequently signed-group roots cannot be assumed unmanaged. Parent's installed rows/owner proof must determine actual status. Only the exact signed check-only route may receive an adjacent managed-root exception; unrelated Source/NHSP managed roots stay excluded. Crossing the protected boundary stops that dependent lane.

**J08 — FAIL / J09 — UNPROVED.** Initial entry must start from P0/request while R0 does not exist. Existing expansion (21072026_1235_00:379–484) resolves already-classified import units, and pair transition (21072026_1235_08:64–86) requires an existing valid chain. Preview must create nothing. Under request/scope/current/signature/proposal locks, mutation creates one R0, expands the complete unit and delegates the unchanged owner atomically.

Current final bulk authorise is physically 14082026_1310:2103 and reinstalled by 05092026_1200_banking_pay_draft_v8_final_authority_closure.sql. It has lifecycle group propagation/postconditions (:2353–2365,2433–2448), but no request-aware 3-member A0 admission. The new adapter must partition complete legal/blocked units before delegation, reserve A0 T0 inside selected P0/request units before ordinary iteration, and de-duplicate by durable request/correction ID. T0 alone retains the confirmed warning/ordinary action. Unsigned/stale P0 cannot indirectly approve T0. Both SQL errors and returned partial/failure receipts must roll back that complete unit after verifying member postconditions.

**J10 — UNPROVED.** No Candidate “authorised” emitter was established in the inspected generic/bulk authorise bodies; audit events and notification consumers are not proof of installed emitter/trigger absence. Census the exact installed trigger/caller path before any notification suppression. Only request-bound A0 T0/R0 ordinary notifications/projections are suppressed; P0 is the effective approval and the original displays “replaced”, while deliberate T0-only ordinary Authorise stays normal. No pinned generic body edit is authorised.

### Frozen mail actions, COMPLETE follow-up and C09

**J11 / J12 / J13 — three definite FAILs.**

1. Mode A gate in broker/src/weekly-source/routes.js:897–922 filters solely by nonempty auto-authorise targets, so query-only clients never reach the existing runner. Extend only its persisted EMAIL admission; preserve auto-authorise fail-closed semantics and use truthful asynchronous query-only delivery state.
2. Retry in broker/src/import-review.js:1022–1056 rejects every PENDING operation. Allow only exact source-committed operation/no-delivery cases older than 10 minutes; young cases stay pending and existing delivery cases use reconciliation.
3. Enqueue in 21072026_1820_04:66–83 requires the selected action IDs equal the immutable original response set and existing outcomes. Runner at import-review-follow-up.js:229–245 short-circuits COMPLETE. Enqueue's reminder sequence derives from sent_count (:111–118), and its deterministic key incorporates that sequence (:143–146); after SENT a same-command replay can therefore select a new key unless command identity is frozen.

Reuse the operation/outcome/delivery stores with explicit adjacent reminder-command admission/resume. Freeze command identity, permitted issue set, actor/client/scope/current version, accepted comparison, recipient/evidence and sequence under the existing locks. Same-command retries before/after SENT return the same delivery/outbox; a new deliberate reminder receives a new durable command. Do not mutate the original apply action/outcome or COMPLETE receipt, rerun apply/TSFIN/auto-authorisation, build a scheduler, or derive replay identity from sent_count. Queued, provider-accepted, SENT and command-admitted remain distinct states. No hosted enqueue/delivery proof ran here.

**J14 / C09 — existing route FAIL, D8 remains ANSWERED.** Exact controlling original owner requirement is OWNER_ANSWERS_ROUND22_20261012.md §2 R-243 item 3: “query Temp Staffing about every difference, with reminders, reusing the existing query-email machinery”. Later D8 gives the operative conditional remedy; it is not reopened.

Concrete case: T0 is a correctly manager-signed 10-hour Timesheet; the first HealthRoster upload matches it; I1 is issued. A later upload wrongly says 9 hours. T0 is still correct. _import_review_query_evidence_core_v1 (21072026_1820_00:713) returns TIMESHEET_PRESENT_BUT_INVOICED. Final action catalogue 06102026_1523:1271–1373 disables the email/reminder and removes its redundant row where applicable. Parent's once-only installed definition confirms those guards. A truthful “Timesheet present but invoiced” display is lawful; it does not prove the email/reminder requirement.

Recommendation: retain that exact reason and unchanged I1/T0; Office uses Correct this Timesheet only if it decides the signed Timesheet is wrong. Report this route conflict for owner review before the affected feature's activation. Do not invent an after-issue query exception, remove the guard, mandate a correction when HealthRoster is wrong, or ask the owner to re-answer D8.

### Null-Contract Daily, pre-staging matching and canonical readers

**J15 — FAIL.** v_timesheets_daily_match (baseline views :1233–1266) joins current matching TSFIN to current DAILY Timesheet and has client identity, but lacks optional Contract/submission lineage. The final catalogue's Daily authority/rate witness still comes from contracts WHERE id=rtsx.contract_id (06102026_1523:249–267); null Contract produces no witness. hr_daily_validation_preview_v1 (21072026_1820_03:143–160) requires a staged import. It cannot itself prove pre-staging admission.

Extend the same matching/resolution owner/view to expose exact current Timesheet/client/optional Contract/date/dated-authority/signed witness before stage. A genuine null-Contract Daily with proved client remains eligible; unresolved no-client receipts stay unresolved. Never fabricate a Contract, borrow another client's weekly Contract, or treat a matching name/booking as financial client authority.

**J16 — UNPROVED.** Existing weekly qualification has exact Contract candidates and durable/Office tie handling (broker/src/weekly-source/contract-qualification.js:85–126). The complete signed Daily pre-stage route is absent. Reuse exact profiles, then daily detector, then actual matched Timesheet scopes. Group by proved client first; use the existing picker limited to matched client IDs/counts/routes for real ambiguity. Mixed scope and all-no-Timesheet files refuse with separate truthful messages and no stage/query artifact. Empty Daily is an explicit zero-shift declaration for an approved selected client. Existing accepted no-Timesheet rows may create the ruled No-hold query with provenance to their accepted import.

**J17 / J18 — UNPROVED.** Existing workspace readers are reusable (15092026_1534:763; final combined review reader 09102026_1130:6); the check-only Checklist producer/arm is unbuilt. One submitted/request selector must drive Queries and Checklist. T0 stays comparison authority while correction waits; its correction item takes display precedence over “signed, not authorised”. Once approved, only P0 counts; R0, unsigned proposal, planned hours and device drafts never become submitted membership. Current-week actual submitted clients and relevant earlier unauthorised items follow the owner rules, with exact root/current/request de-duplication.

Specify a total order including client ID/week/item ID after display sort, cursor scope containing actor/permissions/filter/sort/selection revision, and separate section/expansion cursors and observers. Existing bounded sentinel/scroll (Office import-workspace.js:1245–1253,1751–1782) is useful reuse, but its shared session.observer/raw scrollTop does not prove new multi-expansion anchors. Reanchor after mutation/refresh by item identity and nearest surviving row; retain caller context on child return. Current positional modal tab wiring must use keys if Finalise is omitted. Hide Finalise only for a single-scope TIMESHEET_AUTHORITY group; keep combined Finalise.

### MyTMS detail, field deltas and drafts

**J19 — UNPROVED.** Existing weekly/daily controls parse multipart/overnight schedules and every break mode. Extend them, not a second schedule store. correction detail must carry exact P0/request/proposal ownership, frozen original signed segment/evidence and unambiguous per-field deltas. Pair stable segment IDs/value witnesses; ambiguous pairing refuses correction preparation. Highlights identify only actual changed starts, finishes and breaks, with genuinely added/removed parts labelled explicitly. Break-only does not claim start/finish changed; unchanged fields remain normal through confirmation/resume.

**J20 — FAIL.** MyTMS core/drafts/context.ts:16–39 chooses the Daily booking first; :49–69 includes workflow anchor/target aliases; :129–137 admits requested aliases. This would allow original/cancelled correction alias contamination without a new request-aware boundary. The single existing draft store and scope remain the authority. Add correction ownership before the ordinary Daily booking branch using stable physical P0+durable request, and include generation only when proposal/ownership changes. Admit only server-proved P0 identities through canonical/alias/requested-record/HOURS/review/WORKFLOW_PROGRESS/approval/sign/submit paths. Preserve all unrelated old drafts. New/cancel-reissued ownership resets editor loading once; harmless refresh/status/action changes retain current edits. Keep review-warning context separate from draft ownership.

Actual consumers are hours.tsx:63–75, daily/shift.tsx:40–74, review.tsx:59 and workflows/[workflowId]/approval.tsx:36,57, all in apps/candidate-app/src. initialEditableDays at hours-model.ts:134–139 requires Contract-week context; it cannot substitute for Daily. dailySegments at features/daily/model.ts:72–95 reads actual schedule first. The current Daily screen has the Booked shift/Yes-No form and no correction mode. Correction mode retains frozen booked display even outside Rota and opens unconfirmed editable proposal; it does not submit/sign by prefill.

**J21 — static PASS, inventory only.** Actual path: candidate_app_timesheet_detail_v2 → v1 → broker/src/candidate-app-backend.js:6299–6314 enrichment (component lineage, submitted facts, expenses, paper) → JSON response. Public broker successful response at candidate-broker/src/candidate-broker.js:1151–1162 forwards the successful body. OpenAPI TimesheetDetail at MyTMS contracts/candidate-api.openapi.json:22549 is additionalProperties=false; generated types at src/generated/candidate-api.d.ts:1582–1620 and app core/api/types.ts:13–15 are real consumers. No correction key exists. Two generic closed runtime TimesheetDetail validators were not established; strict Daily transport/BOOKED_DAILY_SHIFT checks are different owners and must remain strict.

The minimum same-change contract addition is optional bounded correction identity/deltas at the actual producer, exact adjacent boundary validation, generated schema/types and app consumers. No generic response rewrite is presumed. Preserve the current 72-operation inventory. Old-client field acceptance does not establish correction display/draft readiness; exposure needs a proved deployed compatible client capability/version plus actual installed-app acceptance.

## Complete MyTMS source delta and preservation

Initial main18f07fcc is the common ancestor of the active approved-hours history. Parent verified origin/codex/candidate-approved-hours-20261003 = 3d26ed757bc5fed8208e3534abb7ad50a8bd45d3 and adopted it in the clean named worktree by non-forced fast-forward. No default/active dirty source was reset, pulled, restored, merged or rewritten by this lane.

| Comparison with adopted complete source | Static delta |
|---|---|
| 18f07fcc main | 12 successor commits; 19 changed files, 1,296 additions / 109 deletions |
| Reviewed15fb7037 | 9 changed files, 267 additions / 20 deletions |
| Boundedcb0c4a87 | Includes the later reconciled Google/source evidence and prior4b72 history; no app/root contract/client/lock/native build-input delta |
| Prior metadata4b72c445 | One new3d26ed7 commit: 4 files, 52 additions / 7 deletions |

The new3d26ed7 delta is observational projection timing in availability-api/CloudTMSCandidateBridge.gs plus corresponding source hashes and google-availability-hybrid test expectations. Stage logging is try/catch-bounded, preserves completion acceptance and exposes only closed diagnostic facts; code tests were read, not executed here. Earlier pending/receipt/missing-availability reconciliation changes were retained. SOURCE_VERSION still labels the complete prepared package CURRENT_PREPARED_NOT_INSTALLED, so Git source is not a blanket Google installation claim.

Tree inspection confirms root Candidate/control contracts, generated clients/types, both lock/build inputs, Google installation/source/manifests and canonical Android/Play scripts coexist. Future owner-approved app releases preserve that complete repository layout. No native build, EAS/TestFlight upload, Google publication or account change occurred.

## Parent installed catalogue evidence

WP0_INSTALLED_OWNER_EVIDENCE.json records parent-owned serial Arthur TEST catalogue inspection: 37 routine metadata/ACL/hash rows, 27 columns and 4 indexes, with accepted=true, database=cloudtms_test_clone, rolledBack=true and fresh verification connectedFresh=true. All37 browser EXECUTE checks are false. Private Candidate/internal query helpers with service_execute=false may be intentionally owner-only callees, not automatic ACL defects.

The installed booking uniqueness and Daily reset booking/expected-ID guard agree with source. Installed action catalogue also contains the C09 invoice refusal and email masking/removal. This supports the specific existing-owner constraints above. It does not prove installed full-body equality, API first-use, application transactions or any unbuilt feature. This lane made no database call; parent retains its exact receipts. No new catalogue/rehearsal loop is requested.

The JSON includes exact routine/view/index/trigger inventory requests for any still-unproved installed proof. Use parent account-scoped fixed definition tools; the included SELECT-only catalogue text is a proposal and is not authority to pass arbitrary SQL to Arthur's fixed Operations route.

## Packages and future ordered TEST plan

**Ready to specify/start only after D2 owner review:** bounded optional detail/schema/type/draft contracts using the complete source; reuse of existing Weekly/Daily parser/form components; truthful Office Train0/read-only shell. Existing listing and outside-window Daily owners do not need blanket rewrites.

**Dependent build lanes held:** shared WHOLE/request identity and exact retirement, initial signed A0 approval, new reminder-command admission, null-Contract pre-stage resolver and C09 affected mail route. Signed issued corrections additionally wait for every WP-CN P1–P18 gate and the compatible published NEXT join. Ordinary signed pay/HealthRoster invoice independence is proved separately; it does not waive all-state signed-chain activation.

Earliest WP-PP publication is a reviewed, disabled foundation batch once the parent's permission-owner inventory and own tests pass. It precedes its consumers/Banking HR-06 dependency; this lane does not author or publish that helper.

Proposed batches, each separately reviewed/owner-approved under D3:

1. Compatibility shell/Train0 plus WP-PP/foundation contracts, all feature exposure OFF.
2. Additive server request/WHOLE physical identity, retirement, canonical selectors and reminder-command owners, with protected owners unchanged and no consumer exposure.
3. Office and complete MyTMS consumers against the frozen server contract; separate D6 native build/device authority.
4. Joined TEST acceptance after compatible published Banking NEXT and credit/document gates; activate only under D7 after every applicable gate.

Every install follows reviewed exact GitHub source → protected database release → normal backend → real Candidate private → synthetic private using its canonical config → public broker last → Office. Include separately verified invoice/document Worker deployment connections when changed; the four-target coordinator does not automatically cover them. A failed/unverified database release holds runtime and Office. Current D3 approval is not inferred from WP-0. Future local database work is only the mandatory clean PG17 mechanical contract generation/seal, one small container per release removed immediately; no local acceptance testing. Acceptance runs on real TEST.

Serialize shared-test integration with the Banking final stop under one integrator. Refresh committed NEXT pin and shared test HEAD before each promotion; prepare/review conflict-free feature result first. Shared owners include _ctms_expand_lifecycle_items_v1, timesheet_correction_chain_scope_v1, timesheet_correction_pair_transition_v1, invoice_correction_pair_scope_v1 and managed-root/first-authorise/query consumers. Do not edit Banking-owned first-authorise/NEXT monetary bodies, post-Draft sealed artifacts, Policy X freshness, TS_DAY or recovery/economic identity. Parent coordinates by source/receipts, without a Banking chat message. A bad install is fixed by a reviewed follow-up release; never manual database edits/ledger repair.

## Exact TEST setup approval list

WP0_JOURNEY_LEDGER.json contains JS-1…JS-7 with proposed synthetic IDs, before/after scope, verification and retirement limits. **Nothing in that list was created.** Fixed proposed IDs must first be proved vacant; identifiers emitted by future lawful fixture owners must be included in the final concrete approval manifest before mutation. This is explicitly a remaining setup-approval task, not an invented current fixture implementation.

| Approval item | Proposed narrowly scoped setup |
|---|---|
| JS-1 | Three synthetic TEST clients: weekly signed check-only, Daily signed check-only, other-client ordinary negative. Exact IDs fd0a0010-1310-4260-8260-000000000001/2/3; dated flags requires_hr=true, autoprocess_hr=true, no_timesheet_required=false only for check-only clients. No existing settings changed. |
| JS-2 | Two synthetic dated groups, fd0a0020-1310-4260-8260-000000000001/2: TIMESHEET_AUTHORITY/INVOICE_EVIDENCE_REQUIRED and AGENCY_INVOICE negative; membership from01/09/2026; all new application features OFF. |
| JS-3 | Synthetic Candidate IDs fd0a0030-1310-4260-8260-000000000001/2 with isolated TEST membership, one approved weekly Contract, one Daily entitlement and same-worker other-client weekly Contract. No real login/contact/credential reuse. |
| JS-4 | Independent weekly and Daily A0/A/B/C/D/D2 fixtures;05-weeks-old Daily06/09/2026; unresolved no-client and genuine client/scope ambiguity. Final exact Timesheet/booking/week/TSFIN/workflow/signature/document/invoice IDs need the lawful fixture manifest before approval. No manual paid/issued/SENT markers or payment execution. |
| JS-5 | Accepted comparison/query-only, young/stale PENDING-without-delivery, queued, SENT/COMPLETE and C09 cases with immutable command/evidence. Repository fixture clock/delivery support must exist; no provider success fabricated. Any actual enqueue/delivery sink and recipient get separate explicit approval. |
| JS-6 | Start-only, finish-only, break-only (none/duration/range), multipart, added/removed, overnight and unchanged frozen proposals; independent original/P0/cancel-reissued device drafts. Only synthetic draft scopes may be cleared through app controls. |
| JS-7 | Equal-name clients, >2 reader batches and >2 independent expansions, late insert/close and actor/filter/sort changes. Exact row count/IDs follow the frozen reader contract and are approved before setup. |

Acceptance proves real private/public API first use, signatures/document originals, Office selection/anchors and physical compatible MyTMS. Its fixtures must assert unchanged T0/paid snapshot/original invoice bytes; one R0/current P0; one complete-unit authorisation; no orphan/cancelled authority; exact difference-only NEXT/recovery and separate per-shift D1 hold oracle. Stateful mail/delivery and native builds need their own explicit authority. Tests not attainable under the current bounded preparation stay UNPROVED.

## Closing record for this lane

- Created only WP0_JOURNEY_FEASIBILITY.md and WP0_JOURNEY_LEDGER.json in the assigned output directory. No source, instructions, Banking files or existing dirty inputs edited.
- Static code, schema, source history and curated parent catalogue evidence were reviewed once. No SQL/application RPC, database connection, network/runtime call, test programme, build, Docker/local database, phone or browser action by this lane.
- No Worker/database/Office/Google/native deployment, feature activation, commit/push/PR or message to Banking.
- No secrets/private business rows/raw diagnostic dumps printed or saved.
- Cleanup: zero resources created, so zero containers/volumes/images to remove. Output artifacts retained for parent consolidation.
- Stop checkpoint retained. Owner review/continuation, exact setup approvals, final installed/API/document/Office/phone/NEXT/credit gates remain separate.

