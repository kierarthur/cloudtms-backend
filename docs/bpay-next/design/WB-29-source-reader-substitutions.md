# WB-29 native Source reader substitutions

12 October 2026. Read-only source plan for approved BD155 consumers after the authored2135 foundation. No product edits, SQL/RPC/business execution or tests. All implementation/runtime acceptance here is **NOT_RUN**. Canonical approval is original Source authority, never eligibility for an old accepted offer: owner E2 still holds back/system-releases Work whose current revision changed after Create.

Use `private.bpay_next_treatment_approval_revision_v1(uuid)` from `12102026_2135_banking_pay_next_treatment_native_publication_v1.sql:5–34` as the one direct, write-free sealed-revision root qualification. It returns an original itself; a treatment additionally proves same-Work sealed original/predecessor, unchanged revision metadata and genuine POSITION_APPLY command/member/publication. The pre/post-seal payload assertions protect complete native child equality. Do not use raw `coalesce(root_link,id)` to admit a treatment. Native child correspondence is exact `component_key`, then `detail_no`/`segment_key`; fresh UUIDs are not authority mappings. No new helper/protocol is necessary for these finite point joins.

Preserve native NULL/missing/unsealed behavior at every new call site. The helper raises on NULL or unsealed input (:11–13), so use a guarded PL/pgSQL branch or explicit `CASE` around each point call, returning a NULL qualification when that point is absent/unsealed; do not rely on SQL AND evaluation order. For example Work.applied may genuinely be NULL before first native publication completes. Such a snapshot must retain PUBLICATION_PENDING, and a missing displayed revision must retain unavailable projection, rather than acquire a new treatment exception. A present sealed treatment whose own proof fails still fails closed; never fall back to a raw link.

## 1. Workbench native members

Exact current signature `private.bpay_next_workbench_work_members_v1(uuid,uuid)`; parent `10102026_1321_banking_pay_next_workbench_removed_source_classification_v1.sql:5–115`.

Resolve the supplied sealed Source revision's approval once, and point-join that approval `ar` plus its native line `aa` by the supplied actual line's `component_key`. Preserve requested `p_revision_id`, actual `a.id`, approved segment IDs and native quantities in the output.

* `source_books:19–28`: keep actual `r`/actual `a`; change binding `v.approval_revision_id=r.id` (:24) to the positively qualified approval ID. Use canonical event `ar.source_event_id` for the initial-TSFIN predicate (:28), with actual kind/head/financial fields still equal by the foundation. Keep binding version/currentness and the bound-snapshot call.
* `parents:39–52`: preserve `p.applied_revision_id=p_revision_id`, actual Position A versus actual line SOURCE amount, original removed-line/classification fallback and every native R/H/source-channel equality. Change only the book approval comparison (:45) to qualified approval ID and the book line comparison (:46) to `aa.id`, including NULL for a genuinely removed canonical component. Keep actual source channel from `r`. The source-qualified predicate (:44) uses canonical event rather than treating event-NULL clones as non-Source.
* Keys, segments, target credits/holds and all parent/segment conservation remain exactly the current body (:29–38,53 onward). The approval point is not substituted into actual membership or paid-share joins. Do not expand the existing WORK-only book arm to unrelated component kinds as part of this change.

## 2. Bound Source snapshot

Exact signature `private.bpay_next_bound_source_snapshot_v1(uuid,bigint)`; current parent `06102026_1246_banking_pay_next_source_book_binding_v1.sql:585–688`.

Preserve original binding version, receipt/bundle, original publication/command/member/job and output approval IDs. Only pointer-to-original-approval comparisons change:

* I1 count branch: :634 becomes qualified root of exact Work.current and Work.applied equals `p.revision_id`; :641 becomes qualified root of the exact Position.applied equals `p.revision_id`. Keep :631–633 original publication identity/APPLIED and every original command/epoch/DONE/lease-clear check :635–640.
* HEAD count branch: :647 qualifies each exact member Work.current/Work.applied root against that member's original `m.revision_id`. Keep exact original receipt member and one/two original publication tuples :643–653, including original APPLIED/DONE. Never replace the receipt's publication with an own treatment publication.
* Retain actual Position membership: its applied ID must be the owning Work's actual current or actual applied ID, with each pointer's root qualified as above. This is a metadata/native execution applicability check, not an amount change. Do not add blanket actual-current=actual-applied here: inside POSITION_APPLY, a just-written Position can name the new treatment while Work.applied still names its predecessor.
* The root helper certifies own immutable publication/predecessor identity; original source-job DONE checks remain original source-job checks. Do not require the new publication already APPLIED, or even require its stored status already APPLYING at this point. Apply0016:180–200 writes Position and immediately calls collection before later publication/checkpoint updates. Capture and hold callers retain their own actual expected/current/applied/lease/Position eligibility checks; canonical equality does not bypass E2.

Leave book readiness/revision/native posting checks :655–665, original classification :666–667, PRESENT versus actual canonical REMOVED proof :668–676 and native SOURCE figures/output :678–687 unchanged. No bind/seed/book writer changes. An unrelated newer original Source approval fails the root comparison and retains PUBLICATION_PENDING/currentness refusal.

`source_binding_members_v1(uuid)` in the same file :11–64 needs **no successor**: it selects the original unique native event/head revision and its original receipt publication (:43–54); it does not demand Work.current equal that original when a publication already exists. Its no-publication predecessor fallback is an original admission branch, not an invitation to choose treatment history.

## 3. Initial certificate — current parent correction

Exact signature `private.bpay_next_initial_tsfin_certificate_v1(uuid)` is now final **`12102026_0005_banking_pay_next_source_tax_classification_v1.sql:632–730`**, not0018. Use this complete SP20 successor, preserving especially actual component classification/tax :722–724.

Keep :651 unique original event lookup, original TSFIN/physical/authorisation/head absence :648–669, original publication tuple/metadata-phase/DONE job guards :670–693, inventory/certificate/detail/policy qualifications :694–725 and returned original revision/command/certificate :726–728. At :661–662 only replace the tuple's `w.current_revision_id` authority operand with the qualified root of actual current, compared to original `r.id`; keep the Work's real identity and APPROVED state. Current original retains identical behavior. Own treatment retains the same positively qualified original Source authority. A new native original approval or incompatible physical/head state still refuses.

`private.bpay_next_initial_tsfin_bound_assert_v1(uuid)` remains current0018:187–219, calling this certificate and checking immutable original origin/basis/line/digest. It needs **no successor** when the certificate continues returning original IDs and its original certificate; do not recreate or mutate I1 evidence.

## 4. Source origin/application state

Exact signature `private.bpay_next_source_origin_state_v1(uuid,uuid)`; parent `12102026_1946_banking_pay_next_source_applied_category_read_v1.sql:6–164`. Preserve its 17-key reply and original public summary wrapper unchanged.

Separate canonical Source authority from **actual current application state**. For an exact expected treatment, retain strict current=expected. The special Source-category case is an expected **original** (root link NULL) while actual Work.current is a positively qualified own treatment of that exact original: the original Source producer returns its event's original revision even after manual treatment. Only this case may use current treatment `r` for application facts. Do not treat an arbitrary older treatment input as a canonical expectation merely because it shares a root.

This input distinction is proved by retained callers, not inferred: native category0512:161–170 locates the one original revision by `r.source_event_id=v_origin_id`, then calls I3 with that original; protected owner0345:82–90 does likewise. Their unchanged canonical event/inventory checks can consume the updated I3 state; neither caller needs a new history selector. Source paid page1112:130–135 instead enforces the supplied actual current/applied scope before invoking I3, so retain that actual-input branch rather than relaxing its caller guard.

* Facts :49–62: retain exact expected same-Work revision qualification; choose actual current only for that explicit original→own-treatment association. Keep `r.id/revision_no`, current/applied IDs, `p/c/cm/j` and own publication/predecessor/job facts as actual application identities. Select effective source event from the qualified original `ar`; head, financial, physical, inventory and source-kind fields remain preserved native metadata. Head/authorisation joins :51–55 use the canonical event/authority, not event-NULL treatment.
* Revision compatibility :67–73 retains actual current/revision-number/week/APPROVED/seal checks, with effective canonical event. Origin compatibility :74–91 retains all native HEAD or initial-authorisation/financial/version/inventory qualifications.
* Publication compatibility and APPLIED/PENDING :92–135 remain attached to the **actual** treatment publication/command/job. Do not map those IDs to the original SOURCE publication: queued own treatment must display PENDING, then APPLIED only after its real native completion. Keep SNOOZE phase/cursor correspondence and native ordered-backlog predecessor logic.
* Output :146–160 retains actual current/applied IDs and actual command sequence/application state, with canonical Source event/head/financial origin. This is lifecycle presentation; it does not establish accepted Draft eligibility.

Missing or incompatible points must retain existing unavailable/incompatible behavior, not fall back to a different event or historical revision. Avoid unconditional root-helper calls on NULL/unsealed revisions before the existing availability checks. No summary route, count, category clock, producer or authorisation writer change is needed.

## 5. Core query qualification

Exact signature `private.bpay_next_workbench_refresh_work_core_v1(uuid)`; current parent `12102026_2123_banking_pay_next_taxable_resolution_notes_v1.sql`.

After selecting the exact native displayed revision (:60–62), obtain its qualified approval only when it is a sealed Source/Protected revision; use that approval's event for :218–220. Otherwise event-NULL own treatment skips the native Source query gate. Keep actual `v_revision.id` and all existing complete/current/applied, accepted/native H/R, PAY_ON_HOLD, Future/Snoozed and priority logic. Gate still reads the actual unchanged physical Timesheet :242; scope :263–269 stays actual physical/version/Work identity; segment exclusions still receive the actual treatment revision :279. Preserve the existing future AVAILABLE-only classification and pre-query `v_base` for accepted/reserved portions.

`source_query_segment_exclusions_v1(uuid,jsonb)` final `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql:8–63` needs **no successor**: it uses unchanged kind/physical/Work tuple and exact actual segment key membership; it does not inspect raw source_event_id. Do not pass canonical revision instead: current treatment segment IDs/membership remain the selected boundary.

## 6. Durable need continuity across own publication

Exact signature `private.bpay_next_component_resolution_need_v1(uuid,uuid,uuid,uuid,text,uuid,boolean,date)`; current `12102026_2123_banking_pay_next_resolution_need_target_v1.sql:57–99`. Need producer :8–55 remains unchanged: NEED events keep exact EX08 member/native old IDs.

Keep current caller revision/line/segment inputs and latest Candidate post_metadata_version/target filtering :65–75. Replace only raw member matching :84–85 with:

1. Same Work and positively qualified **same approval root** for exact `m.revision_id` and requested `p_revision_id`; each own treatment must pass the foundation's own-publication/predecessor check. Unrelated native approval never inherits need.
2. Exact member line `ma` by `(m.revision_id,m.approved_line_id)` and requested native line by `(p_revision_id,p_line_id)`; equal `component_key`, with their immutable native payload equality supplied by the root qualification/seal contract. Preserve the event→member old revision/line join :79–82.
3. Both segment IDs NULL, or both actual native segment rows present under their respective revision/line and equal `component_key/segment_key`. Do not compare fresh segment UUIDs and do not allow NULL to match a real segment. The root/seal contract preserves native vectors.

Keep exact operation/member/classification/event version, bucket identities, six-row ambiguity guard and latest compatible/protected transition suppression. Active-decision lookup :88–90 still receives **requested actual** revision/line and current target/VAT/policy/date. Thus a saved bucket suppresses only its new mapped decision; unresolved siblings survive its own complete publication. Never select a later ACTIVE row across history or broaden to source-channel mismatch. This same bounded native mapping is required for ordinary own treatment too; it changes no producer or amount.

## Finite scope and later integration

These are the six named/current reader successors plus their existing native joins. No indispensable additional callee was found in the three examined helper paths: binding-members, I1 bound-assert and query-segment exclusions retain their current bodies. Security/search-path/config/ACL and closed return shapes stay original; new calls are to the existing2135 private invoker root reader only.

This plan does not finish W1 estimate inline qualification, capture/hold attachments or retained collection1934/1849/1822. They remain mandatory subsequent matrix entries and require their own exact source review. Preserve current root metadata at observation without mutating original Source receipt/book, seed, bind, producer, financial formula or accepted IDs. Canonical approval does not override late Work/current revision refusal.
