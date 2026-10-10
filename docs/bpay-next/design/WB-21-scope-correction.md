# WB-21 scope correction: local segment choices and exclusion shape

Status: read-only card/checklist/source investigation, 10 October 2026. No product edits, runtime, SQL, tests or business calls. This note narrows earlier investigation wording; it does not approve the current in-progress1832 implementation.

## Local choice scope

WB-21's actual Build at `02_WORKBENCH.md:843–848` names one bounded local intent: ALL_MATCHING/NONE and exact **Candidate / Work / component / Case** overrides, IDs and booleans only. Its required D15 is the local choice store/sequencer. Required D05/D06 are the original approved-line component append/seal owners; no independent segment-selector deliverable appears in any of the23 WB-21 rows.

WB-25 at `02_WORKBENCH.md:952–985` retains the existing `expected_revision_id` + `approved_line_id` SUBSET path, with `componentSubsets` for day and expense/Additional line ticks. Its D12 explicitly owns exact WORK/SUBSET controls. R2 WB-25 (`review/R2.md:240–242`) says “no guessed UUID or new selection mechanism.” WB-23's sequence sends these SUBSET owners before exclusion/matching; it does not introduce another independent segment boolean override.

R2 `addendum/R2_EXACT_CONNECTIONS.md §1:13–23` requires actual approved/selected/run segment membership to preserve **snooze** contribution attribution under the existing `WORK:DAY` parent. Its selected segment disposition is INCLUDED/SNOOZED, derived from genuine WB-36 control and accepted revision. WB-36 is its sole snooze schema/command owner, WB-08 projects it and WB-16 consumes it. It does not name a separate user `segment_key + selected:boolean` picker/store alongside component selection.

**Narrow correction:** remove the newly introduced independent `segments[]` intent, `chooseSegment` API and generic per-segment override acceptance from this WB-21 store/descriptor. Keep the named Candidate/Work/approved-line component/Case choices. Retain existing genuine SEGMENT_PAGE/seal infrastructure and automatic complete stable segment membership required by WB-36: removing a speculative UI choice must not remove accepted snooze certificates, same-day sibling conservation, signed recovery membership, or segment control capture.

The initial FE draft read at `js/bpay-next-workbench.js:685–708` added `segments:[]`, schema `{candidate_id,work_id,expected_revision_id,segment_key,selected}`, `chooseSegment` and freeze filtering. The current1832 SQL descriptor read still permits this additional array and generic override. Those are the exact finite correction seams. No screen needs to be invented now to justify the extra type.

This supersedes earlier investigation phrases “component/segment override” or component/segment local subset choice wherever they imply a new independent local segment checkbox. Those notes' requirements to retain actual accepted WB-36 child membership remain valid. No change to WB-36 owners/producers or Rule B is requested.

## Exact exclusion relation

WB-21 D14 requires `private.bpay_next_run_selection_exclusion`; WB-23 D05 is context and D06 owns later complete matching/exclusion enumeration. The exact WB-23 card Build requires these native row fields:

```text
run_id, page_no, work_id nullable, candidate_id nullable,
actor_user_id, created_at_utc
exactly one of work_id / candidate_id
unique(run_id,work_id), unique(run_id,candidate_id)
RLS + service-owner policy
```

The inspected1832 migration (SHA256 below) instead has `exclusion_kind`, `member_id`, **nonnull** candidate_id for both kinds, no page_no, no actor_user_id and no created_at_utc. Its discriminator PK supplies distinct identity deduplication, but it is not the named shape. In particular a Work exclusion stores both Work and Candidate, conflicting with the card's explicit exactly-one pair; it also loses the native page/actor/time provenance.

Use the exact declared fields/uniques. Work rows have work_id only; Candidate rows have candidate_id only. A check such as `num_nonnulls(work_id,candidate_id)=1` avoids SQL NULL escaping the XOR. Existing native tables provide genuine FKs: Work UUID → `bpay_next_work(id)`, Candidate UUID → `public.candidates(id)`, actor UUID → `public.tms_users(id)` (same existing run actor authority;1324:420). The owner supplies actor from the locked accepted run/Office authority, never trusts a new actor field from page input. Current time is server-owned. A Work's Candidate for precedence is read from its genuine immutable Work/Candidate relation, not stored in the exclusive Candidate field.

The extra discriminator/member UUIDs are unnecessary for the named semantics. If retained solely as internal convenience they must not replace the exact nullable keys, provenance fields or required uniques. Smallest correction is the card's actual schema.

An independent immutable `selection_exclusion_page` receipt is justified because a **zero-ID page** has no native exclusion rows. Keep ≤100 combined IDs, exact original ordered body, `(run_id,page_no)` replay/conflict, and accepted descriptor binding. Its receipt cannot be inferred from row count or an exclusion's existence. An empty receipt advances actual page coverage, not a fake excluded member.

If CREATE prepopulates accepted exclusions, explicitly distinguish accepted/planned page assignment from an **acknowledged upload**; do not invent page0 or claim EXCLUSION_PAGE replay from mere prepopulation. Otherwise materialise rows from the already accepted bounded descriptor during actual EXCLUSION_PAGE. Either approach must supply the true stable page identity and no later live inclusion decision.

WB-23 additionally requires EXCLUSION_PAGE closed once first MATCHING_PAGE/seal exists, identical replay and changed-body conflict; MATCHING skips Candidate exclusions and Work exclusions plus already-chosen SUBSET Work. Candidate exclusion dominates conflicting Work/component/Case choices (R2-F06). These facts belong in the existing named intake successors, not a new selection protocol. This note does not declare WB-23's later global controls or matching UI completed by WB-21 schema support.

## Sources re-read

Full WB-21/WB-23/WB-25 cards, all their exact checklist ITEM/DELIVERABLE/CONTEXT rows, the seven-step WB-S interface, R2-F01/F06 and per-card findings, R2 exact connections§1, current1832 schema/descriptor seams and FE local store were read.

| Source | SHA256 |
|---|---|
| Approved `02_WORKBENCH.md` | ef974a5f7670ce1ffeb93952a9d2887c36d9b5380a14e99111c7d5bd12d5a409 |
| Approved `review/R2.md` | ef775ebc4d8e3396c2155142ff82f3d6d1e78c92015e982e088b97990dddf521 |
| Approved `addendum/R2_EXACT_CONNECTIONS.md` | 52dde331f580d19e2a80c3866b79a0625f080d674b08d0d929e1ac72c048f125 |
| BUILD-BE checklist | 8a1553459c36b1885b9471624dc64e2cbc53a63e99b3c5ee8e4a27494ed2f240 |
| In-progress1832 membership migration read | 2f11320a901e2e7b054dc36342637ef5f9df04536f26d2df3c077a3b3fe37624 |

Rule B supersedes the historical F-25 accepted-input/version producer language in R2. This correction introduces no parked resolver, financial snapshot, Source/query producer change, money owner or command kind.

## Fresh saved-source follow-up

The builder concurrently corrected the FE store. Fresh `bpay-next-workbench.js` SHA256 `b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a` now permits only mode/Candidate/Work/component/Case keys, has no chooseSegment, and filters Candidate exclusions across the four named arrays. The independent segment-choice finding is closed for that FE seam. Fresh1832 SQL descriptor SHA256 `415983e72b4684d2dad1d098d7406465c9559242ae1c632e38cc34fcc4ea113d` still contains the extra segment intent, and the migration hash/exclusion shape above remains unchanged. This is a partial source follow-up, not approval of the entire implementation.
