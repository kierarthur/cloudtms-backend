# SP-13 D01 exact gate verification census

10 October 2026. Independent read-only review of the current BUILD-BE source against the full SP-13 card (`05_SOURCE_AND_PROTECTED_PAY.md:457–485`), current checklist and its referenced Source/critic requirements. **D01 source verdict: PASS; no confirmed mismatch requiring a gate change was found.** No SQL, business, tests, runtime, fixtures or hosted operations were executed. D02/D03 display-page implementation and all joined acceptance remain separate; this census does not complete them or revive parked F25.

## Current deliverables and boundaries

The three deliverables are D01 exact Query ScopeV2/ReceiptV3/PAY_QUERY_STATE_V3 gate verification; D02 the new bounded private open-query display page; D03 its typed WB-07 interface record. Dependencies are F05, F06 and SP12. The existing five-key gate carries no display reason, actor, time or cursor; D02 must not change its digest/answer. Only the already named Final/Correct Final callers may clear queries after their own successful fresh CURRENT CAS.

Current single-definition reader owners remain the seven SCX-044 files below. Registry-selected `private.weekly_source_pay_query_facts_v1(uuid)` and `private.weekly_source_covered_hours_incident_v1(uuid)` physically come from `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc`, included by the final root `05102026_0308_weekly_source_pay_query_gate_v1.sql:9`. Current later-Final helper final is `05102026_0428_weekly_source_manual_review_commands_v2.sql`. The actual Final and Correct Final entry definitions remain the original `15092026_1534` files, not stopped F25 successors.

## Requirement-to-source verification

| Requirement | Actual source proof | Verdict |
|---|---|---|
| SRQ-016 raw root booking identity | `0057:28–44, 62–78` reuses actual canonical root resolver and emits stored `v_root.booking_id`, never the trimmed lookup value. `17092026_0200_weekly_source_rotation_authority_v1.sql:456–602` refuses raw/trim collision, ambiguous canonical/current cardinality and duplicate/null family versions. | PASS |
| SRQ-017 exact nine-field ScopeV2 | `0057:74–78` emits exactly the nine keys listed below. Canonical/current WEEKLY/HOURS, ordinary/non-adjustment, unarchived/unrevoked root and exact Contract Candidate/Client are required at 28–46. | PASS |
| SRQ-018 legitimate no-exceptional-family scope | `0057:57–73` allows no actual family and emits both family fields NULL. If a family exists, exact root/raw booking/Candidate/Contract/week and positive bound version are required; duplicate trimmed family ownership refuses. Ordinary common-HEAD Source is not forced to manufacture exceptional identity. | PASS |
| SRQ-019 Source domain and real ordinary Contract-week | `0057:47–56` always invokes `_candidate_expense_source_family_v1(root)` before testing self_bill OR classifier, preserving its actual exceptions. Exactly one non-adjustment `contract_weeks` additional_seq=0 row must bind this root/Contract/stored configured week. No scope returns NULL into the factual reader's explicit unavailable reply. | PASS |
| SRQ-076 / SRQ-314 closed five-key gate and real errors | `0308:13–20` is STABLE SQL SECURITY DEFINER, logical owner postgres, revoked from public/anon/authenticated/service_role. It subtracts only the two factual arrays. `qualified_coverage.inc:21–27, 168–175` gives exact OK/scope-unavailable/evidence-unavailable replies; null typed root raises 22023. No catch-all handler converts permission/identity/constraint errors to clear or unknown. | PASS |
| SRQ-077 all relevant owning queries across cutoff periods | `qualified_coverage.inc:30–43` includes all OPEN manual rows for exact Candidate/Client/Contract/root workweek, not the currently selected upload. Hours at 47–81 use incident-owned revision 1 for relevance and highest actual comparison revision for the current-tail check; missing event/current/owning comparison or mismatched scope marks evidence unavailable. Relevant siblings cannot disappear because today's provisional upload omits them. | PASS |
| SRQ-078 / SRQ-313 exact versioned digest and projections | `qualified_coverage.inc:30–34, 66, 160–175` sorts both arrays by native UUID, retains the exact manual/hours projections below and hashes canonical JSONB text converted to UTF8 using SHA-256. The object has only schema_version PAY_QUERY_STATE_V3, scope, manual_reviews, hours_incidents. Display/communication/notification fields and mutable Banking application state are not digest operands. | PASS |
| Missing-Timesheet issue remains non-blocking | `qualified_coverage.inc:157–158` excludes `CANDIDATE_TIMESHEET_MISSING` from the positive hours blocker predicate while retaining its incident in the complete digest. Malformed/missing comparison evidence still refuses, not fabricated clear. | PASS |
| SRQ-079 / SRQ-312 latest qualified exception and exact one receipt | `qualified_coverage.inc:84–153` selects actual highest event_sequence for exact family/work event and requires WAIT plus that event's evidence approval. Family-event sequence uniqueness is retained in `15092026_1534_weekly_source_plan6_schema.sql:2243`; there is no latest-timestamp or arbitrary-first-row choice. Exactly one non-null financial target-generation link is required; genuine generation-less WAIT audit rows must prove unchanged vectors and canonical hashes. All three lane owners are called; related partial/extra/conflicting lane records cause EVIDENCE_UNAVAILABLE. Exception object is exactly six keys below. | PASS |
| SRQ-311 SOURCE_LOCAL lane | `0114:47–150, 177–377` binds actual scope/approval/event, exact Local generation/receipt/request and approval's creation run. Local state is COMPLETE or PENDING_FREEZE, request RETIRED with no C1 fields, run COMPLETE with exact result hash. Actual sealed original root/version, typed Local origin/qualification and real pending or committed publication are qualified. It emits exactly six non-null lane keys at 377–379. No current Final substitutes for sealed Local origin. | PASS |
| SRQ-311 RETAINED_C1 lane | `0245:30–145` requires exact scope/approval/latest WAIT, one genuine C1 request with PUBLISHED publication id/revision/hash, original physical/financial pins and original complete run/checkpoint/step evidence. A Local receipt prevents C1 substitution. Exactly nine keys are emitted at 146–150. No new C1 publication is performed. | PASS |
| SRQ-311 DIRECT_NEXT lane | `0258:18–117` binds exact approval creation run, receipt family/generation, work/revision composite, immutable Source manifest and publication hashes, deterministic Source command, one POSITION_APPLY Candidate member and accepted bound version <= real current bound. State COMPLETE is derived in its twelve-key projection at 23–31, not read from a fictitious receipt-state column. The owner requires exactly one qualified result at 118. It neither requires job enrolment nor hashes QUEUED/APPLYING/APPLIED. | PASS |
| SRQ-308 legitimate sibling-superseded generations | `0114:137–147`, `0245:53–62`, `0258:64–73` accept real PUBLISHED or SUPERSEDED generations while checking their retained vector/result/publication proof. `qualified_coverage.inc:90–120` follows the immutable approval's financial-generation link rather than today's current_generation; later generation-less unchanged WAIT rows remain audit evidence. No invented UUID or financial-application digest replaces the original proof. | PASS |
| SRQ-071 / SRQ-301 exactly two fresh-CAS clear placements | Final `15092026_1534_weekly_source_finalisation_v1.sql:2688–2736` performs actual cycle/report CURRENT CAS, checks NOT FOUND, then the sole clear call at 2736 before original NEXT selector 2742. Correct Final `15092026_1534_weekly_source_correct_final_source_v1.sql:2007–2039` performs exact old→new cycle/report CAS with version/pointers and checks NOT FOUND, then its sole clear call at 2039 before selector 2046 and original completion receipt below it at 2049–2074. | PASS |
| Clear only actual later contained event, never old replay | Final exact CURRENT replay returns at 1655–1668 and PREPARED returns at 2649–2665, before its clear. Correct Final APPLIED exact actor/key/hash replay returns at 1644–1651, before its clear. `0428:249–323` is private/owner-only and explicitly relies on this closed caller CAS path; it checks real CURRENT pointer/actor, exact contained work event/Candidate/Client/Contract/date and immutable opening authority. Same opening Final/upload, missing opening anchor or missing exact Client manifest does not clear. It has no xmin/GUC/timestamp fresh-CAS certificate and cannot be called as a service RPC. | PASS |
| SCX-044 selected-duty witness | `0645:5–79` is STABLE owner-only factual read: exact family/event/action-cycle/agency/configured workweek, one selected client source, exact current row with genuine Final precedence. `0645:86–317` conservatively checks every potentially relevant non-superseded current import across cutoffs, rejects incomplete/failed/ambiguous mappings, and records deterministic native-row census. `0645:319–440` proves real NHSP positive/full-negative history or exact roster cancellation/confirmed omission, or narrowly genuine OFFICE_PROTECTED_SHIFT with no import/source history. At 442–447 it returns the retained witness and canonical basis SHA; no invented zero/Final/payment or query mutation. | PASS |

## Exact shape census

ScopeV2, exactly nine keys (`0057:74–78`):

`root_timesheet_id, family_booking_id, target_family_id, candidate_id, client_id, contract_id, week_ending_date, root_version, family_bound_version`.

The root/family version fields are decimal text; the two exceptional-family fields are both NULL only for genuine no-family scope. `family_booking_id` is the raw stored string.

Gate, exactly five keys (`0308:17` and `qualified_coverage.inc:25–27, 168–175`):

`ok, code, scope, blocked, query_state_sha256`.

- OK: qualified scope, boolean blocked, lowercase 64-character digest.
- WEEKLY_SOURCE_PAY_QUERY_SCOPE_UNAVAILABLE: scope/blocked/digest all NULL.
- WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE: qualified scope retained, blocked/digest NULL.
- Invalid null input: real 22023 exception; other actual errors propagate.

Manual digest projection, exactly nine keys (`qualified_coverage.inc:30–34`):

`id, source_group_id, source_cycle_id, work_event_id, candidate_id, client_id, contract_id, work_date, state`.

Hours digest projection, exactly thirteen keys (`qualified_coverage.inc:160–166`):

`id, source_group_id, source_cycle_id, work_event_id, candidate_id, client_id, state, work_date, current_comparison_revision_id, comparison_contract_id, issue_family, material_comparison_fingerprint, latest_qualified_exception`.

Nullable exception, exactly six keys when qualified (`qualified_coverage.inc:151–153`):

`family_id, bound_version, event_sequence, state, evidence_approval_id, accepted_receipt`; state is WAIT. Version/sequence are decimal text. ReceiptV3 is exactly one of:

| Lane | Exact keys |
|---|---|
| SOURCE_LOCAL (6) | `lane, publication_request_id, orchestration_run_id, generation_id, request_sha256, receipt_state` |
| RETAINED_C1 (9) | `lane, publication_request_id, orchestration_run_id, generation_id, request_sha256, c1_publication_id, c1_head_revision, c1_receipt_sha256, receipt_state` |
| DIRECT_NEXT (12) | `lane, orchestration_run_id, generation_id, work_id, revision_id, command_id, agency_sequence, accepted_family_bound_version, prepared_request_sha256, publication_request_sha256, source_manifest_sha256, receipt_state` |

No lane emits timestamp/message/application-status keys. Receipt-state values are respectively COMPLETE/PENDING_FREEZE, PUBLISHED, and derived COMPLETE. Related partial evidence cannot silently become a nullable exception and clear a genuine incident: the factual owner returns evidence unavailable.

The separate `0645` selected witness has exactly seven top-level keys: `schema_version, scope, kind, basis_kind, source_proposal, basis, basis_sha256`. It is not the five-key gate or part of a new banking input protocol. Its genuine CURRENT_ROW and CERTIFIED_ABSENCE evidence remains a distinct existing Source owner; it cannot clear a manual query merely by claiming absence.

## File-specific SCX-044 source record and fresh hashes

These are saved-source hashes only, not database-definition/installation or runtime acceptance proof. Every listed reader retains its original stable/security/search-path/owner/ACL declaration. No grant to service or browser roles is added.

| SCX-044 file / principal definition line | SHA-256 |
|---|---|
| `05102026_0057_weekly_source_pay_query_scope_v1.sql:9` | `18317d4152fa8e8df4c17ac7895046453a3e26d534b10c45b1fafb50b91a983d` |
| `05102026_0114_weekly_source_pay_query_evidence_v1.sql:12` | `bb6290374ea7857d1ec995565c78267ff9dc761b7d7f4fe05f460ed2903f9b2f` |
| `05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql:11` | `e7e7717499f6e081fb2d2a5445167814efeb1c98ec6848cf828fd874b549a9bf` |
| `05102026_0258_weekly_source_pay_query_next_receipt_v1.sql:11` | `9888caaf5720e1078340474354a41cf55b0d90ed721b1366d24f133d9ec77978` |
| `05102026_0308_weekly_source_pay_query_gate_v1.sql:13` | `fb70e23a80c7686606b5cbf8f17e2878ca37224f5752067d903e9a253a3cbfa8` |
| `05102026_1417_weekly_source_coverage_support/qualified_coverage.inc:7,181` | `45a8a0bc2364318d734c1ec678d79fa617d549a62c3a532ef851c928773aee6e` |
| `05102026_0645_weekly_source_selected_source_witness_v1.sql:5` | `1d455ca1a9088ce591b046e9c4659b5994967a5f901808faf5ab7fbd1ce04c8d` |
| `05102026_0428_weekly_source_manual_review_commands_v2.sql:249` (clear helper) | `1e9f16241ffa5e9249a7e8bf796693b81b633c2268cd1864147223451628e8d6` |
| `15092026_1534_weekly_source_finalisation_v1.sql:1194` (Final owner) | `19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b` |
| `15092026_1534_weekly_source_correct_final_source_v1.sql:1522` (Correct Final owner) | `55786d5a85f69447054677f02df2406c710438987539c8f21efbe0ebbec552f5` |

The actual runtime Monday query block/clear, other Candidate isolation, no unique ordinary Contract-week, common-HEAD/no-family clear, raw booking spacing, wrong-lane receipt refusal and replay that cannot clear a reopened query remain **NOT_RUN**. D01's source census is complete; do not treat it as D02/D03 or whole SP-13 acceptance.
