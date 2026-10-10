# SP-07 next-item source investigation

10 October 2026. Independent read-only source investigation for plan sequence 42. No SQL, tests, runtime, fixtures, hosted reads or business actions were executed. This is a build-scope/design record, not joined acceptance evidence.

## Scope and smallest complete change

The full SP-07 card is `BPAY_NEXT_BUILD_PLAN_20261009_codex_r3/05_SOURCE_AND_PROTECTED_PAY.md:277–303`. The current checklist has exactly three deliverables: D01 the existing `04102026_1257_weekly_source_local_protected_decision_v1.sql`; D02 the existing private six-argument pending-release apply owner; D03 the missing explicit T-13 legacy-pending outcome mapping. Dependencies are F05, F06, F18, SP06 and SP12. The editor chain is **SP12 → SP07 → WB02 → WB05 → SP15 → SP09 → SP28**; SP-07 is position 2 of 7.

**Recommended product scope: reuse the existing SQL owners unchanged and write the finite D03 mapping/closing record.** No confirmed Source-definition mismatch requiring a successor was found in the inspected paths below. Worker and Screen have no new SP-07 deliverable; editor confirmation/refresh belongs to SP06 and the already completed SP12 retention seams. Do not import F26, revive F25 producer/version/certificate/protocol/resolver work, create a NEXT pending ledger, or alter Rule B selection consumers. Rule B continues to mean actual expected revision, captured Candidate, saved snapshot and existing PIN.

The checklist's historical D02 evidence caveat remains valid: a capped hosted ledger scan that did not observe the exact `17092026_0700` unit receipt neither proves non-installation nor proves installation through an unrelated reassertion receipt. This source investigation does not replace that missing receipt evidence or authorise a hosted probe.

## Exact owners and source evidence

| Object | Current physical definition / relevant lines | Source conclusion |
|---|---|---|
| `public.weekly_exceptional_pay_complete_local_v1(jsonb)` | `04102026_1257_weekly_source_local_protected_decision_v1.sql:42–579` | Existing closed five-key input; scoped `APPROVE_PROTECTED_PAY` authority at 115; immutable receipt replay before fresh admission/locks at 103–123; genuine Source admission at 125. No SP12 successor of this owner was found because this existing key is already present. |
| Unauthorised Save | same unit:260–301 | Actual unauthorised current financial snapshot is updated through the existing snapshot writer, retained as `SAVED_UNAUTHORISED_LOCAL_V1`, with exact financial/detail digests. Result requires first Authorise. This branch does not mint Banking WORK or claim authorisation. |
| Effective later local Save | same unit:235–245, 302–394 | Real authorised-root evidence is required; the later local lane requires actual NEXT ownership. The five typed origin fields are captured under the original PREPARING/current-transaction receipt at 362–373, then the existing immediate common-HEAD owner is called at 382. |
| Nullable typed origin columns | `05102026_0155_weekly_source_local_publication_origin_v2.sql:10–14`; same-named repeatable guard | Existing columns are all-null or all-present, with real bundle/revision FK and 32-byte digests. The guard only permits original same-transaction PREPARING sealing; subsequent updates freeze snapshot/origin tuple and completion time. Its only later state change is `PENDING_FREEZE → COMPLETE`. No backfill or JSON-derived origin is appropriate. The unauthorised first-Save branch legitimately has no common bundle and leaves these fields NULL. |
| Immediate publication versus legacy pending | `17092026_0300_weekly_source_entitlement_publication_v1.sql:3000–3115` | Uses the existing admission key and freeze census. An actual FROZEN legacy scope returns the existing verified pending bundle. A RELEASABLE scope calls the common core immediately. |
| Freeze census | `17092026_0400_weekly_source_freeze_census_v1.sql:136`; legacy item joins at 433–464 | Enumerates actual `public.pay_batch_items`, `pay_batch_candidates`, `pay_batches` and their legacy obligation evidence. It does not enumerate `private.bpay_next_*` Drafts. SRQ-166 therefore has a real source basis: a genuine NEXT Draft does not itself create Source PENDING_FREEZE. Existing unrelated legacy exposure/integrity may still refuse or defer and must retain its own disposition. |
| `private.weekly_source_pending_entitlement_release_apply_v1(uuid,bigint,bytea,text,uuid,uuid)` | `17092026_0700_weekly_source_pending_entitlement_release_v1.sql:1116` | Existing admission at 1211 before the pending-row lock at 1232. It rechecks the actual saved decision, current proposal/HEAD and positive RELEASABLE census before calling the common core in DEFERRED mode at 1445. Permanent stale/conflicting evidence is superseded or sent to manual review, not blindly adopted. |
| Deferred Local completion bookkeeping | `04102026_1257_weekly_source_local_protected_decision_v1.sql:474–578` | AFTER actual pending-state release, verifies the actual common publication receipt, bundle/revision/digest/member root, pending generation and family tuple. It completes the same Local receipt and updates the same already COMPLETE orchestration run's result fingerprint at 537–561, preserving run identity and completion time. The original Local completion step remains unchanged and is checked as pending audit evidence; it does not repeat a financial tail. |
| Saved-status readers | `05102026_0624_weekly_source_local_saved_status_v1.sql`; `04102026_2338_weekly_source_candidate_saved_local_hours_v2.sql:86–163`; Candidate approval reader `03102026_1540_candidate_approved_hours_visible.sql:8–44` | Exact COMPLETE Local receipt, canonical generation/snapshot hashes and completed-run fingerprint are required. Candidate saved-hours returns the explicit saved unauthorised basis, not a payable entitlement. Pending-family publication keeps the previously effective generation/HEAD. |
| Banking first-authorisation boundary | `04102026_0345_banking_pay_next_protected_source_v1.sql`, `private.bpay_next_protected_require_authorised_root_v1` | Requires actual live root authorisation and exact root family/version. Unauthorised Save refuses with `BPAY_NEXT_PROTECTED_FIRST_AUTHORISATION_REQUIRED`; a saved display basis does not grant financial authority. |

These single-definition Source owners need not appear as multi-definition `chains` entries in `final-definer-closures.json`. Their absence there is not a reason to manufacture replacement definitions. The shared common core's actual frozen final remains `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql`; preserve it. The already current SP12 successor covers its own named missing seams, not a replacement Local completion owner.

The existing `05102026_0355_weekly_source_local_flat_origin_cutover_v1.sql` is a one-time rollout fence requiring an empty older Local common-origin protocol. It is not a standing trigger or a future publication refusal. Its checks explicitly prohibit rewriting historical origins/pending requests to make installation pass; preserve its migration bytes and receipt.

## Finite T-13 / J09 / J12 disposition map

The requirements register names `S4-J09-PENDING-PROTECTION` and `S4-J12-SAVE-CLOSE/-PENDING/-REPLAY/-UNKNOWN`; C12 expressly names the J09/J12 legacy PENDING_FREEZE cases. This map records the real source transitions and their clean-NEXT replacements. It does not assert that any retained application bundle exists or that a runtime case passed.

| Pack/source case | Clean NEXT outcome | Genuine pre-switch retained scope and F18 disposition |
|---|---|---|
| J12 Save before first Authorise, including positive six hours or certified zero | Actual snapshot + COMPLETE Local receipt; saved approved display basis; no Banking WORK. Later first Authorise remains the existing distinct authority. | No common pending bundle is invented for this branch. A PREPARING receipt is an integrity condition, not successful Save. |
| J09/J12 later Save behind a genuine NEXT Draft | Existing immediate common HEAD; original Draft selection/lines/holds stay fixed. No Source PENDING_FREEZE and no new freeze row. | The old legacy-freeze predicate is unreachable **from the NEXT Draft itself** because the census reads legacy pay rows. Do not fabricate an old batch to make this case run. |
| J09 pending protection / J12 Save behind a real legacy frozen scope | This is not a new NEXT-Draft path. | Retain the actual saved Local receipt/bundle and previous effective generation. F18 `SOURCE_LOCAL_PENDING` and `SOURCE_PENDING_BUNDLES` both classify genuine pending mapping as blocking. Complete through the genuine LEGACY release owner before switch where applicable, or record an owner-approved exact alternative disposition of an actually observed bundle; no quiet assumption of absence. |
| Actual saved pending decision remains current when legacy freeze is positively released | Conditional exact DEFERRED common-HEAD adoption through the existing apply owner; later Local-origin publication requires actual NEXT ownership. | Positive saved-request/current decision/census and exact real publication receipt are required. Local bookkeeping completes the same receipt, updates the already completed run and retained request result, and preserves the original pending step. No second command or financial tail. |
| Saved pending decision/proposal/HEAD no longer current | Existing exact refusal/superseded/manual-review disposition; do not adopt obsolete approval. | Pending/manual-review exposure stays explicit in F18 until its real owner closes it. A SUPERSEDED ledger row alone is not proof that every companion Local receipt is COMPLETE. |
| J12 pending Save confirmation | Editor closes/refreshed parent under SP06/SP12; previous approved/Stage basis remains until exact release becomes effective. | `SAVED_PENDING_FREEZE` / PENDING is acceptance of a saved decision, not publication or financial acceptance. |
| J12 replay or unknown response | Same actor, literal request/hash/idempotency key and `:local-complete` owner; exact retained result returned. | Before release replay returns retained pending result. After positively proved release replay returns the same receipt's completed released result; never duplicate the decision/run. Unknown stays unknown until that exact retained owner establishes outcome. |
| NEXT cancellation of the old frozen selection | Releases only its original holds under its existing owner. | It does not release a Source pending bundle or activate the retired legacy freeze engine. Any genuine legacy pending bundle remains independently counted by F18. |

F18 category source is `09102026_2109_banking_pay_next_cutover_readiness_page_v1.sql:566–573`: Local PENDING_FREEZE is `SOURCE_PENDING_MAPPING`; Local PREPARING is `INTEGRITY_REVIEW`; pending bundles in PENDING/RELEASING/MANUAL_REVIEW remain `SOURCE_PENDING_MAPPING`. An unrelated retired C1 request/COMPLETE orchestration run is not a substitute closure witness.

## What can be closed, and what remains NOT_RUN

D01/D02 can be closed for current source reuse after the builder records the exact installed/release authority evidence allowed by the current phase; D03 can be completed with this explicit mapping and the builder's recorded choice for any positively observed retained bundle. No such bundle or counts were observed here. OD-F5/F19 is conditional on actual retained exposure, never a required invented old fixture.

The card's in-app/queue/Worker acceptance remains **NOT_RUN**: six hours and certified zero shown as saved Approved before Authorise, zero Banking WORK until real Authorise, exactly one six-hour WORK afterwards, later local typed receipt and replay, previous basis while actual legacy pending, exact-current release and same-run fingerprint. Do not relabel this source investigation as those checks. Normal permitted source gates may confirm exact defining SQL/ACL/field names without exercising those business paths.

## Source hashes at investigation

| Saved source | SHA-256 |
|---|---|
| 04102026_1257 repeatable | `2a4f0d619ec2800f3d5a880459d0c0421f41c890bb6706750eb0464cab24fb03` |
| 17092026_0700 pending-release repeatable | `177400e1d5f1be23e789ef4cd3596cdac1f9860fe94602a5a031bdd1142488c6` |
| 05102026_0155 migration | `2c065e71ed09ef1d393f30f6518776379c1ea14e43e50c861d624a69ba7391e1` |
| 05102026_0155 guard repeatable | `87ada3dcd6e1f33484acc2d09f67e7b164a23ccaf94689479f6e43c816a5e70b` |
| 17092026_0400 freeze-census repeatable | `5e96a3735e364abb41f712f8ab162daa364893d6c3480b84075d9d63dd4d379f` |

**Source-scope verdict:** existing named owners appear to satisfy the preserved SP-07 behavior. Smallest next build is the explicit mapping/completion record, not a new SQL producer. Installation provenance and actual joined acceptance remain separate, honest outstanding evidence.

Re-read correction: the initial investigation incorrectly described release as updating the original completion step. Actual `1257:537–566` updates the run fingerprint and retained request only; `0624:415–455` positively checks the original unchanged pending step against the released result. The two descriptions above are corrected accordingly. This was a documentation error, not a SQL defect or source change.
