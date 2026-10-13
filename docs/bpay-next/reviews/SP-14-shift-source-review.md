# SP-14 independent shift source review

**Final verdict: SOURCE PASS — S14-01 closed; no remaining essential finding.** Read-only inspection on 11 October 2026 of BD122, the full owner/checklist boundary and current saved `11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql` plus its additive migration. No product edits, SQL execution, business data, fixtures, tests, hosted actions or runtime acceptance.

## S14-01 — CLOSED: first Authorise manual exception qualifies its typed incident

Initially reviewed2223:1987–1991 exempted an OPEN manual query when any same-event/Candidate/Client/Contract OPEN incident's current comparison pointer reported SOURCE_HOURS_DIFFER. It did not require the comparison's incident_id to equal that incident, the pointer to be the actual latest comparison, or incident.source_group_id to equal manual_review.source_group_id. The native schema's current_comparison_revision_id FK references comparison ID alone (`15092026_1534_weekly_source_plan6_schema.sql:1474–1476`); it does not prove that closure. A foreign/stale comparison or different Source group's incident could therefore exempt a manual blocker. The canonical factual query owner deliberately treats those comparison/scope failures as unavailable.

Required narrow correction: qualify the incident-owned actual current comparison and matching Source group before granting this new exemption, using the existing native identities/indexes. Unknown/manual/unavailable evidence remains refused. Preserve the original canonical family locks, protected permission, CANDIDATE_TIMESHEET_MISSING exception and generic-authorise/publication tail.

Closure independently re-read: the saved successor now requires matching source_group_id, comparison.incident_id=incident.id and absence of a newer native revision. The same event/Candidate/Client/Contract predicates remain. Foreign/stale/group-mismatched evidence cannot qualify this exemption. Canonical family/permission/generic-authorise/publication behavior is retained.

## Already inspected supporting behavior

The original-cut helper compares native factual scope/blocked/digest to the captured raw gate, refuses unsupported or unmapped effective blockers and maps exact native event keys to the expected approved revision. Original descriptor Create retains the array in its existing immutable snapshot; replay precedes new Source admission. Selected-segment INSERT consumes the retained array and preserves every original OR47 inventory member, INCLUDED/SNOOZED tuple and counters. Both captured segment and its guard derive effective NOT_SELECTED for a retained finite reason. The counter/parent/hold-origin financial clauses remain unchanged.

Exact predecessor comparisons completed for the eight other successor bodies and current generic Authorise: inspected deltas are the named shift-query support and separately authorized A3 pre-lock admission applicability. The six-argument guarded Create was compared against its actual six-argument1358 parent, preserving its PAYE guard/replay/descriptor logic; the five-argument wrapper is not its predecessor. All ten provenance parent file hashes currently match. Remaining closure checks are bounded source/provenance checks only.

Initial reviewed repeatable SHA-256: `9e0f00510d72b76ad78c5325834315bc64a6c76cb181a764ea5b113db74778e0`. Later repaired bytes require a fresh exact hash and verdict. Source compilation, installation, joined application, actual money outcomes and queue contention are **NOT_RUN by this reviewer**.

## Final source closure

| Boundary checked | Source result |
| --- | --- |
| Exact blocker census | PASS. Native factual arrays are complete; unsupported issue family, unmapped event, mismatched raw gate/scope/digest and untyped manual query return NULL. No partial array promotes a mixed unknown blocker to clear. Manual source-hours qualification uses actual same-event/source-group/Candidate/Client/Contract incident facts. Protected qualified exceptions remain native. |
| Exact event mapping | PASS. Only expected revision's actual approved segment_key equal to native work_event_id is admitted; no date/name/amount inference. Results are deterministic and deduplicated by exact segment/reason. |
| Original Create cut | PASS. Six-argument1358 parent replay, original descriptor, PAYE override and Source admission order retained. New array is captured with raw gate on the existing snapshot before asynchronous pages; original accepted acknowledgements do not read live state. Historical descriptorless seal remains its original cut path. |
| Selected membership and replay | PASS. All original ALL/SUBSET members remain selected; no new disposition. Retained reason is NULL or one of three finite codes. Existing INSERT guard matches it to exact original run/Work/revision/key; update/delete/truncate immutability, page replay and complete seal/count ownership remain. |
| Position BLOCKED exception | PASS. Only a nonempty qualified retained array on exact original run/Work/revision/root snapshot bypasses the existing whole-Work BLOCKED branch. Missing companion/UNAVAILABLE, invalid origin and unsupported unmapped Additional remain refused; no live Source read enters capture. The original pin owner, raw gate and digest are unchanged. |
| Effective segment exclusion | PASS. Capture and immutable segment guard both use NOT_SELECTED for retained query reason while preserving original INCLUDED/SNOOZED truth. Approved/realised/held balances, excluded positive, recovery, overhang, parent counters, origin and effect guards are unchanged. |
| Workbench fresh classification | PASS. Same native helper distinguishes the exact held segment from clean siblings; only genuine fresh positive Source WORK is affected. Ordinary/expense/additional/zero/negative/reserved/recovery and accepted observations retain original behavior. |
| First Authorise | PASS after S14-01 and final edge repairs. Original IS DISTINCT FROM CANDIDATE_TIMESHEET_MISSING is retained. The three-family exception requires comparison.incident_id, actual event Candidate/Client and latest native comparison, wrapped IS NOT TRUE so unknown fails closed. Typed manual matching owns its actual comparison/group. Unknown families/manual queries retain refusal; no query resolution or producer change. |
| Owner A3 supporting generic admission | PASS, exact source scope. Lock-free pre-read precedes business locks; group applicability requires known sheet_scope and line_type. Revoked/archive restoration and unknown/ambiguous applicability retain the admission key. The original native source-family/import/self-bill/active-authorisation checks also retain the key when Office group applicability alone says non-Source; exception remains conservative. Genuine non-Source ordinary/Daily applicability omits the key. Actual generic Authorise mutation, rate refusal, response and exception tail remain unchanged. |
| Migration/security | PASS. Two additive nullable fields on existing private relations, array/finite-code CHECKs, no new table/public reader/route/command. Existing RLS and immutable triggers retained; new helper is private security invoker with bounded existing owner caller, postgres ownership and browser/service_role EXECUTE revocation. |
| Exact parent/provenance comparison | PASS. All ten parent file hashes, ten original body hashes and ten successor body hashes match the provenance record. Complete function deltas inspected; only named shift-query support and owner A3 pre-read changed. Original five-argument Create wrapper is preserved as a caller, not incorrectly substituted for six-argument parent. No financial formula, Source producer, pin guard, parked protocol or queue rewrite. |

Final independently reviewed SHA-256:

| Saved evidence | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql | `b6ac0d7edac3f8c3ebf835125b8b1ff9c706e926bfe305e0aa850074db193089` |
| supabase/migrations/11102026_2223_banking_pay_next_shift_query_exclusion.sql | `939cfaf247eabcbcdb54495f18f08d86beaaaeed607147a25a8e202ff4db89aa` |
| design/SP-14-successor-provenance.json | `6554e31f276641074d65cdfc5487de473acfece99f7f211362bb2aefc6ae1889` |
| design/SP-14-owner-amendment.md | `5d25272f359b2dfb433bb2f83707b40b9b2a92cef1dbfd750c99fa982f775065` |

This approval covers saved source under BD122. Parent performs ordinary compile/catalogue/contract checks separately. All real financial outcomes, queued/joined application, contention/no-starvation/timing and in-app acceptance remain AUTHORED_NOT_RUN; no item/runtime PASS is inferred.

Final Authorise amendment independently re-read: known scope/type is required before group lookup can omit admission; the original missing-Candidate-Timesheet predicate and NULL-safe qualified three-family exception are retained. The final conservative source-ownership check reuses private._candidate_expense_source_family_v1 (registry physical26092026_0202:178–235), contract self_bill and the existing active root authorisation; it introduces no business lock/write. All ten exact parent/original/successor provenance entries were freshly rechecked and match. SOURCE PASS remains; no further essential source finding.
