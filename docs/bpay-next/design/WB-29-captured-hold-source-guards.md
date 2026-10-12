# WB-29 captured hold Source guards — read-only source investigation

Date: 13 October 2026. Scope: current final registry owners, full capture/hold bodies in 1419, the full segment fact guard in 2223, bound Source snapshot 2146 and native treatment continuity 2135. This is a static source recommendation, not product completion or runtime acceptance. No product, SQL, database, business, fixture or test action was performed.

## Findings

**CHS-01 — canonical approval IDs must be qualified separately from actual captured IDs.** The current `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` in `supabase/repeatable/12102026_1419_banking_pay_next_ordinary_umbrella_components_v1.sql:1342` obtains the current 2146 bound Source snapshot, then compares its `approval_revision_id` and `approved_line_id` with `run_position.captured_revision_id` and its captured line at lines 1345–1356. These identities differ after a legitimate treatment publication. The snapshot intentionally returns the original binding's canonical approval revision and line (2146 lines 94–108), while 2135 stages a new revision and new child IDs (lines 99–111), retaining original earnings and classification. Thus a READY book can currently raise `BPAY_NEXT_SOURCE_BOOK_CAPTURE_CHANGED` even though the actual captured Position is unchanged.

The minimum hold change is inside this existing Source-binding branch, after the original Position equality check at 1318–1341 and before the snapshot tuple comparison. Resolve the captured revision's approval root with **existing** `private.bpay_next_treatment_approval_revision_v1(uuid)`; point-read the same Work/root/component's native approved line. Compare the snapshot's approval revision and line with that root tuple. Retain all other tuple operands: canonical Work/component, approval Work, Source channel, tax, approved Source, native PAYE/UMBRELLA realised/held totals, bound book revision, genuine Source component classification and self-canonical identity. Keep the actual captured revision/line in `run_position`, `run_line`, all Position equality checks, lease/cursor checks and accepted member identities. Do not replace actual applicability with root equality.

This needs no new canonical-root helper or stored identity. Existing 2135 `bpay_next_treatment_approval_revision_v1` (lines 6–34) verifies sealed/approved root, same Work, predecessor continuity, unchanged revision metadata and native POSITION_APPLY member/publication. Treatment revision and child immutability plus existing 2135 payload assertion establish the sealed clone's complete unchanged child sets. A scalar `coalesce(payment_treatment_approval_revision_id,id)` without that positive native qualification is insufficient. Root line mapping is a unique `(revision_id,component_key)` point, not a history scan or latest-line lookup; absent native component remains absent and must not be filled from a different revision.

**CHS-02 — the same canonical mismatch already blocks capture, before hold.** Full current `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` in the same 1419 file has an exact binding lookup at lines 919–930 that demands `v.approval_revision_id=v_row.captured_revision_id`; its snapshot tuple at lines 964–975 repeats the raw revision/line comparison. Both require the same positively qualified root revision/line substitution for Source evidence only. Its original Position check at 878–887 and inserted actual captured fields at 1072–1096 must remain unchanged.

There is an additional initial no-HEAD case: 2135 staging deliberately writes `source_event_id=NULL` on a treatment revision (lines 99–102). The current 1419 `v_answered_source` at 890–896 and Source binding branch at 916–918 therefore no longer recognise the initial native Source arm; lines 926–929 also use that cleared raw event as initial root authorisation. Reuse the canonical root revision's `source_event_id` solely for native Source-origin/binding qualification, as current `private.bpay_next_source_origin_state_v1(uuid,uuid)` in 2146 already does (facts line 20 and the `ar` root join at 59–61). Retain raw captured Source kind, physical snapshot/version, HEAD, policy, classification and actual RunWork revision. The effective root event must also keep the original positive Source query-pin applicability at 1020–1055; its query pin, sealed exclusion snapshot and scope remain qualified by the actual captured revision. Do not bypass the original query gate, reread live query evidence, re-seed a READY book, or broaden Direct no-HEAD PROTECTED_WORK admission.

**CHS-03 — the 2223 origin-share guard does not impose the old target valuer.** Full current `private.bpay_next_segment_fact_guard_v1()` is in `supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql:836`. Its origin-share INSERT arm at 931–946 checks the actual ALLOCATE preparation owner, ACTIVE hold, line/hold parent identity, completed captured parent, INCLUDED segment, Work/component/segment identity, the exact native parent's Included Source amount, and equality of the full hold Source/target tuple with the frozen line. It contains no `bpay_next_value_source_for_target_v1` call and no old target computation. A selected target quote and properly conserved prefix shares can pass this existing guard. Keep the guard unchanged; no target-value exemption or new provenance protocol is justified by this body. Its immutable effect-share arms later copy actual origin target shares rather than recalculate them (978–983).

The actual old target constraint is **the existing recorder**, `private.bpay_next_record_work_segment_origin_shares_v1(uuid,uuid)` in `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql:2692`. The cumulative-prefix CTE calls the original valuer at 2723–2726; the native final conservation tuple at 2741–2743 then compares these old-valued totals with the hold. Once hold uses the selected composer, leaving that recorder arithmetic unchanged can produce `BPAY_NEXT_SEGMENT_HOLD_SHARE_TOTAL_MISMATCH`. Use the already recommended single captured working-set/prefix composer in this recorder; preserve every Source member amount, count, actual identity, immutable insert guard and final totals check. This is a necessary recorder join, not a reason to weaken 2223.

## Minimal successor boundary

| Existing owner | Exact supporting change |
|---|---|
| 1419 capture position page | Positively resolve canonical approval revision/line and initial root event for original Source evidence; retain actual captured/current Position and query cut. |
| 1419 hold fresh position page | Resolve that same canonical tuple for the bound snapshot comparison at 1345; retain actual Position/money/lease/admission guards. Selected-quote arithmetic is separately covered by the captured composer edit map. |
| 1236 record origin shares | Consume the same composed captured prefix targets; keep native Source identity and final hold conservation. |
| 2223 segment fact guard | Reuse unchanged. No original-valued target expectation exists in its origin-share arm. |
| 2146 bound snapshot / 2135 treatment helpers | Reuse unchanged for this support task. They already expose and qualify the canonical root without changing actual Work applicability. |

Historical/no-treatment revisions resolve to their own sealed original IDs and preserve their existing comparison. Missing, foreign, unsealed or malformed treatment continuity must use existing refusals, not a nullable fallthrough. No new table, trigger, financial owner, command, route, producer, public schema or live-rate fallback is required. This note does not certify unrelated downstream current Source consumers or the whole WB-29 item; final implementation still requires source review.

## Current source hashes

| Source | SHA-256 |
|---|---|
| 1419 ordinary Umbrella components | `16e437bb839cc8a91cd499303cd0d6d574797e3fb0da6570ab24858d42b6ce87` |
| 2146 bound Source snapshot | `560982266cf01ccc9f743f9008e88107013e7831e0acdfa79837eb88afcf9400` |
| 2135 native treatment publication/helpers | `56d91e37ede376018c72245ee02fd1d30237f987c656754b2fd0ec668582ddc1` |
| 2223 shift query exclusion / segment fact guard | `b3d7b39a31d35f573dbcc4ea21e9fc0015c801d09a02f1508fe8c9b3ed4535dd` |
| 1236 segment authority / recorder | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| 2146 Source origin state | `08c7ec51a1fcaeac01057f82337682a49b71b19d62cb311a37627337b44f7930` |
| final-definer registry | `22f4a83143b05d0e5bdcc23701ad08d7cc979be7c2da55f72d7fdcc65e4abe8d` |

