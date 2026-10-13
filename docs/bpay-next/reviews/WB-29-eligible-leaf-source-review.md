# WB-29 accepted eligible-leaf source review

Verdict: **SOURCE PASS — 0 open findings.**

Reviewed 13 October 2026. This is independent static source approval of the six owners in `13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql`, against the approved eligible-leaf design. It is not whole WB-29 approval, runtime acceptance, or a financial result claim. No SQL, RPC, business data, fixtures, tests, browser or hosted actions were executed by this reviewer.

## Exact reviewed bytes

| Evidence | SHA-256 |
| --- | --- |
| `supabase/repeatable/13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql` | `35648FDE73F982466ED72C065EB9B6916D6EF998364BD64AF0F491625A73CFD3` |
| Approved `design/WB-29-eligible-leaf-join.md` | `D183403891DAD551341E1F160785820C5B84523191230381704A28F267D43798` |
| `design/WB-29-eligible-leaf-join-review.md` | `0F21C81053BB51DF5099AB948E4267741B0CB2F9A840A1FD5D51F36611B18623` |

The current saved SQL was reread after the EL-S01 repair. Earlier reviewed SQL hash `28C3AA591C3D0E0783F97803D534493D8A48DC20F359897818265484BA96D840` is superseded by the hash above.

## Finding closure

**EL-S01 — CLOSED.** The initial helper filtered a component's captured leaves for each segment, which could repeatedly scan the same component. The repaired predicate uses the original native key `WORK:<work>:<component>:WORK_SEGMENT:<segment-key>` or `WORK:<work>:<component>:WORK_COMPONENT:<component>` with `run_id` and `row_kind`. This qualifies the existing `(run_id,row_kind,leaf_key)` primary key. Candidate, Work, revision, line, component, PAYMENT direction and native segment identity checks remain present; the repair does not broaden eligibility.

## Six owner checks

| Owner | Result | Source evidence and preserved boundary |
| --- | --- | --- |
| `private.bpay_next_choice_leaf_eligible_v1(uuid,uuid,uuid,uuid,uuid)` | PASS | Exact native HEADER, descriptor/raw intent, actor and official date qualify the frozen cut. Descriptor runs without the HEADER fail with `BPAY_NEXT_CHOICE_ELIGIBLE_CAPTURE_MISSING`. Original non-descriptor admission remains unchanged. Exact SEALED Work choice, expected revision, native line/segment and SUBSET component membership precede the captured LEAF point lookup. A known absent leaf returns false. |
| `private.bpay_next_selection_memory_capture_v1(uuid,jsonb)` | PASS | Original namespace/HEADER and exclusion provenance are retained. READY leaves are streamed in a stable component grouping; the count and encoded identity/basis budget are checked before each insert. The declared per-component bounds are 10,000 leaves and 8 MiB, with explicit overflow refusal. There is no new global Draft cap or giant aggregate JSON. |
| Six-argument `private.bpay_next_create_preparation_v1` | PASS | Complete-body comparison with the 2227 parent confines changes to universal native capture, original SUBSET page sealing and frozen segment materialisation. Original validation, replay before currentness, parent prelocks, Source admission, expected-revision/E1 checks, Case choice, financial fences and accepted descriptor/command are retained. Optional memory flag, resolve branch and acknowledgement remain gated by the original memory flag. |
| `private.bpay_next_materialise_choice_segments_v1(uuid,uuid)` | PASS | Original ALL/SUBSET mode and component limits apply to the exact union of eligible captured LEAFs, native Snoozed-at-cut segments and captured query-held segments. Native cut identity is retained. Existing sealed count short-circuits later calls, so later acknowledgement cannot widen the original cut. |
| `private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint)` | PASS | Its count and missing-inventory checks use the same exact union as materialisation. Original request/page/count/status/replay checks are preserved. Full parent financial inventory remains in the original capture owner; an unresolved ACTION sibling absent from the eligible cut stays unselected. |
| `private.bpay_next_segment_fact_guard_v1()` | PASS | Complete-body comparison with the 2223 parent shows only the new eligibility condition for effective INCLUDED: `disposition='INCLUDED'` and no captured query hold. Original native Snoozed IDs/version/cut, query reason, page membership, SUBSET identity, captured-state, Source, settlement, Case and conservation predicates remain intact. |

## Choice, exclusion and replay checks

- Universal capture does not tick a Work or Case. Captured READY leaves establish admission only; original Work choice and selected-component membership still determine selection. ALL mode is retained, and SUBSET cannot admit an unselected component.
- The absent optional memory key keeps its original NULL baseline and unacknowledged HEADER. Resolve, command/agency attachment and acknowledgement remain conditional on the original memory path. Universal capture is not a new acknowledgement or memory-selection mode.
- Non-memory SUBSET sealing uses the original descriptor's selected line IDs, original 100-item page owner and original component seal. The Office upload path recognises already SEALED members and does not resend an invented wider component set. Existing exact seal replay remains available.
- CASE-only descriptors retain their original native Case path; they do not gain a synthetic Work. Work materialisation operates only on actual selected Work choices.
- Native Snoozed and captured query-held exclusions are preserved separately from eligible payment leaves. They cannot supply included units. Eligibility is required only for effective INCLUDED, closing the earlier design EL01 concern without deleting exclusion provenance.
- Descriptor replay returns through the original early replay path. Existing current/expected/applied revision guards are retained: a later publication does not revive or reprice a changed accepted offer.
- No late ACTIVE decision or live EX08 need classifier is used as a fallback for captured admission. Full parent A/R/H facts remain with their original owners.

## Narrow downstream join inspection

The current uninstalled composer `12102026_2357_banking_pay_next_captured_resolution_composition_v1.sql` checks exact eligible LEAF proof for positive included native segment operands and its non-segment path. The current uninstalled common hold successor `13102026_0005_banking_pay_next_captured_resolution_hold_v1.sql` sets Included to zero and disables authorised-zero freezing when a non-segment Work component lacks the eligible leaf. This prevents unresolved non-segment siblings from bypassing admission through a no-resolution-reference path. The full parent facts, existing collection proof and financial guard ownership remain separate.

These two call-site checks are approval of the eligible-leaf integration only, not approval of their entire financial implementations. Their reviewed hashes were `DD453D9030B65A25D144FBCE9E6A0C986914BE8A2B0A1F8336043EE220B89342` and `5DD92083243DDE0BD41418DA6436D28D1566C32C1B0A0A540D9A8D4AE742F66A`, respectively.

## Parent and access evidence

The comparison used the saved final parents: 2227 descriptor Create, 2306 memory capture/schema, 1832 materialisation, 1236 seal/full capture and 2223 segment guard, with the original 0121 native LEAF key authority. The target defines exactly six private functions. Existing owner/revoke declarations are explicit; no public route, command kind, table, trigger, producer, monetary calculator or new protocol is introduced by this file.

Local installation, catalogue matching, execution plans, joined Create/upload/acknowledgement, concurrent replay and business monetary behaviour are **NOT RUN by this reviewer**. Those remain separate owner verification boundaries.
