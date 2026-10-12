# WB-29 captured working set — minimal implementation

12 October 2026, sequence140. Read-only implementation recommendation supplementing the approved captured composer design and current edit map. Root alone writes product. **Design proposal ready for root selection; no unwritten SOURCE or whole-item approval.** Root has chosen a new explicit **10000 captured parent rows / 8388608 encoded working bytes** ceiling before hold; this is an implementation budget, not a claim that1100/page100/reference100 already imposes that total bound. Overflow refuses before financial mutation. No SQL, business, test, fixture, hosted or screen action was performed.

## One captured invocation, not one invocation per prefix

Use the approved six-argument input overload and existing four-argument thin delegate. Add only the already-approved private component facade plus, if needed for readable code, **one STABLE owner-only native gather helper**:

```text
input_quote(intent, boundary_revision, position_basis, included_source,
            actual_run_position_id, through_segment_key) -> jsonb
captured_component_quote(actual_run_position_id, through_segment_key) -> jsonb
gather_captured_operands(actual_run_position_id) -> jsonb  [optional internal helper]
```

The essential refinement is that **one captured input_quote invocation evaluates the entire component's independent bucket plan and every required positive-member prefix**. It returns the requested intent bucket's existing quote fields plus a private `component_quote`, up to five full `bucket_details`, and compact `prefix_targets`. Scalar NULL/NULL retains its exact original output shape/admission and represents a one-entry quote plan. This permits the original2218 identity/mode/window/1157 quote code to remain **one shared body**, applied in a finite plan loop, rather than copying a second captured calculator or calling the entire capture qualifier for every prefix.

The component facade reads the original exact choice refs and uses the first deterministic component ref as the requested native intent. That intent must equal its exact stored ID/version and captured boundary mapping; it is not a browser or invented saved decision. The captured branch reads **all** this component's original refs itself, constructs the other native intent records/defaults, gathers once and evaluates the whole plan. The requested-bucket top-level result remains the correct quote for `component_resolution_quote_v1`; the facade returns the component quote/details/prefix targets. No-ref components stay on the original one-component valuer/reimbursement path; they need no fake representative intent. Nonsegment Additional/Adjustment retains the original scalar saved Position rules; prefix is unavailable there.

All helpers have postgres owner, SECURITY INVOKER and revoke-all including service_role. Existing guarded financial/read owners alone call them. No argument ever accepts a working JSON object, Source units, derived balance, prefix vector, quote, token or evidence from an RPC/browser. The optional gather helper accepts only an actual native RunPosition ID and constructs its own data. Its result is an ephemeral internal working operand, **not a caller certificate**. No cache table, temporary table, session variable, new public route/type/protocol or persisted financial snapshot is introduced.

## Native working representation

The gather result has three private sections, created only from native rows:

* `header`: RunPosition/RunWork/Worker/Run/WorkChoice and actual captured revision/line/component/Candidate identities; immutable full basis; Worker target/Umbrella/VAT, immutable policy/window/date; exact line refs; maintained full A/R/H, Included/Excluded/recovery/overhang/counts. Include full actual approval separately from any prefix's A.
* `members`: complete parent membership in **existing native segment_key order**, each with exact parent/run_work_segment/approved_segment/line identities, disposition and native A/R/H/four attribution values. Positive Included real approved segments additionally carry the five native hours/pay cells. Removed absent approval never supplies units. No arbitrary detail labels/reason bodies/history rows are needed for financial gathering.
* `buckets`: at most five native STANDARD identities, or the actual one nonsegment code, with one exact pinned decision record per identity when present and otherwise a tagged **NATIVE_DEFAULT** in-memory intent. It retains original Source units/rate separately from Included/equivalent units, a nullable proved Source contribution, and a qualification status explaining whether Suggested and/or ManualRate can be used. It has no fake decision ID/version for defaults.

One materialized native parent query under1100's `(run_work_id,component_key,segment_key)` index supplies members; joins are exact composite native identities, not text labels. Count the complete physical membership before Included filtering. Require count equal both captured member_count and expected_count, attribution_complete, full sum(A/R/H) equal RunPosition, four attribution sums equal its maintained totals, and all per-parent/full native conservation identities. Every prefix basis then comes from **all** materialized parents up to its actual positive Included key; only positive Included members supply target units and Source. Do not first remove negative/removed/excluded parents from prefix conservation.

Charge encoded size of the actual retained working representation, including header/refs/member cells/bucket plan and derived compact prefix outputs; refuse >8MiB and >10000 physical parents without silent truncation. A sentinel/exact count must prove completeness before any result is used. Bucket expansion is at most five per member; derived prefix targets need only one compact entry per positive Included member, not five verbose rows per prefix. There is no100-shift assumption. Maintain native decimal values inside SQL; do not pass through JS numbers.

## Qualification once, shared quote plan

Before target arithmetic, qualify each member's usable units/Source once as in the approved design:

1. Intact positive Included members supply their exact immutable bucket vectors regardless of other component members' R/H.
2. Suggested/default Source per same Included member is the original-channel1157 unit/rate quote with VATfalse; summed bucket Source ex must equal that member A/Included. If not proved, keep bucket Source unknown; all-manual intact units remain usable.
3. A partially spent single positive bucket requires exact original unit/rate Source roundtrip, a finite<=six-decimal Included/rate quotient, multiplication equality and Included roundtrip. Partially spent multibucket debit units remain unavailable unless a real retained native allocation supplies them. Refuse an applicable positive ambiguous member; never drop it from the hold Source amount.
4. Zero prefix bucket contribution is knownzero only after native identity/mode/window/units qualification; unknown positive attribution is not zero. No full component Source is copied into each bucket. Signed captured A/R and Included>algebraic residual are accepted only after complete native capture conservation. Scalar2218 remains unchanged.

Prequalify each plan entry's native identity/basis/manual operands and selected immutable policy/window once. All entries belong to the same captured target/policy/date. Then walk materialized members in order, accumulating per-bucket Included/equivalent units and proved Source; the loop's target quote section is the original shared mode/window/1157 path. For each positive Included prefix, call unchanged1157 for each applicable bucket's **cumulative** operands and sum its ex/VAT/inc once. Preserve penny/range/inc=ex+VAT validation. The final prefix produces `component_quote`; `bucket_details` contains only the full-scope bucket facts and its own quote/mode/decision pair. Source reserve is the actual summed Included once, not the sum of repeated per-bucket component diagnostics.

With non-NULL through_key, first prove an exact positive Included native member after the full census. Supplied basis/Included must equal that exact prefix. Return its requested-bucket/component quote and only the relevant compact prefixes. With NULL through_key, supplied basis/Included must equal full captured totals and the final result covers all prefixes. Full original approval/mapped decision basis is never relabelled as prefix approval.

Pseudocode expresses sequencing, not product SQL:

```text
working := gather(actual RunPosition)                 // one full census
require exact supplied full/prefix basis and Included
plan := qualify own exact accepted refs + native defaults once
for parent in complete materialized native order:
    advance full prefix conservation and qualified bucket accumulators
    if parent is positive Included and inside requested prefix:
        for bucket in plan:                          // maximum five
            bucket_target := existing shared quote body -> unchanged1157
        append compact(component target, parent key/id, Included)
return requested bucket + component target + full bucket facts + prefixes
```

This is O(parent_count × at-most-five buckets), not repeated full-capture O(parent_count²). Calling six-argument input_quote separately from the recorder's existing lateral query would **not** achieve this, even though STABLE: different prefix arguments still rerun full qualification. Returning the compact prefix targets from one captured invocation is the required fix.

The genuine authorised-zero branch remains explicit: after native identity, mode, window and full-capture qualification, when native Included is zero, return a zero component target tuple, known-zero qualified bucket facts and empty prefix_targets. The original hold owner's v_freeze_zero still freezes its advice line and creates no financial hold. A non-NULL prefix is unavailable because no positive Included member exists. MANUAL_AMOUNT retains scalar amount-led admission; this zero branch does not split or override it.

## Hold and origin recorder reuse

Hold1419's new pinned-ref branch calls the facade **once with NULL prefix**, after its original job/lease/Position/Source/currentness checks and before RunLine/hold insertion. It stores exact line ref subset and the single component target tuple; source consumption remains native Included. Its no-ref and reimbursement branches remain original.

Recorder1236 calls the facade **once** using the exact line's RunPosition, requires actual line refs equal original choice subset, and checks full component target equals the stored line/ACTIVE hold. It enumerates the returned compact prefixes in the same native order, subtracts previous ex and VAT, and inserts those shares. Source shares remain each parent's actual Included. Retain original count/Source/ex/VAT/inc final equality and all native share guards; negative differences refuse, no remainder spreading. Recorder invocation need not repeat hold's earlier gather; it performs its own single native immutable gather, so each invocation is linear. No working set must be persisted on RunLine.

## Uncovered STANDARD bucket and inspector honesty

Pinned buckets fetch the exact saved decision **only** through2227 WorkChoice refs and, once frozen, the equal RunLine subset. A CLEARED later state remains readable at that exact immutable version. Never call ACTIVE/current Candidate/current Contract to choose a treatment at capture or advice.

An uncovered bucket does not imply a stored CM resolution exists. Current1157 is an immutable arithmetic helper accepting a supplied mode/Source/units/ERNI/VAT; it **fetches no receipt, rate or resolver state**. Current2227 attachment supplies WB45 decision pairs only. Thus the permitted fallback is the existing native equivalent-basis behavior represented as an in-memory Suggested intent, using that same bucket's genuinely qualified Included Source and the captured immutable policy/target/date. It is not a CM receipt or a manual choice. Original native accepted-selection and financial classification still own admission; this helper cannot resolve a genuine unresolved positive sibling or missing Source basis. If its required Source attribution is unavailable, return the existing typed Source-basis/refusal rather than infer a rate, choose current Available pay rate, revive a later ACTIVE decision or certify an invented CM resolver. Positive siblings excluded by native capture supply no target operands.

Inspector/frozen advice distinguishes PINNED versus NATIVE_DEFAULT, shows exact native pair only for PINNED, approved units versus Included/equivalent units, nullable attributed Source and actual selected target quote/window. Defaults show the actual equivalent-basis treatment with **NULL decision pair**. Unknown remains NULL. W1/W2 accepts no private working set or captured inspector quote. Existing public response budgets and closed transport additions remain separately required; internal8MiB is not a public reply allowance.

## Current source anchors and limits

Use current2218 input quote (`1f161c8a632ded5985bac8cd37db7cca9de189c7275b9e2309fe423e98f67a89`),2302 saved wrapper (`dd3a2bb6d0e0a262a65cdcb96c0620e6b5a0a84e0a518191da1d6d15b6075b7b`),2227 pin (`739e238d39d6848c50eb90746c8f5486091eb39e38948ea28c95ae5d6353f8e8`),1419 hold (`16e437bb839cc8a91cd499303cd0d6d574797e3fb0da6570ab24858d42b6ce87`),1236 recorder (`f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d`),1100 segment schema/index (`0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9`) and unchanged1157 (`ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c`). Preserve2302 live snooze/scalar guards; only the qualified captured branch gains this batch result. Do not replace the existing frozen SRF return type through CREATE OR REPLACE; public readers may project new selected metadata from the private bounded result.

This recommendation is authored design advice, not an independent second approval of its own earlier composer design. Actual implementations, full estimate/frozen/advice joins, closed schemas, compile/source evidence and joined acceptance remain pending. No timing or whole WB29 PASS is asserted.
