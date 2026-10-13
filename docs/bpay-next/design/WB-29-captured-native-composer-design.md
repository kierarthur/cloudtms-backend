# WB-29 completed captured native quote composer

12 October 2026, sequence140. **DESIGN PASS boundary for DQ01–03**, supplementing the accepted-quote join/review and BD156–158. Current scalar input owner is2218; saved wrapper remains2213. This is a concrete implementation recommendation, not SOURCE/compile/runtime/whole-card approval. Root alone writes product; no SQL/RPC, business rows, fixtures, tests, screenshots or hosted action was performed.

## Exact private interface and one quote body

Use a six-argument overload of the existing helper:

```text
private.bpay_next_component_resolution_input_quote_v1(
  p_intent private.bpay_next_component_resolution,
  p_boundary_revision_id uuid,
  p_position_basis jsonb,
  p_included_source_ex_vat numeric,
  p_run_position_id uuid,
  p_through_segment_key text
) returns jsonb STABLE SECURITY INVOKER
```

Retain the four-argument2218 signature as a thin delegation with both new operands NULL. Put original identity/basis/mode/manual/window admission and unchanged1157 calls in **one** six-argument body. With `p_run_position_id=NULL`, require the prefix NULL and retain all existing scalar A/R/H, nonnegative-residual, included<=residual, exact partial quotient and whole native bucket qualifications. This branch's financial admission is not weakened.

With a native RunPosition, the overload positively derives the captured scope below. The existing saved `component_resolution_quote_v1(uuid,bigint,uuid,uuid)` retains its exact decision/version, mapped boundary, live-or-captured guards; its captured arm passes the real RunPosition and NULL prefix to the overload, instead of applying scalar residual bounds before delegation. Add a private captured component composer `private.bpay_next_captured_component_resolution_quote_v1(p_run_position_id uuid,p_through_segment_key text)` returning the component quote and finite bucket details. It obtains refs/target/policy/date from native rows, never a browser array. Its prefix is optional and must be a real positive Included member. No new public token, table, stored decision, producer or money owner is needed.

## Native identity and complete scope qualification

Point-read RunPosition → RunWork → Worker → Run and WorkChoice on `(Worker.run_id,RunWork.work_id)`. Require exact RunWork/Worker/Work/captured revision/component/approved line identities and Candidate equality. WorkChoice.expected_revision_id is the actual captured treatment boundary, not the canonical Source alias. Read the approval/basis and exact immutable policy/window using the retained Worker target channel/Umbrella/VAT and Run pay date. A historical CLEARED decision remains readable by exact ID/version; never require ACTIVE at consumption. Existing hold caller1419 retains current Position/Source-book equality, original lease and financial admission under its existing locks.

For a component with segment members require RunPosition.segment_attribution_complete, `segment_member_count=segment_expected_member_count`, and an exact complete census of `bpay_next_run_work_segment_parent` by its indexed `(run_work_id,component_key,segment_key)` scope. Join each parent through its exact run_work_segment_id to `bpay_next_run_work_segment`; require same RunWork/Worker/Work/revision/segment. A positive Included parent must join its real approved_segment_id to immutable `bpay_next_approved_segment` with the same captured revision/component/segment/approved line. REMOVED/old-parent rows retain the existing native absent-approval shape and cannot supply units.

Before any prefix, require the census count to equal both maintained member counts; sum parent A/R/H to the RunPosition's captured A/R/H; sum Included/Excluded/recovery/held-overhang to its four maintained totals; and require the existing per-parent and whole conservation identities. The actual native identities are:

```text
segment residual = A - R - H
Included + Excluded - recovery - held_overhang = segment residual
full sum(A,R,H) = captured RunPosition(A,R,H)
full sum(Included,Excluded,recovery,held_overhang) = captured maintained totals
```

Do not call the mutating completion helper0559 from this STABLE path. Its6158–6210 completion proof and the1236:3758–3781 guard demonstrate the native equality to recheck. Preserve its economic/managed-collection classification; this quote design does not make a blocked financial component admissible.

For full scope, `p_position_basis` must equal the full captured four-key object `{approved_source_ex_vat,realised_source_ex_vat,held_source_ex_vat,source_basis_channel}` and `p_included_source_ex_vat` must equal the full sum of positive Included money. For a prefix, first qualify `p_through_segment_key` by an exact point lookup as an INCLUDED parent with positive included_positive_source_ex_vat in this same component. Then use the existing native segment-key order and all captured parents up to that member for prefix A/R/H/conservation; require the supplied object and Included money equal those actual prefix sums. Quoted units come only from positive INCLUDED members in that prefix. Excluded/negative/removed parents participate in conservation, not target units.

Captured A/R can be signed native values; H and the four attribution totals retain their native nonnegative constraints. Validate finite penny values/range and exact equality, not the scalar helper's blanket A/R>=0/residual>=0 condition. Positive Included money can exceed the algebraic residual when recovery or excluded parents coexist. **Only** this fully qualified native captured arm bypasses that scalar inequality. Source component approval in the immutable decision basis remains checked against the full actual approval/RunPosition; a prefix's summed A is not relabelled as the original decision's full component approval.

Materialize the exact captured member census once per composition/recorder call. Derive bucket and cumulative prefix operands from that materialization, not repeated live/history reads. Use maintained count as the exact membership bound and the existing physical captured-scope byte/row budget; any implementation ceiling must refuse overflow before treating the scope as complete. Do not silently cap to100 decision refs or confuse a reference ceiling with the number of captured shifts. Prefix lookup/ordering uses the1100 parent index; no financial history walk is introduced.

## Included units and same-scope Source qualification

Expand each real Included approved segment's five native `(bucket,hours_*,pay_*)` cells in DAY/NIGHT/SAT/SUN/BH order. These are approved_segment.hours_day/night/sat/sun/bh and pay_day/night/sat/sun/bh, not whole RateDetail hours and not current Contract rates. Require finite nonnegative native unit/rate values within original precision/range; source qualification also requires the original approved Source channel and the existing bound Source receipt/head/classification authority. Treatment cloning does not reprice those SOURCE cells.

* An intact Included member has native R=H=0 and Included=A>0. Its exact bucket vectors are usable for manual-rate target quote, even when other component members are paid, held, snoozed, query-held or unselected. Aggregate only these Included vectors. **All-manual partially selected multibucket composition must succeed on these intact members**; aggregate component R/H outside them is not a reason to refuse.
* Source attribution for Suggested or mixed resolver fallback is qualified **per same Included member**. Call unchanged1157 internally in original channel with VAT disabled and `MANUAL_REPLACEMENT_RATE` only as native Source-value qualification for every positive bucket's immutable units/used rate. Require the sum of those bucket Source ex amounts equal that member's approved/Included Source. Aggregate the proved bucket contributions across included members. No leftover penny allocation, approved fraction, target ratio or parent Source repeated per bucket is allowed. If this split is unproved, all-manual units remain usable; any Suggested/fallback bucket requiring that split must return the specific typed Source-basis refusal.
* A partially spent Included member with exactly one positive native bucket may use BD157's exact qualification: unchanged original unit/rate Source quote equals that member's A; source rate strictly positive; Included/rate quotient itself is finite, <=full units, <=six decimals and multiplication-exact; original-channel1157 roundtrip equals Included. No rounding/truncation admits a repeating quotient. Known single-rate Source may support Suggested with units NULL; ManualRate requires proved units.
* A partially spent multibucket member has money A/R/H, not per-bucket debit units. Its vectors alone do not prove what remains. Use an existing exact retained allocation boundary if one actually supplies that fact; otherwise typed residual-ambiguous refusal applies **to that member**, not every partial component containing other intact Included members.
* A bucket with zero contribution in this prefix is known zero after basis/mode/window qualification, not an unavailable value. It contributes zero units/Source/target rather than receiving the whole component's Included money. An absent/unproved positive bucket is not coalesced to zero.

For each independent native bucket, call the shared helper with its proved cumulative Included units/Source and selected intent;1157 alone produces target ex/VAT/inc. Aggregate bucket target outputs once. The component returns **actual Included Source once**, separately from per-bucket Source (which may be NULL for all-manual scope with unproved monetary split). A bucket helper's existing `included_source_component_ex_vat` diagnostic must not be summed as if it were independent Source reserve. Validate target penny/range/nonnegative/`inc=ex+VAT` and one winning immutable window before hold storage.

MANUAL_AMOUNT remains genuinely amount-led and the existing exact saved original Position basis/full residual scope. This segment extension supplies no authority to allocate one component-wide manual amount across standard shifts or scale it over a selected prefix. Non-segment Additional/Adjustment paths keep their native scalar basis/quantity and four-argument admission. A non-NULL prefix is unavailable for a component with no segment membership. Reimbursement remains the current DR38 branch.

## Exact accepted refs and mixed composition

Current2227 migration provides WorkChoice accepted_resolution_refs with <=100 entries/32768 bytes and RunLine refs with <=5 entries/8192 bytes. NULL means historical, without a new descriptor-cut promise. New explicit `[]` means no accepted treatment and must never consult later ACTIVE. New nonempty refs are the immutable accepted IDs/versions, qualified against the actual expected/captured revision and original target/policy/date. WorkChoice guard2227:89–103 freezes them even during OPEN subset upload.

The composer extracts this component/line's own exact refs from WorkChoice and, after freezing, requires RunLine's retained refs to equal that exact subset. Check duplicate family/code/line identity, exact decision version and original/published mapping; target tuple/policy/date must match Worker/Run. Store explicit [] on a newly frozen no-treatment line; do not leave a descriptor line historical NULL by omission. A decision changing state to CLEARED after Create does not remove the pinned version.

With no component refs, preserve original one-component valuation once and the existing reimbursement path. With refs, enumerate independent native bucket/code identities once: pinned decision when present, otherwise only actual installed compatible native receipt/resolver precedence. There is no late ACTIVE fallback, guessed CM receipt or silent resolution of an unresolved sibling need. Default native suggestion needed in mixed composition is an in-memory native intent, not a stored/dummy decision or changed user mode. Any unresolved positive sibling that lacks genuine Source/units qualification refuses before hold insertion.

Return a bounded private result with RunPosition/boundary/line/prefix identity, literal captured full/prefix Position basis, actual Included Source, immutable policy/window/target tuple, component quote, exact component refs and up to five native bucket details (approved units separate from included/equivalent units; selected mode/rate/amount; nullable attributed Source; quote and decision pair). Incoming Preview never accepts this private result as authority. Frozen advice uses these facts, not a current Candidate/Contract/ACTIVE reread.

## Hold and origin-share joins

At1419:1303 keep native Included-source derivation; preserve1315 onward current Position/source-book/lease checks. In the existing selected-treatment branch replace only1410–1416's old valuer with full captured component composition. Its one target tuple feeds RunLine, ACTIVE hold, Position target-H and Worker totals as existing1419:1419–1454. Source reserve is the actual component Included Source once.

At1236:2718–2730 preserve the existing deterministic native positive-member prefixes. For accepted treatment quote the **same composer** at each actual through_segment_key, using cumulative included bucket units/Source; difference successive target ex and VAT as the existing origin recorder does. Quote cumulative per-bucket operands, not rounded per-segment target shares then sum them. The last prefix must exactly equal the full composer and stored hold/line target. Keep1236:2741–2743's count/Source/ex/VAT/inc equality and every existing origin/fact/effect-share guard. Reject a negative/invalid share rather than spreading a rounding remainder.

The recorder should materialize all members and qualified bucket Source once, then derive cumulative operands for all prefixes inside its existing invocation. This avoids an O(member-count²) membership re-read; it does not add persistent staging or another arithmetic owner. A small private operand-gathering helper is acceptable only if it produces the same positively qualified native scope and is never exposed as caller-supplied proof. Shared intent/window/arithmetic admission stays in the one overload body.

## DQ closure and evidence

| Finding | Concrete design closure |
| --- | --- |
| DQ01 | Exact RunPosition + real optional Included prefix; full census/conservation first; captured full/prefix A/R/H/Included equality replaces scalar bounds only in the qualified captured arm. |
| DQ02 | Native Included approved-segment bucket vectors/precisely qualified single-rate residual units; no whole RateDetail or excluded/snoozed units. Intact Included multibucket manual rates work despite outside component R/H. |
| DQ03 | Native Source quote/conservation at each same Included segment and actual prefix, then cumulative bucket quote/composition; no whole-parent Source copied into narrowed buckets or remainder allocation. |

Reviewed source anchors:1100 segment schema/indexes and maintained counters;0559:6158–6210 completion;2223:1029–1128 capture and931–945 share guard;1236:2692–2748 origin recorder and3758–3781 counters;1419:1300–1454 hold;2100:8–61 basis;2218 scalar input;2213 saved wrapper;2227 metadata/cut/immutability. Current2218 SHA-256 `1F161C8A632DED5985BAC8CD37DB7CCA9DE189C7275B9E2309FE423E98F67A89`;2227 pin `D330B8E53521875857D78448D31DA53BEC1C5A64D3E68D3BD18E4D328FA57C75`;2227 migration `2BC1B4E50FED7F33FAB5ECE420A7C53CDCE04F1DE92865DDB6DB79E8D6EFD85D`.

DQ01–03 are closed at DESIGN level by this exact interface; implementation/source verification remains required. This does not pass any unwritten captured composer, Source attribution that is genuinely absent, financial classification, future advice join or whole WB29 item.
