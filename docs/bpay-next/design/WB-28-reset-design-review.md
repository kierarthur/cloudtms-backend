# WB-28 scoped reset design review

13 October2026. Independent read-only review of root's concrete proposed per-unit reset design against card02:1046–1067, W12–14, R2-F01/F07/WB28 and the current2306/0016/0117/0135/0121 owners. Root alone authors product. No product/SQL/RPC/business/runtime/tests. **DESIGN PASS for this narrow memory architecture, with the exact required implementation conditions below; not a whole WB28/source/A4 completion claim.**

## Why this is the necessary small support

An indexed marker on the card-required frozen clear-operation header can represent an actor/date/filter reset without deleting immutable accepted rows. One nullable remembered_selected boolean on the existing native LEAF is sufficient to carry a partial Candidate/Client/channel reset through the next accepted broader baseline, where HEADER.mode alone cannot represent A=true/B=false. No second money cache, fabricated run, new default protocol, new clock, public trigger or producer change is required.

Concrete sequence: baselineR is broad NONE, unchanged A/B false. A-only reset later thanR makes native future A defaults true whileB retains false. Next broad Create has HEADER.mode NONE but records per-unit A=true/B=false at its original click. Later unchanged A reads that recorded true; B reads false. New/changed units remain selected by original WB22 rules. A later explicit Candidate/component false still wins. Copied inherited A exclusion rows may remain historical; a fully known captured true must precede their old fallback so they cannot resurrect the cleared exclusion.

## Exact required ordering and integrity

1. **Original sequence/reset outcome.** Qualify an effective accepted reset marker against baseline HEADER agency/epoch/actor/official-date and the current native unit's Candidate/Client/target-channel, with reset original accepted sequence > baseline HEADER original agency_sequence. Earlier accepted Create sealed later still has an earlier sequence and must not undo reset. A later accepted Create supersedes it normally. Discovery/unconfirmed/pending marker is not an accepted reset; replay cannot apply the reset twice. Real workbench revision must advance even for zero-owner accepted reset, so unaccepted stale descriptors/cursors fail rather than continuing an old apparent context. Existing four resolver caller contracts can stay unchanged.

2. **Default order.** Preserve the original detached-null-baseline branch first: a current local NONE fresh choice remains false, not overridden by some historical reset. Preserve genuine new/changed unit behavior. For an existing unchanged baseline unit, a matching later effective reset supplies ALL defaults; otherwise a complete recorded unit boolean supplies the actual prior choice; only all-NULL historical rows use the original HEADER mode/exclusion fallback. Current explicit Candidate/Work/component/Case overlay and Candidate-false dominance remain last. Do not use a current raw mode as an accidental blanket override of the new mixed per-unit baseline; global Set all/none already detaches baseline via the original store.

3. **Actual selection unit.** Current choices2306:88–95 groups Work by Work+component_key/approved_line_id and Case by exact case_component_id. There is no independent segment tick/memory owner. Every exact unit LEAF must receive the same native remembered_selected value; native segments only contribute exact equality/new-changed facts. Known recorded values on an unchanged unit must be complete and equal. Refuse mixed/partially nonnull contradictory records instead of bool_or or first-row selection. Old rows are untouched NULL; no historical backfill.

4. **Source of captured flag.** Capture0016:54–95 must compute original effective choices from the exact descriptor namespace/baseline/raw intent under the original accepted cut, once, then join each native eligible leaf by exact unit identity. A materialized native choices CTE joined to the streamed original leaf inventory is enough; do not call a whole-scope choices scan per segment, use browser selected totals, replace original positive manifest, or record later PIN-time selection. Keep existing per-component10000/8MiB capture budget, adding the small new field to its encoded budget. The recorded flag is presentation/default metadata; original query/snooze/guard skip and original captured membership remain actual financial authority.

5. **Client qualification is not yet in the leaf DTO.** Current0121 selection_memory_leaves_v1 returns leaf_key/Candidate/Work/revision/line/Case-component/direction/basis; it does not return client_id and its basis also omits client_id. Candidate and target channel have positive native operands. Client reset matching therefore needs the exact current native Workbench item/unit identity under the same original qualification, or one truly needed private internal projection addition. Do not assume a Work/Case Client, use current names/Contract fallback, relabel NULL as a match, or reset all Clients because the operand was absent. Case-only NULL Client remains excluded by a specific Client filter exactly as original item scope does. No new public schema/producer is required.

6. **New column and native guard.** Nullable boolean, no default/backfill; HEADER must remain NULL (explicit CHECK). Include it in LEAF INSERT only. The existing guard's complete to_jsonb UPDATE equality automatically preserves it outside original HEADER command/ack fields, and DELETE/TRUNCATE prohibition remains exact. Do not allow a later UPDATE to accepted LEAF flags or widen HEADER update exceptions. Native capture alone supplies the value; no request/DTO financial operand is added. Same-unit consistency is asserted/read fail-closed, not silently normalised to a false/true historical fact.

## Rule-B and historical semantics

Accepted replay in0016:290–305 returns the original accepted result before new header/default cut; current PAYEguard accepted branch1358:257–262 uses retained descriptor/selection_candidate counts. Original queued eligibility0016:4–50 reads exact accepted HEADER/LEAF identities and basis, not current reset markers or remembered_selected. A later reset therefore cannot change old PIN, query/snooze dispositions, accepted rate refs, original holds or line/case monetary membership. Retain those bodies/paths; do not route their equality check through live choices.

The proposed choices successor does change **future default interpretation** of an explicit old baselineR when a caller asks for current W1 defaults, while the explicit resolver still returnsR and its retained rows are unchanged. State that distinction plainly: it does not promise an unchanged current-default SELECT after reset. Actual historical/frozen R rows and accepted financial membership remain unchanged. Existing Create header stale workbench/adoption comparison0016:315–328 runs before original member capture; command-clock serialization276 and reset's original accepted-order seam must prevent a reset becoming visible halfway through one accepted cut.

Do not confuse remembered_selected with effective financial inclusion after the one-time PAYE same-week override: original raw choice/default is retained; actual guarded captured positive membership remains its native authority. Other actors' reset markers cannot affect this HEADER. Snoozes, Pay Query exclusions and old earned Source/Position money are untouched.

## Office consequence

Current clean Ready read1121 asks implicit latest, compares CANDIDATES/HEADER memory/context1139–1140 and adopt1035 updates only !dirty. Because partial reset can leave the scalar run_id equal toR, clean renderer must still re-read native defaults/counts under the new real workbench revision; do not assume unchanged run_id means unchanged selection. Captured post-reset broad baseline's per-unit flags then make later ordinary clean adoption stable.

After exact reset outcome, use the original namespace/local revision captured at confirmation. If unchanged, reset the relevant local arrays/baseline/remembered mode and counters; no reset network call. If staff edited later, retain those overlays/original bases, requalify through the existing unresolved mechanism and do not erase their changes. A late reset response must not blindly replace dirty state. Original pending/accepted Create and resolution journal bytes remain unchanged; no newer-version auto retry.

## Reviewed source references

-2306 actor_selection_memory_v1: choices83–131; resolver46–81; guard1810–1874. SHA3F77BBB57500F1320D3873935C8504264A46E50287BE9740267F838F8D1DC750.
-0016 accepted_eligible_leaf_v1: capture54–95, original6arg Create97ff; frozen eligibility4–50. SHA35648FDE73F982466ED72C065EB9B6916D6EF998364BD64AF0F491625A73CFD3.
-0121 case_choice_control_point: original native leaf scope4–47 (no Client output).
-0117 chosen_display_read_barrier: native W1 memory194–195/context233 and current defaults; SHA E924CB0C55D2AF7C85F9C37C0B6164DEC2EA8532F319EAD1F49B38F0C7E2EE5B.
-0135 selection_complete_counts: exact native per-unit default/row/count joins5–83; SHA5F8D2E1CC3AD80460C840AAC028CB76A499F3DE2A7496B721BD8C7369ED9BA85.
-FE workbench store/current refresh998–1065,1117–1143; SHA C19E85817F57AEC3AEE0BCF2F9985DF32E9D7CCD7C7D42A6C6CE772551711F57.

All are source facts, not installation or app proof. Whole frozen clear discovery/pages/receipt/mapping and the actual UI are outside this focused memory design review and still require their own complete WB28 implementation.

