# WB-36 money integration — current source design

2026-10-10. Read-only source investigation. No SQL, runtime, tests, data, fixtures, network or product edits were executed. This is an implementation recommendation, not a built/installed/app acceptance claim. F-25 Rule B remains closed; use `choice.expected_revision_id`, `selection_candidate`, the retained query snapshot and `pin_v2`. Do not revive accepted-cut/resolver/protocol files.

The required integration is exact segment attribution beneath the existing financial component. Ordinary `WORK:DAY:YYYY-MM-DD` and the corresponding `TS_DAY` parent remain money/selection authority. **Source retains its actual `SOURCE:<source_component_id>` component, even when its work_date changes.** A Source work_date is not permission to rewrite its component into a DAY key.

## Binding scope

Read full WB-36 card `02_WORKBENCH.md:1287–1330`, current checklist `IMPLEMENTATION_CHECKLIST.md:1253–1290`, and addendum `R2_EXACT_CONNECTIONS.md:5–25`. Actual generated checklist sequence is 48. Required money children are D14 approved_segment, D15 selected_segment, D16 run_work_segment, D17 immutable hold origin shares plus separate append-only effect membership, D18 record_work_segment_origin_shares, D19 derived gross balance. D08–D11 control/ack commands remain separately required, and are not claimed by this money note.

R2 requires exact positive exclusion, real negative recovery, prior native paid/held attribution, date/ordinal correction safety, and original-cash-only return/reissue. A helper with no actual financial insertion/consumption seam is insufficient to close D17/D18.

## Actual surviving winners and seams

All paths below are relative to the BE build root. The exact function names, identity argument types and physical files come from the current final-definer registry and saved source, not the historical filenames quoted by the card.

| Owner | Current physical winner / useful lines | Minimal segment seam |
|---|---|---|
| `private.bpay_next_publish_ordinary_from_financial_v1(uuid)` | `supabase/repeatable/10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql:306–469` | Persist genuine upstream `segment_id` (validated 337–340) into shift_detail and approved_segment. Preserve WORK:DAY grouping and the existing actual segment gross. No snooze/pricing branch in approval. |
| `private.bpay_next_seal_selection_v1(uuid,bigint,bigint)` | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql:6–76` | Freeze segment INCLUDED/SNOOZED membership against the exact work_choice revision and accepted control boundary. Keep Rule B channel/floor and raw query snapshot semantics. |
| `private.bpay_next_pin_selected_work_v1(uuid,uuid,bigint,bigint)` | same `0151:181–546` | The exact choice revision is checked at 298–340, retained query snapshot at 453–518, run_work INSERT at 523–526. Attach the selected segment identities to that exact run_work/revision; do not reread a later snooze. |
| `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:739–1158` | Existing bounded ALL/SUBSET parent census 826–859, WORK then exact position locks 861–893, full parent snapshot INSERT 1073–1094. Capture segment attribution underneath the same parent and retained revision. Preserve monetary revalidation and original Source book/query witnesses. |
| `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | same `2018:1161–1442` | Existing parent monetary equality 1301–1348, shared valuer 1358–1364, run_line INSERT 1366–1380, hold INSERT 1384–1389. Replace only the selected source operand with exact included positive contribution; obtain hold ID and append captured origin shares atomically. |
| `private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` | same `2018:2298–2533` | Actual BANK **and INTERNAL_ZERO** WORK realisation owner. Exact member/hold validation 2411–2435; effect INSERT returning ID 2456–2468; position move ACTIVE→realised 2472–2489; collection reconciliation 2490–2491. Append effect children from the exact existing hold shares in this transaction. |
| `private.bpay_next_receive_internal_settlement_v1(uuid,uuid,uuid)` / `private.bpay_next_complete_internal_zero_v1(uuid,bigint)` | `04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql:86–151,154–244` | Receipt admission and terminal completion, not a second WORK effect insertion path. 2018 post_simple_outcome already handles both kinds and calls complete_internal_zero at 2515–2516. Do not duplicate effect membership in 0490. |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3955–4260` | Required narrow segment branch: derive explained negative gross from exact same-parent stable-segment origins, not max(day paid−day approval,0). Retain existing one collection/case/component/rule per (work,component), actual owners/locks, protected capacity and separate forgiveness. |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` / `private.bpay_next_work_collection_capture_guard_v1()` / `private.bpay_next_work_collection_guard_v1()` | `04102026_1519_banking_pay_next_work_collection_v1.sql:11–60,66–157,160–297` | Admit and verify exact segment-origin branch, including a genuinely removed old component proven by a complete captured approved-segment set. Preserve current nonsegment path and all original paid/Source qualification. |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:414–494` | Actual current signed effect writer (not old1647). Validates real RECOVERED event/case hold/capacity/member 426–443; negative effect 447–459; exact replay 460–477; parent reduction 479–489. Add immutable consumed negative-origin membership; preserve original negative effect and parent update once. |
| `private.bpay_next_apply_write_off_v1(uuid,uuid,bigint)` | `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql:268–343` | WRITTEN_OFF case event and counter changes 325–335; it does **not** reverse paid WORK source gross. Attribute genuine written-off debt to retained negative origins, separately from realised-gross effect membership. Never manufacture a financial effect for forgiveness. |
| `private.bpay_next_receive_simple_csv_return_v1(uuid,uuid,text,numeric,timestamptz,uuid)` | `04102026_0230_banking_pay_next_simple_csv_return_v1.sql` | Reopen original cash obligation only. Leave realised segment gross/effect history intact. |
| `private.bpay_next_build_simple_reissue_v1(uuid,uuid,bigint)` | `04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql` | Reissue original cash, no new segment gross or WORK hold. |

Selection helpers are current0570: `append_work_choices_v1(uuid,bigint,jsonb)`, `append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[])`, `seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint)`, and `work_choice_guard_v1()`. New SEGMENT_PAGE belongs to the existing preparation service intake, not a new transport protocol.

## Exact schema conservation and moved-date shape

Existing core1324 constraints remain authoritative:

- position PK `(work_id,component_key)` at345;
- approved_line UNIQUE `(revision_id,component_key)` at206 and composite `(revision_id,id,component_key)` at208;
- run_position UNIQUE `(run_work_id,component_key)` at547, FK to the exact run_work at548–550, and `residual = approved − realised − held` at554–555;
- run_line UNIQUE `(run_work_id,component_key)` at583 and nonnegative source operand at570–571;
- hold UNIQUE(run_line_id) at605, exact line/work/component FK606–607, and immutable amount identity under its actual lifecycle;
- financial_effect unique `(operation_id,operation_item_id,effect_kind)` at405; its work/component FK is the existing position at410–411.

Keep the specified `run_work_segment UNIQUE(run_work_id,segment_key)`. It is one retained stable identity, **not** one row per date and not an economic position. A segment can move dates more than once while old original parents retain credit. One old/new date pair is insufficient; duplicating this identity or changing its uniqueness is unnecessary.

Add narrow typed subordinate **parent-attribution children** (recommended local name `bpay_next_run_work_segment_parent`):

- PK `(run_work_segment_id,component_key)`; same run_work/work/captured_revision and FK to its actual run_position `(run_work_id,component_key)`;
- captured current approved segment ID/approved_line ID nullable only for proven absence in that exact complete approved revision;
- actual original parent key and descriptive work_date, source channel/binding provenance, approved source gross, exact realised source credit, exact ACTIVE held source credit, signed residual, included positive remainder, excluded positive remainder, genuine paid recovery remainder, and retained-hold overhang;
- one logical run_work_segment may consequently have parent A and parent B children. Original hold/effect shares keep their original parent, original run_work_segment and revision. Capture children merely retain that attribution; they do not rewrite original rows.

Example: same stable key S paid £40 on day A, then amended to £40 on day B. One run_work_segment(S); A child approved0/realised40/recovery40; B child approved40/realised0/positive40. Do not move A's credit into B. A later A→B→C history may need three actual parent children. If a source segment changes date but retains its actual SOURCE component, one component child suffices; never create an artificial DAY parent.

For current and removed segment origins, the bounded capture member set must include **both the selected current parent and its exact linked historical parent components**. The existing SUBSET lane only visits selected_component (2018:841–855); merely reading new-day membership would omit old-day recovery. Extend the bounded parent census from explicit segment-origin membership, deduplicate by actual(work,component), and retain existing ≤100 parent cursor/order. No Candidate-history sweep or ordinal/date matching. Do not silently miss removed original keys.

For each actual parent/component and stable segment i, define from retained source facts:

`A_i` = exact approved gross at this parent in the chosen complete revision, or proven0 if absent;
`C_i` = signed sum of realised effect-origin source dispositions at this same original parent;
`H_i` = exact origin shares whose native parent hold is ACTIVE;
`D_i = A_i − C_i − H_i`.

Retain `sum A_i=parent.approved`, `sum C_i=parent.realised`, `sum H_i=parent.held`, and `sum D_i=run_position.residual`. Check source and target gross amounts separately; never compare payroll gross to bank NET.

Positive `max(D_i,0)` becomes included or excluded according to the frozen control. Genuine paid negative recovery is `max(C_i−A_i,0)` and is never snoozed. Held-only excess is **not** paid debt: `max(H_i−max(A_i−C_i,0),0)` is a retained executable-hold overhang. It cannot be released, moved into another day, or treated as realised recovery by a snooze. Keep any necessary pending financial disposition explicit; do not create a second fresh hold for the same stable origin while its original executable hold still owns the obligation.

Summed positive+excluded−paid_negative−held_overhang equals the existing signed parent residual. No rewrite of the run_position residual CHECK is necessary.

## Mixed positive/negative parent: concrete required collection integration

Paid A40; current A20+B60 at one day gives parent approved80/realised40/residual40, but the exact segment differences are A−20 and B+60. The current implementation loses the negative arm:

- 2018 capture only looks for a collection when whole-parent `approved−realised<0` (1004–1019);
- reconcile creates principal from `greatest(realised−approved,0)` (4191), so this example creates no £20 debt;
- 1637 `bpay_next_position_collection_shape_check` requires managed residual<0 (migration21–24);
- hold uses managed **or** normal `if ... else` (2018:1350–1358), so one parent cannot both reserve its £60 positive and acknowledge its £20 recovery;
- a missing current approved_line with nonzero approved parent gross is an issue at898–904. A removed old parent with approved0 gets past that particular check, but still needs the exact collection/segment qualification (1020–1032 and1519:47–51).

Required finite changes, justified by named R2 capture/hold/collection seams:

1. Store a typed segment-attribution discriminator/observation on the existing collection branch and a retained **negative-origin child set** for each actual collection reconciliation revision. Children reference exact historical positive effect/hold share, current selected/captured stable key and original parent, with outstanding negative amount. Preserve one existing collection/case/component/rule per `(work,component)`; no per-shift Case or payment unit. The original single effect/run_line pointers remain genuine first anchor; **all** segment-origin children, not that one anchor alone, prove completeness of later credits.
2. Reconcile's segment branch obtains `N=sum max(C_i−A_i,0)` without offsetting unrelated B positive. Replace only its negative principal operand. Preserve the existing formula for actual recovery K, forgiveness W and protected case recovery Hc:

   `principal = K + W + greatest(N − W, Hc, 0)`.

   **2026-10-10 refinement:** the scalar formula above describes the original parent formula but is superseded for genuine segment attribution by `design/WB-36-negative-origin-integration.md`: `principal = K + W_total + SUM_i greatest(N_i-W_i,H_i,0)`. Forgiveness/protected capacity from A cannot offset an independent B debt. This retains the current4191/4219–4221 native case authority with the max evaluated inside each exact original-parent/key group. A recovered effect lowers its exact C_i and N; forgiveness remains separate and does not lower realised gross. Keep all case/component counters, revision increments and APPROVED/CORRECTED events in the same current owner.
3. Origin/capture guards get a closed segment branch which proves the complete same-parent C/H history, current approved-segment membership or exact absence, original taxable native PAYE authority, and actual owner/lease. Permit explained removed-day and mixed-parent shapes; preserve all nonsegment checks unchanged. Extend only 1637's named managed-shape constraint to admit an exactly verified segment N>0 even when parent residual≥0. Additive support/constraint successor is necessary; changing only SQL without this DDL fails.
4. Capture stores one original run_position with its whole truthful residual plus the positive/excluded/recovery children. Managed and positive counts are independent. Hold acknowledges managed capture once **and** creates the one parent run_line/hold for included positive P; it does not use the managed branch to skip B. In the example reserve60, present the existing gross recovery20 to the native case allocation, and preserve day-level conservation; never value a net40 WORK hold as if A's recovery did not exist.
5. Bind an actual case instruction/hold's finite recovery amount to an immutable vector of exact negative-origin children while the existing owner holds Candidate/case locks. A small feature-owned INSERT attachment can retain this vector without rewriting the case allocation/calculator owner. For partial aggregate recovery or write-off, consume the already captured outstanding origins in a documented deterministic native order (e.g. stable segment_key then original effect UUID); store the actual amounts consumed, not proportional guesses. Exact replay must read this vector. Total consumed equals the real RECOVERED event or WRITTEN_OFF amount, never exceeds each remaining origin, and excludes capacity already reserved by another real case hold.
6. Current0316 inserts one genuine negative parent effect. Append a child per consumed original segment share with the same effect_id/case_event_id and signed amount; exact effect children must sum to that effect's source and target gross. Preserve0316's full effect+native membership replay equality and parent update. No new negative payment line, no copy of current approval into a past share.

This is the smallest complete explained-native path. A blanket REVIEW for the two concrete explained examples would leave R2's required correction behavior unbuilt. Truly missing/unsupported attribution remains explicitly unavailable, never seeded by ratios or matching old dates/ordinals.

## Hold/effect membership and seal guards

D18's named `record_work_segment_origin_shares_v1(run_line_id uuid,hold_id uuid)` should insert exact included positive child membership after the hold ID exists in the original hold transaction. It verifies same captured run_work/component/revision and full child totals against the run_line/hold. The immutable hold share must not acquire an effect ID by UPDATE; a distinct append-only effect child links it later. Uniqueness `(hold_id,segment_key)` and `(effect_id,segment_key)` is retained; extra original-share keys on negative consumption must distinguish genuinely multiple old effects for the same key while an aggregated per-effect/key child remains unique.

There is only one current WORK realisation seam: post_simple_outcome in2018. Either append the effect children immediately after its effect INSERT or use a narrowly guarded private feature INSERT attachment which derives them solely from the existing operation_item_id=hold_id. The latter avoids prematurely replacing a later settlement owner; it must prove the exact real outcome owner/member and commit-time REALISED hold. Do not introduce a generic finance callback or a fabricated receipt. Both BANK and INTERNAL_ZERO are covered by this one seam.

Use the parent shared valuer exactly once for the included source operand; preserve valuation_policy_id and actual policy_window_id. Source/target gross share allocation must reconcile to that same valued parent with deterministic penny conservation. Use the captured existing shared quote/policy formula for any needed rounding attribution; do not introduce a new calculator or infer child values from payroll NET/bank cash. Native PAYE gross ex=source and VAT0 remains exact. No independently rounded child sum may alter the parent amount.

Frozen1342 source remains unchanged:

- revision_guard 39–65 validates existing day line/rate/schedule totals;67–82 requires genuine SHIFT detail;
- revision_child_guard96–117 locks the parent revision and forbids insertion after seal;
- effect_immutable188–198 rejects nonINSERT; original trigger bindings remain intact.

New approved-segment child guards must resolve/lock the actual revision, forbid UPDATE/DELETE/TRUNCATE, and require exact stable identity/date/line and complete bucket/gross totals before seal. Add a feature-owned segment seal check, retaining1342's original attachments/body. Do not pass a new table to its generic child guard without the exact approved_line_id shape that it actually reads. Existing SHIFT existence alone does not prove segment completeness. New run_work children and origin/effect rows need equivalent immutable guards, correct composite FKs and private owner/RLS/revokes. No public/browser/service table grant.

## Derived balance, cash and later master editors

`work_segment_gross_balance_v1` is a small indexed read of exact work/component/key origin shares, not a mutable balance or Candidate history query. Join actual parent hold statuses to distinguish ACTIVE/REALISED/RELEASED; sum signed realised effect membership once. Cancellation/expiry release the parent hold and thereby stop that hold contributing H. Return/reissue retain realised C; the exact original cash obligation remains on the transfer and return_cash. Written-off debt is separate; it must not become a negative payroll-gross effect.

New indexes should directly support `(revision_id,component_key,segment_key)`, `(run_id,work_id,component_key,segment_key)`, `(run_work_id,segment_key)`, parent capture membership, hold/effect unique identities and `(work_id,component_key,segment_key,origin_id)` point history. Existing1324 already has work/component effect-history1011–1012 and ACTIVE hold1013–1014 indexes. No agency-wide historical backfill or unexplained legacy adoption is needed.

Master chain boundaries (08:41–76): selection0570 WB36→WB25; RuleB pin WB36→WB40; capture WB36→SP14→SP20→DR38→CM13a→CM03→CM10; hold WB36→SP20→DR38→WB29→CM03→CM19; approval1009/1530 WB36→WB02→DY04→DY06→DY10→DR38. Later full settlement changes are DR38→EX14→EX15→EX19→EX22→EX25→CM13a→SP29→CM13; internal completion EX34→CM13a; returns EX31→EX25; reissue EX32→PS10; cancellation EX27/EX13/EX23; expiry DR08/DR05/FC15; broad collection1519 FC15→SP09→CM13a→CM13→CM21.

WB36 must ship the exact schema, actual selected/captured/positive-hold and native realisation membership, plus the narrow explained-negative qualification/consumption described above. Later cards own their broader economic/rail/cancel/replacement changes. Record narrow R2-required supports explicitly in registry/design so the later owner starts from the genuine current predecessor. Do not author those broader future bodies now, and do not mark D18 complete merely because a future owner has a helper available.

## Source hashes

SHA256 are raw saved bytes at investigation time, not installed-object hashes:

- `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` — `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`
- `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` — `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`
- `supabase/repeatable/10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql` — `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7`
- `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql` — `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`
- `supabase/migrations/03102026_1324_banking_pay_next_core.sql` — `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c`
- `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql` — `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3`
- `supabase/migrations/04102026_1637_bpay_next_work_collection_recovery.sql` — `96c18eba594b864429858b65f1ff8e27225500f7c81b07058f2d17e61012ea9c`
- `supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql` — `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988`
- `supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` — `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6`
- `supabase/repeatable/04102026_0490_banking_pay_next_internal_zero_settlement_v1.sql` — `88a5679e4daeb440c36402f11c866394030af43509fe3520d71bb5399e5ef47c`
- `supabase/repeatable/04102026_1647_banking_pay_next_work_collection_write_off_v1.sql` — `77019761e500193ddd5d8c4147aa0db5bf389c2d1deb02d576407548c0ed4a87`
- `supabase/repeatable/04102026_0230_banking_pay_next_simple_csv_return_v1.sql` — `0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d`
- `supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql` — `5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714`
- `supabase/repeatable/03102026_1548_banking_pay_next_source_chosen_detail_v1.sql` — `c8b8c83d183eb392d7339f8d9cce92a8adadc401d8469fa74082ad1e9921a819`
- `supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql` — `ad1c7a92754f2937edb642dd5ee387f62761ef241cef77d3b792185b2eb22f3c`
- `docs/bpay-next/final-definer-closures.json` — `2dc51e7f79fb45b97905af6bcdc39bef57d172c57e53c111af2ce2ab7b0fd8e4`
