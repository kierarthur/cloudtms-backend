# WB-29 fresh estimate: native current parent census

13 October 2026. Read-only investigation for the proposed private current-selected entry into the same 2357 quote body. Root alone writes product. No SQL/RPC, data, fixtures, tests, browser or hosted actions were performed. This is a concrete source-supported recommendation, not runtime or whole-item acceptance.

## Finding and smallest boundary

**Workbench items are not one parent per segment and are not a complete historical physical parent inventory.** They are financial presentation portions. A native member can have AVAILABLE or RECOVERY plus a separate RESERVED row. A fully realised member with `A=R`, `H=0` and no positive/recovery portion is omitted entirely. `group.complete` proves the underlying current projection qualification and absence of unknown presented portions; it does not supply an expected physical member count.

The smallest no-history-SUM current gather is the bounded union of **all current approved segment keys plus all current projected WORK_SEGMENT keys for this exact component**, with all portions aggregated per key. This yields complete current financial operands, including omitted current fully realised approval and represented removed/signed/recovery/held members. Retained historical keys omitted by the exhaustive native refresh have all-zero current A/R/H and no usable units; do not claim that this union enumerates those physically retained historical keys or borrow a captured expected-member count.

Keep the root's proposed private `current_component_resolution_quote_v1(work,revision,line,paydate,selected_leaf_keys text[],optional_prefix)` gather and seven-argument shared input owner, with the original six-argument delegate supplying NULL current-selection. Its private current-selection operand is a closed server-constructed native representation; no public caller receives or supplies a monetary working set. Same 2357 qualification/plan/1157 arithmetic remains one body. Current and captured modes must be exclusive; no synthetic RunPosition, accepted refs or receipt is introduced.

## Actual current owners and guarantees

| Authority | Exact source and anchors | Meaning |
| --- | --- | --- |
| Native members | `12102026_2146_banking_pay_next_treatment_workbench_members_v1.sql:6–115` | Current approved component/segment keys union retained Position/origin-share keys. Actual per-member A/R/H and target attribution totals qualify against Position. This owner invokes historical balance/effect/hold sums and is **not** the bounded no-history read to call again per estimate leaf. |
| Native refresh | `12102026_2146_banking_pay_next_treatment_workbench_query_identity_v1.sql:193–202,232–234,324–368,445–449` | Splits native members into portions, omits financially exhausted members, conserves each represented member across portions, then makes the group incomplete if an unknown portion exists. |
| Workbench schema | `10102026_1252_banking_pay_next_workbench_summary.sql:32–63,67–179` | Unique `(group_key,component_key,member_kind,member_key,portion_kind)`. WORK_SEGMENT requires `member_key=segment_key`; portions are AVAILABLE/RESERVED/RECOVERY/ZERO/EVIDENCE. No unique one-row-per-segment guarantee or expected-member-count column exists. |
| Current approval | `10102026_1100_banking_pay_next_segment_exclusion_v1.sql:11–32` | Immutable segment PK and revision/line/component/key identities; native five hours/pay cells and Source A. Index `(revision_id,component_key,segment_key)` supports exact component traversal. |
| Existing item scope index | `10102026_1252_banking_pay_next_workbench_summary.sql:178` | `(work_id,component_key,segment_key)` supports this exact component's projected portions. No new cache or history index is required for the proposed gather. |

Current registry confirms the first two 2146 files are the final member and refresh-core owners. Original 2223 refresh code alone is not current authority.

## Exact row qualification

1. Point-read real Work, sealed approved revision/line, Candidate/current target tuple, Position and WORK group. Require approved Work; actual current/applied/Position revision equals the requested revision; line belongs that revision/Work/component; Position A equals actual line Source A; original Source-book/current-publication qualification is retained by the estimate owner.
2. Require group Work/Candidate/current revision/target to agree, `complete=true`, `dirty=false`, `progress_state='CURRENT'`, and current financial-view revision. Every component item used must have the same Candidate/Work/revision/component, group projection/FVR/metadata identity and CURRENT/known state. An inconsistent row is a refusal/PENDING condition, not a row to discard before the census.
3. Scope all raw WORK_SEGMENT items to the exact Work/component; do not filter READY, PAYMENT, AVAILABLE, snooze or query state before parent aggregation. Historical removed keys may have NULL native segment ID. Actual current segment keys must map to exact revision/line/component/key; selected positive members always require a real native approved segment and its exact original vectors.
4. The helper must receive the actual chosen keys from the existing descriptor/choices→leaves join. For each chosen key, require an actual READY/selectable/known/unselected PAYMENT AVAILABLE-or-ZERO item with original current/complete/FVR and exact leaf key. Check current per-segment resolution need by that native segment ID. Omitted, snoozed, query-held, unresolved, selected-in-another-Draft or historical removed positive members cannot become selected because they appear in the census.

## Aggregate portions correctly

For each `(work,component,segment_key)`, sum **all** native portions' `approved_source_ex_vat`, `realised_source_ex_vat`, `active_reserved_source_ex_vat`, `residual_source_ex_vat`, `included_source_ex_vat`, `excluded_source_ex_vat`, `recovery_source_ex_vat`, `held_overhang_source_ex_vat`.

Do not read H from the selected AVAILABLE leaf alone: the native refresh stores H on RESERVED. When AVAILABLE/RECOVERY also exists, RESERVED deliberately zeros its duplicate A/R/residual/overhang while retaining H (`345–355`). Per-member conservation (`357–368`) is across the whole portion set:

`A − R − H = residual = Included + Excluded − Recovery − HeldOverhang`.

Retain signed native A/R. Nominal display amount, deduction balance, selected amount and written-off/manual collection presentation are not substitute parent operands. No target-money-to-units division is permitted.

For a current approved segment that has **no portion at all**, the exhaustive current refresh permits only known zero residual `A=R`, `H=0` with no positive/recovery/overhang. Under all complete/current qualifiers above, reconstruct this exact omitted case from that immutable segment's A; set Included/Excluded/Recovery/Overhang to zero and supply no target units. This is source-proved exhausted membership, not an assumed unpaid/unknown zero. Do not apply this rule when an EVIDENCE/unknown portion exists or the group is incomplete.

For a represented removed key, A is zero unless genuine current native approval exists; preserve its projected signed R/H and attribution, but never give it native units. A removed historical key omitted by the refresh is all-zero and economically contributes nothing. If any supposedly omitted case would be needed to reconcile the component totals, refuse rather than invent it.

The selected quote's Included is the original chosen native Included once per member. All unchosen positive portions become excluded **in the ephemeral selection operand**, including query-held members whose presentation still has native positive Included. Their A/R/H is retained. Do not mutate Workbench rows or treat their presentation `included_source_ex_vat` as proof they were selected. Preserve the same native per-member/final split conservation.

## Completeness and bounds

- Materialise the two physical component inputs with overflow sentinels **before** selection/money/eligibility filtering or aggregation. Reuse the declared 10,000-component-parent/8 MiB working budget and the original 1,000 selected-leaf estimate limit; neither implies a new global Draft limit.
- Raw items can be more numerous than parents. Bound their physical traversal separately before grouping. The schema admits at most five portion kinds per WORK_SEGMENT key, so a 10,000-parent budget admits at most 50,000 scoped raw portion rows, with one extra sentinel; the actual current renderer normally uses at most two. This is a derived private scan bound, not a public list/page/protocol change. If the root selects a smaller explicit working bound, overflow must refuse instead of truncating.
- Bound the approved-segment input with the parent sentinel and the grouped union with the same parent sentinel. Charge full retained identity/portion aggregates/vectors/bucket plan/prefix outputs under 8 MiB; do not build an unbounded aggregate then measure it.
- Require exact sum of current approved segment A equals actual component approval A. Require union parent sums of A/R/H equal the actual current Position A/R/H and all per-parent/final attribution identities. This checks complete financial coverage, including signed/unselected/held/recovery members; matching net residual alone is insufficient.
- Group ready/action/blocked/waiting counts count presentation rows across the Work, not native parents. Do not use their sum as a parent count. The helper's working-member count names only the bounded union, not a fabricated captured/history count.
- The optional prefix must name an actual selected member and follow native segment-key order. All financially relevant earlier parents remain in prefix A/R/H; only selected positive members supply target vectors/Source. Apply the existing cumulative bucket/penny rule in the one shared body.

Nonsegment Additional/Adjustment can use the original exact Position/approved-line/current WORK_COMPONENT path and exact selected native leaf, retaining scalar mode and MANUAL_AMOUNT basis rules. No segment history is manufactured for them.

## Important limitation and required refusal

This recommendation can provide complete **financial** current operands without reading history because the existing complete native projection certifies attribution and the final A/R/H checks retain the current Position. It cannot prove a complete inventory/count of every retained zero-valued historical origin key. If the proposed seven-argument implementation insists on that physical history count rather than financially complete current operands, existing Workbench items cannot supply it; do not silently claim that guarantee. Use the explicit current-mode boundary above, with full financial coverage and no captured count/HEADER assertion.

## Saved evidence

| Source | SHA-256 |
| --- | --- |
| Current 2146 members | `F43AF34F67E9AC1B33EE0AE0D65A11A2A06D956D8B31A4B56D9B4CA032665D17` |
| Current 2146 refresh core | `66BCFA404439221C75CCD23138F95F17EE651D0AFA1789A5611E54FFBB66C522` |
| 1252 Workbench schema | `AA9A42864893856C9AE02479EDB1172CDF39C764A6CA2E9E77F2FD11253AFFAB` |
| 1100 segment schema/index | `0A2E8A72F43BD6DE767E0B277C8C895E02ED29482588A40C23CD51EBD8AC5EF9` |

Source implementation, compile, performance and joined business proof remain separate. No new producer, table, framework, calculator, history SUM, public fields or protocol is recommended.
