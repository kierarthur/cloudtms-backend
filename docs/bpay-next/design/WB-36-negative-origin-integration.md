# WB-36 exact negative-origin reservation and consumption

2026-10-10. Read-only source design against actual current files. No product edits, SQL/runtime/tests/business calls, fixtures, data or network operations. No new command/agency clock/producer protocol, no per-shift payment or Case, no NET allocation. This supplements `WB-36-money-integration.md` and corrects its provisional aggregate forgiveness/protected-capacity formula as stated below.

**Minimum complete integration:** one existing collection/case/component per actual `(work_id,component_key)`; immutable attribution from each native collection recovery hold to the exact original positive WORK shares it reserves; signed recovery effects consume that held vector; write-off events retain a separate forgiven vector. All economic counters/lifecycles remain with existing native owners. Explained paid/changed/reordered/moved segments must work; missing genuine origin evidence must not be guessed.

## Actual native owner map

| Exact signature | Current physical winner | Required seam |
|---|---|---|
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql:3955–4260` | Segment branch computes the genuine same-parent negative vector, retaining actual original effect anchor, owner/lease and observed revision. Creation principal currently4191, existing-case formula4219–4221, counter/event writes4193–4243. |
| `private.bpay_next_work_collection_origin_v1(uuid,uuid,uuid)` | `04102026_1519_banking_pay_next_work_collection_v1.sql:11–60` | Current READY qualification is exact ordinary/Source native taxable PAYE, observed parent equality, one existing case/component/rule. Add only the complete exact segment branch, including removed original day parents. |
| `private.bpay_next_work_collection_capture_guard_v1()` / `private.bpay_next_work_collection_guard_v1()` | same1519:66–157/160–297 | Required shape/owner/observed-counter qualification for the new branch; preserve existing nonsegment behavior and original recovery/cancel/expiry owner checks. |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | Actual DRAFT allocation inserts immutable result1061–1070, locks case/component/period1072–1075, inserts ACTIVE case_hold returning v_hold_id1076–1080, then capacity_use1081–1083 and native active counters1092–1097. A narrow feature-owned AFTER INSERT case_hold attachment can reserve exact negative origins here without a whole case-allocation successor. |
| `private.bpay_next_case_hold_guard_v1()` / `private.bpay_next_case_capacity_use_guard_v1()` | `04102026_0310_banking_pay_next_case_guards_v1.sql:211–280` | Existing ACTIVE insert/result equality; only ACTIVE→RELEASED/REALISED transitions; immutable identities; exact realisation event. Reuse unchanged. |
| `private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint)` | `04102026_0410_banking_pay_next_case_transfer_post_v1.sql:352–528` | Actual BANK/INTERNAL_ZERO case owner. Locks actual case/component/period414–416 and exact hold/use419–425; inserts real RECOVERED event430–437; updates native counters500–517; sets hold/use REALISED520–522; invokes actual recovery helper524–525. Reuse unchanged where possible. |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql:414–494` | Current signed WORK recovery effect447–459; exact duplicate replay460–477; parent source/target reduction479–489. Its operation_item_id is **RECOVERED case_event.id**, not the case hold ID. Append signed effect attribution from that event's actual held vector. |
| `private.bpay_next_apply_write_off_v1(uuid,uuid,bigint)` | `04102026_1647_banking_pay_next_work_collection_write_off_v1.sql:268–343` | WRITTEN_OFF event325–327 precedes native written-off counters328–331 and terminal receipt337–341. Attach separate exact forgiven-origin membership; never manufacture a WORK financial effect. |
| `private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_0460_banking_pay_next_case_cancel_v1.sql:156+` | Native RELEASED hold/use334–335 releases its derived negative-origin reservations automatically; retain immutable membership, no deletion or reconstruction. |
| `private.bpay_next_expire_preparation_worker_page_v1(uuid,uuid,bigint,bigint,integer)` | `04102026_0510_banking_pay_next_preparation_expiry_v1.sql:412–463` | Native RELEASED hold/use458–459 similarly releases reservation. Keep actual cancellation/expiry/lease/counters unchanged. |

The current collection instruction shape (migration1637:60–65) is **taxable PAYE OVERPAYMENT, RECOVERY/DEDUCTION/GROSS_DEDUCT, GROSS_RECOVERY**. The negative-origin reservation attachment must not extend every generic Case or NET capacity hold. Other purposes pass through untouched. Future Case/NET/replacement/move owners retain their own master positions.

## Required small schema

Use existing1100 original WORK tables `work_segment_origin_share`, `work_segment_effect_share`, `work_segment_effect_origin` and `run_work_segment_parent`; do not duplicate their gross history.

Recommended new normalized rows (names are proposed design, not a claim they exist):

1. `private.bpay_next_work_segment_recovery_share`: immutable PK `(case_hold_id,origin_share_id)`. Store real work_collection_id, original positive origin_share_id, original work/component/segment key, captured approved revision/approved-segment basis (nullable only when exact complete revision proves absence at this parent), exact `reserved_source_ex_vat > 0`; optional captured N/W/previously-active operands if required for later source audit. FK to actual case_hold and exact origin_share composite scope. Case_hold already retains instruction/result/case/component/Candidate/week identities; validate those through the actual parents rather than invent a new request or economic unit. Index `(work_id,component_key,segment_key,case_hold_id,origin_share_id)` and case_hold prefix.
2. `private.bpay_next_work_segment_write_off_share`: immutable PK `(case_event_id,origin_share_id)`, same original work/component/key and positive exact `written_off_source_ex_vat`. FK real WRITTEN_OFF event and original positive share; exact event/request/collection scope validated by INSERT guard. Index `(work_id,component_key,segment_key,case_event_id,origin_share_id)`.

One case hold may reserve several segment origins, and one segment may have multiple genuine positive paid origin shares. Do not require one original effect for the whole vector. The existing collection original_effect_id/run_line pointers remain a genuine first anchor, not authority to disregard later exact credit. Negative effect_share remains unique `(effect_id,segment_key)`; its existing effect_origin children identify every consumed original share. Thus aggregate one effect/key while retaining multiple real old origin IDs.

Extend the existing work_collection with a narrow segment-attribution discriminator and the currently observed exact aggregate negative basis needed by its guards. Use existing reconcile_revision, observed_revision_id and observed parent gross fields. No new agency order/version clock. A retained per-reconciliation origin observation can be added only if an actual guard/caller needs it; actual reservation and consumption rows already carry immutable historical authority. Do not FK old children to the collection's mutable current reconcile_revision value.

Rows are owner-private, RLS enabled, explicit logical owner/revokes, no browser/service table grants, immutable UPDATE/DELETE/TRUNCATE. INSERT qualification proves current actual owner and all parent identities/amounts; individual UUID FKs alone are insufficient.

## Correct same-parent debt and protected capacity

For i = actual `(work,component,stable segment_key)` beneath one original component:

- `A_i` exact source gross at this parent in the complete current/captured approved revision (or proven0 when removed/moved away).
- `C_i` realised source credit, from signed native effect_share history at this same original parent. PAYROLL_SETTLED adds, real WORK_COLLECTION_RECOVERED subtracts.
- `N_i = greatest(C_i − A_i,0)` — genuine paid excess. ACTIVE WORK holds are not realised C and cannot become negative paid debt.
- `W_i` retained exact forgiven-source share sum for this origin, from WRITTEN_OFF children. Forgiveness does not subtract C.
- `H_i` sum of recovery_share.reserved_source whose actual case_hold is ACTIVE, at this exact origin. Released and realised holds no longer reserve this capacity.

**The max must remain inside each origin group:**

`outstanding_obligation = SUM_i greatest(N_i − W_i, H_i, 0)`

`principal_approved = actual_total_recovered + actual_total_written_off + outstanding_obligation`

This preserves the existing native formula4219–4221 while preventing unrelated segment origins offsetting each other. The preliminary money note's scalar `greatest(sumN−totalW,totalH,0)` is superseded for segment attribution.

Two real counterexamples explain why: forgiven A20 must not erase newly negative B20; protected active recovery A20 whose current N becomes0 must not consume the free B20 obligation. With inside-origin max, native active capacity is `sum H_i`, and free recovery/write-off capacity is `sum greatest(N_i−W_i−H_i,0)`. Existing recovered/written-off event counters remain unchanged and conserve the case/component parent.

Before admitting the new branch, complete origin census must reconcile C_i to parent realised source, all WORK active shares to parent held source, W_i to native case written_off, and H_i to native active_recovery_hold_amount/component/use rows. Missing existing child evidence is not zero and not a guessed seed. Retain nonsegmented predecessor qualification unchanged. Clean launch needs no historical backfill.

For mixed A−20/B+60, N_A20 and positive B60 stay distinct under one existing day parent. Reserve WORK B60; one existing day collection case owns recovery20. Existing native GROSS_DEDUCT contributes−20 to payroll preparation. No net40 WORK hold and no new per-shift Case. Moved A→B keeps A's negative origin at A and B positive at B; Source keeps its real SOURCE component.

## Reservation: one real case_hold, immutable vector

Recommended private helper signature:

`private.bpay_next_record_work_segment_recovery_shares_v1(p_case_hold_id uuid) returns void`

Call via a narrowly scoped private **AFTER INSERT on bpay_next_case_hold** for only the actual collection-backed GROSS_RECOVERY DRAFT path. Required parent facts:

- actual hold ACTIVE; exact immutable result/instruction tuple, result pass DRAFT and equal source/target amounts;
- instruction.work_collection_id links same real case/component/Candidate; original recovery shape/source tax native PAYE; collection/current segment observation qualified and same approved basis;
- the actual PREPARE job/module/owner/lease/FINANCE_ALLOCATE state is qualified by the existing owner; no new late module lock or Source admission;
- actual Candidate/case/component/period locks are already held by the native insertion path.

At this callback, **capacity_use and active case counters have not been inserted/incremented yet** (0151:1081–1097 follows hold INSERT). Do not require that future row as an immediate precondition or move native statements. An end-of-transaction constraint may require the genuine capacity row and exact committed parent/child totals. Immediate reservation checks validate native result/owner and previously committed active reservations, excluding this new hold.

Compute the exact available negative vector from complete current/captured same-parent source origins, less attributed forgiveness and **other** actual ACTIVE recovery reservations. Use every actual eligible origin; do not pick an arbitrary latest effect or redistribute across parents. Within a stable key with several original paid shares, assign a documented deterministic consumption order over the full retained set (for example native positive effect command sequence then effect UUID/origin_share UUID); this is child attribution of one real aggregate amount, not a new Case priority, rate or NET choice. Freeze that actual vector on the hold. The sum equals hold.source_reserved_ex_vat; no child exceeds its available original credit/debt/capacity.

For future replay, current approval may change but this held vector does not. Protected H_i remains on its exact key until original release/realisation. Retain it through payroll/cancellation; no resizing or updating old rows. Any native NET replacement creates a new genuine hold only through its later owner and is not implemented by WB36.

## Consumption: bind to actual RECOVERED event and negative effect

Recommended private helper:

`private.bpay_next_record_work_segment_recovery_effect_v1(p_effect_id uuid) returns void`

Minimal current integration is a feature-only financial_effect AFTER INSERT attachment for **WORK_COLLECTION_RECOVERED**, or one call immediately after0316's INSERT returning v_effect_id. Choose one seam, never both. This avoids rewriting the future full case transfer/settlement owners.

Resolve actual `case_event.id = effect.operation_item_id`; that event.operation_item_id is its real case_hold ID. Resolve instruction/result/collection via the event. Validate the complete original0316 tuple: real operation/job, RECOVERED type, actual transfer/member, native Case capacity/use, source/target amount equality, actual PAYE source basis, real case/component/Candidate. The original0410 owner has already made the hold and use REALISED and linked realisation_event_id before calling0316 (520–525). Here, requiring REALISED is correct; it would be incorrect in initial reservation.

Read only the frozen recovery_share vector for that exact hold. Insert negative effect_origin amounts against its original positive shares, grouped into negative effect_share by exact component/key. Native collection currently has source=target ex=inc, VAT0, so each child target negative equals its real consumed source amount; do not apportion any payroll NET or original bank cash. Children sum exactly to real event.source_amount and all signed parent effect amounts. Verify no original positive share is over-consumed by earlier signed recovery effects; these are immutable source-disposition facts, not paid cash guesses.

Preserve0316's once-only native parent reduction and exact ON CONFLICT replay. An AFTER INSERT callback naturally runs only for a genuinely new effect. If using an explicit helper call, its duplicate path must compare the existing exact child vector without another parent/case update. No missing child set can be silently treated as successful replay.

## Forgiveness: separate real WRITTEN_OFF vector

Recommended helper:

`private.bpay_next_record_work_segment_write_off_shares_v1(p_case_event_id uuid) returns void`

Feature-only AFTER INSERT case_event attachment for the existing1647 WRITTEN_OFF path. Resolve exact write_off_request by event.operation_id, collection by its real work_collection_id, original command/job/lease and locked case/component. Validate the original request expected_component_revision/basis_case_revision and native amount/protected-capacity rules unchanged.

The existing event contains written_off_delta and real case/operation/item identifiers; it is not a typed RECOVERED event and may not have instruction/allocation_result/WORK source fields. **Do not require or invent those fields.** Derive actual written-off quantity from event.written_off_delta and the existing request. Reserve only free negative origin capacity after exact W_i/H_i; retain its immutable vector in write_off_share. This event occurs before native counters/terminal receipt update, so commit-time validation confirms the original counter/receipt completed; immediate qualification uses actual original owner and pre-update operands.

No financial_effect, no reduction of position.realised_source, and no negative effect_share for forgiveness. Future N and case principal read W_i on the exact same origin. A later correction can reduce that origin's current debt without erasing historical W or applying it to another segment.

## Lock and lifecycle closure

Keep original outer order: module/read admission → Candidate worker_control → actual job/run_worker → existing WORK/position or case/component/period → actual hold/use. New append callbacks perform qualified reads under those already-owned locks and insert children; do not acquire a new WORK/position, Source key, module row or agency clock after case locks. The actual collection reconciliation owner keeps its original WORK/position serialization; derive complete attribution there, not in a deferred view callback.

| Native transition | Derived origin consequence |
|---|---|
| ACTIVE collection hold created | Immutable recovery_share reserves exact H_i. Commit proves real capacity use/result/counters. |
| ACTIVE→RELEASED by cancellation/expiry | Existing hold/use and counter owner releases H_i by status join; share rows remain immutable. No new release command, deletion or fabricated effect. |
| ACTIVE→REALISED with genuine RECOVERED event | The exact held vector becomes negative effect membership; H_i disappears and C_i falls once. |
| Real WRITTEN_OFF event | W_i rises on its exact original vector; C_i unchanged. |
| Bank return or cash reissue | Gross/recovery/forgiveness remains. Only native original cash obligation reopens/reissues. |

Source remains bound to actual SOURCE component and its existing certified Source book; ordinary remains WORK:DAY. The exact stable key can have several historical parent-attribution children, but no credit moves between those parents.

Bound the upstream member preparation with the builder's existing capture pages/finite native cursors and exact work/component indexes. Do not grow one Candidate-wide JSON or add a new queue/proof programme. The new helper must consume an already complete finite basis or use the existing owner continuation for large normalized vectors; an arbitrary new product cap or incomplete child sum is not success. Native parent economics, real capacity limits and original receipts remain authoritative.

## What WB36 changes now versus later owners

Now: normalized reservation/forgiveness schema; actual immutable owner/scoped INSERT guards; native hold/effect/event attachments; narrow1519/current2018 negative qualification and inside-origin principal formula; exact captured segment positive/negative counters and the1637 managed shape support; actual negative effect vector consumption. Named R2 negative-origin seams require these supports, not an unused helper for a later editor.

Reuse current0151 case allocation and0310/0410 lifecycle through scoped child attachments. Later WB27/DR/FC/EX/CM cards own their complete allocation, NET replacement, execution/cancellation/move and broader collection changes. No signatures/security/math of those wider owners are changed merely to provide the WB36 attribution vector. Register any genuinely replaced current1519/2018/0316 definition with the exact predecessor and narrow R2 reason; installed predecessors remain immutable.

## Raw source snapshot hashes

- `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` — `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`
- `supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql` — `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988`
- `supabase/migrations/04102026_1519_banking_pay_next_work_collection.sql` — `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3`
- `supabase/migrations/04102026_1637_bpay_next_work_collection_recovery.sql` — `96c18eba594b864429858b65f1ff8e27225500f7c81b07058f2d17e61012ea9c`
- `supabase/migrations/04102026_0300_banking_pay_next_case_instruction_core.sql` — `04b44425d288e714037012c3b4eb4d27679137905e1c380682812cc8273ad40e`
- `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` — `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`
- `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql` — `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`
- `supabase/repeatable/04102026_0410_banking_pay_next_case_transfer_post_v1.sql` — `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8`
- `supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` — `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6`
- `supabase/repeatable/04102026_1647_banking_pay_next_work_collection_write_off_v1.sql` — `77019761e500193ddd5d8c4147aa0db5bf389c2d1deb02d576407548c0ed4a87`
- `supabase/repeatable/04102026_0460_banking_pay_next_case_cancel_v1.sql` — `4417b25f96d6caefb627eae013c0c956268356104be15e2411dead56cc60bcf5`
- `supabase/repeatable/04102026_0510_banking_pay_next_preparation_expiry_v1.sql` — `f4eb08c3c963c1850396642fd98947cf27722d96e20b6bb77db394635c3341aa`
- `supabase/migrations/10102026_1100_banking_pay_next_segment_exclusion_v1.sql` — `7cb60e23c875f83632ea33e549664b85f3821ad6f8de5672dcc18abb9381160f`
- `supabase/repeatable/10102026_1100_banking_pay_next_segment_authority_v1.sql` — `d6e22fa9320175ee789e071aa7d77128006b8c388448ba29a3abd6b7f514b0d5`
