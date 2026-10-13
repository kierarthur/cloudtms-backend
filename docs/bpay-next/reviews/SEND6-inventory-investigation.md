# SEND6 item6 inventory investigation

13 October 2026. Independent saved-source reading only. Read the original owner `codex_outputs/claude-stage2-continuation-20260925/SEND_6_TO_BANKING_PAY_CORRECTED_CANDIDATE.md` and the full `BANKING_FULL_REHEARSAL_13102026/REHEARSAL_REPORT.md`, applicable workspace/repository rules and DATABASE_RELEASE_BIBLE. Read the complete current statement19 DO block, its immediate/deferred negative helpers, current inventory guard, source trigger definitions and current contract/registry. Root alone edits verifier/product source and executes the approved rollback route. This reviewer made no DB/RPC/business/network/test/Git call and no product edit.

**Verdict: item6 remains OPEN. No exact repair is proved by saved source.** The positive fixture declares2 and inserts exactly2. The existing runtime guard already supplies the requested closed head/count evidence in DETAIL; the retained rehearsal discarded it. A narrowly instrumented verifier can preserve the same failure and carry that evidence in MESSAGE for the next approved rollback rehearsal. Instrumentation is not a repair or proof of a clean candidate.

## Controlling owner/rehearsal evidence

SEND6 approves one current-head corrected backend candidate addressing all six items together and permits item6 to remain **OPEN — instrumented, not repaired** if isolated reproduction fails. It forbids guessed zeros, weaker inventory/FK checks and treating cascades as independent FK defects. It requires the approved rollback-only agency TEST route for actual diagnosis, fresh canonical seal/contract and honest executed versus source-reviewed evidence. Promotion/install remain held; no release authority is exercised here.

The report's inventory root is S006754, original file statement19, generated line260223, SQLSTATE55000 / `WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH`, at candidate `a4e87c601bd567b14240af483283d602d21cc715`. The five subsequent0100 failures are source-proved cascades: missing e1→prior-head FK; missing b2→pending bundle FK; missing e1→root authorisation FK, whose failed block also removes e6/f7→charge check/projection publication FKs. Do not change these FKs. They and all assertions skipped within the failed DOs must be re-executed by root after the prerequisite succeeds.

The report did not identify a top-level constraint-mode leak; it scanned the full prefix. It also states that SHIFT/expense shape does not explain this55000 (projection seed count mismatch is23514), and that forced RLS with its unconditional mapped-owner policy does not establish a visibility defect. Those are evidence limits, not permission to guess another cause. The 91 migrations/216 closures having replayed with zero errors does not establish complete installed-contract or fixture correctness.

## Exact current positive and negative fixture

`supabase/verification/17092026_0100_weekly_source_entitlement_schema_v1.sql:917–1281`, `DO $verify_entitlement_heads$`, uses fixed positive head `b0000000-0000-4000-8000-0000000000e1` and bundle…b1.

| Original operation | Current source and established fact |
|---|---|
| Accepted decision bundle INSERT |925–941; proposed head array contains e1. |
| Positive STAGED head INSERT |943–957; `certified_zero=false`, `component_count=2`. |
| Immediate-negative head tests |959–1151; root identity/bundle/revision/uniqueness/receipt-field/zero-shape attacks use the original exception helper and roll back their own mutation. No successful negative row is retained. |
| Positive component INSERT |1153–1164; one statement inserts exactly two rows, ordinal1 SHIFT and ordinal2 EXPENSE, both same e1/bundle1. |
| Actual receipt INSERT |1166–1177; real head/root arrays and original token, no receipt fiction. |
| Activation / bundle completion |1179–1186; activate e1 only after component inventory and receipt. |
| Positive named assertion flush |1193–1196; exactly the head inventory, component inventory and head receipt constraints IMMEDIATE.1197–1200 restores those exact three DEFERRED. No ALL mode change. |
| F3 inventory attack |1203–1213; append ordinal3 inside `expect_deferred_failure`, requiring the real inventory mismatch and rolling the attack back. |
| F4 receipt attack |1216–1221; corrupt receipt digest inside the same helper, requiring actual receipt refusal and rolling back. |
| Final uniqueness/immutability attacks and positive assertion |1223–1279; preserve the real guards and exactly-one-current-head assertion. |

The immediate helper61–84 EXECUTEs a single attack inside a subtransaction, checks exact SQLSTATE/reason and returns after rollback. The deferred helper105–140 EXECUTEs its attack, forces only the three named constraints at118–121, restores those three at122–125 if no error, and catches/qualifies the expected55000 inside that same rollback subtransaction. It does not use ALL IMMEDIATE/DEFERRED.

No `expect_deferred_failure` call occurs before the positive component/receipt/activation sequence. Therefore the helper is not visibly flushing the partially populated positive head between943 and1154. The current file's first explicit successful flush is1193. This disproves that particular speculative source ordering fix; it does not prove the runtime constraint mode at entry.

## Runtime guard and actual diagnostic data

Original schema migration `15092026_1534_weekly_source_plan6_schema.sql:2907–2961` defines the inventory assertion and both DEFERRABLE INITIALLY DEFERRED constraint triggers: AFTER INSERT OR UPDATE on heads, and AFTER INSERT on components.

The inspected current replacement is `supabase/repeatable/22092026_1052_weekly_source_operation_local_validation_v1.sql:2776–2825`, `private.weekly_source_entitlement_head_inventory_assert_v1()`:

- SECURITY DEFINER; search_path pg_catalog,pg_temp.
- Choose `NEW.id` for the heads relation, otherwise `NEW.head_id`.
- Establish operation-local observation storage; a previously checked head can return early at2795–2797.
- Lock/read that exact head's declared count and certified-zero at2798–2803; a genuinely absent head returns without inventing data.
- Count exact same-head components at2806–2808.
- Refuse when actual count differs or certified-zero differs from(actual=0), at2810–2819.
- Cache only the successful head at2821–2822. The same file's inventory-change helper154–168 invalidates that exact cache entry.

The refusal already has SQLSTATE55000 and exact MESSAGE `WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH`, with this **five-key DETAIL**: `{reason_code, head_id, declared_component_count, declared_certified_zero, actual_component_count}`. It includes no name, bank tuple, source payload or personal record. These are the primary data to retain; no runtime guard rewrite or additional count algorithm is needed.

The current generated contract records this guard as private/postgres owner, owner-only EXECUTE, SECURITY DEFINER, volatile/parallel-unsafe, search_path pg_catalog,pg_temp and normalized definition SHA `dca4ecea6f239821906683aba1c73f23fd9741d16d824e0e1f59b769db106302`. The current F04 registry does not carry a separate changed-final entry for this unchanged Source guard; no replacement owner for it was identified in this investigation. That generated contract is source/release evidence, not this reviewer's physical read-back of the failed TEST transaction.

## Possible firing points, not diagnosed causes

The production function is a trigger, so it fires from its real constraint queue, not from a direct ordinary SQL call. The actual failed55000 lacks the retained execution phase/head/counts. The closed possibilities in this DO are:

1. **Positive head stage INSERT**943–957, if that real inventory constraint was already immediate in the executing context. It would see declared2 before the two component rows exist. No such inherited mode is proved by the report/source; do not change mode to hide this possibility.
2. **Positive component/activation/receipt sequence**1154–1186, if another reachable trigger forces the actual inventory queue during those original statements. The saved fixture itself does not add an early inventory SET CONSTRAINTS there. A two-row INSERT is not source proof that a different head/queued event was absent.
3. **Positive named flush**1193–1196. This is the explicit normal firing point and drains those named queues. e1's literal inventory is2/2, but the head that actually failed could be another queued event; only original guard DETAIL can identify it. Do not assume its head equals e1.
4. **Deferred attacks**1203/1216, with the helper's exact expected-error rules. F3's inventory failure should be caught as its intended success; F4 would turn an inventory error into P0001 WRONG_REASON rather than the reported raw55000. Thus those direct paths are less consistent with the retained SQLSTATE/message. This is a control-flow inference, not a runtime attribution.

The other expected-failure attacks similarly wrap their errors and ordinarily produce an EXPECTED_FAILURE_* P0001 if the wrong guard wins. A raw unhandled55000 is therefore evidence to prioritize the positive operations/named flush in diagnostic labels, without ruling out every nested trigger path or modifying any production guard order.

## Verifier-only closed instrumentation recommendation

If root's exact approved rollback reproduction does not establish the cause, retain item6 OPEN and change only the verifier's failing DO/helper diagnostic surface:

1. Add one local finite phase variable. Set literal labels immediately before the unchanged original operations: S19_BUNDLE_INSERT, S19_HEAD_STAGE_INSERT, the finite head-negative groups, S19_COMPONENTS_INSERT, S19_RECEIPT_INSERT, S19_HEAD_ACTIVATE, S19_BUNDLE_COMMIT, S19_POSITIVE_CONSTRAINT_FLUSH, S19_F3_INVENTORY_ATTACK, S19_F4_RECEIPT_ATTACK and final immutable/current-head assertions. Do not reorder, omit or auto-retry any original operation.
2. In an outer DO error handler, GET STACKED DIAGNOSTICS for original returned_sqlstate, MESSAGE_TEXT and PG_EXCEPTION_DETAIL. Handle **only** exact55000 plus exact inventory MESSAGE; every unrelated error is re-raised unchanged.
3. Parse the existing DETAIL as a bounded JSON object and accept only its five known keys/types: original reason constant; UUID head; finite nonnegative integer declared/actual counts; boolean certified-zero. Copy those closed fields and the finite phase into a short MESSAGE suffix, while preserving the original inventory reason and SQLSTATE55000. Limit the emitted message (for example1024 bytes) and include no arbitrary DETAIL/CONTEXT text. The next rehearsal captures MESSAGE even when it discards DETAIL.
4. If DETAIL is absent/malformed/outside the exact schema, emit the same failure with the finite phase and a truthful `DIAGNOSTIC_DETAIL_UNAVAILABLE` marker. Do not substitute zero or claim an observed count. Do not query the head after an outer exception rollback and mistake its absence for an actual0-count observation; the original guard's stacked DETAIL was captured at the firing point.

The existing fixture head UUID may be included as a separate known expected fixture head alongside the actual guard head, so the next result proves whether the failed event was e1 or another head. A phase label identifies the original statement whose queue fired; it is not proof of which of the two inventory triggers generated that event unless the retained native error context separately proves it. Avoid raw unbounded pg_exception_context, function arguments or business row payloads.

This adds no production function/table/trigger/policy/GUC. It changes no constraints, FKs, economic counts, certified-zero values or named publication order. Do not change the guard simply to add trigger metadata: its existing closed DETAIL is sufficient for this first approved diagnostic candidate. Root may also record the raw closed DETAIL in its approved rollback receipt without any source change if the runner retains it.

## Source checkpoint and status

| Inspected file | SHA256 |
|---|---|
|0100 entitlement schema verifier|`c1899930198365bead48d77136b8b5ea1e22ca64905fbd9f498ba6f01a1896b3`|
|22092026_1052 current observation/inventory guard source|`5044b7649c7cb594f57321a78c2ad535d86688e62ef1eff44ae6701ac0b9d8e7`|
|Current F04 registry|`384c04e7d3452806fb508f45cb6af7b4b196dcd60176e5f6e0e8687c2465f60a`|

**SOURCE-REVIEWED ONLY / OPEN — diagnostic design, not repaired.** No reproduction or instrumentation execution was performed by this reviewer. Root's approved rollback route, the actual captured firing/head/counts, skipped internal assertions and current-candidate seal determine subsequent status. A5-style/in-app/payment/timing proof and any install are absent; Policy X and all runtime protections remain unchanged.
