# F-23 D06–08 / D12 closure review

10 October 2026. **PASS for the current D06–08 and D12 saved-source boundaries.** The earlier finite findings are closed at the hashes below. This is independent read-only source review: no gate, unit, SQL, runtime, fixture, browser or hosted operation was executed, and no product file was edited. Root's reported source-gate PASS is separate execution evidence. This review does not claim installation, application behavior or completion of unrelated F-23 rows.

## Closed boundaries

- **D06:** the controlling writer exception is the exact `public.bpay_next_owner_transition_v1(jsonb)` signature, with no private same-name or overload waiver. Bootstrap is restricted to the exact frozen module-control `INSERT(id) VALUES(1)`. Actual qualified SQL writes and executable Worker/config/release-source writes are checked. This source allowance grants no owner-transition execution authority.
- **D07:** the actual EX47 marker is `private.bpay_next_test_database_marker`; the gate refuses material marker writes and the second `SIMULATED_TEST` vocabulary in executable SQL. Source comments, documentation and normal test examples remain excluded appropriately. No additional capability or marker was invented.
- **D08:** the gate checks the real `private.bpay_next_rail_capability`. Explicit INSERT tuples permit CSV/EXTERNAL enabled and all finite rails disabled, including SIMULATED/PROVIDER; unknown tuple/column layouts, omitted enabled, provider enablement and unclassified upserts refuse. INSERT/ALTER `ONLY` targets are normalized. ALTER enabled defaults require the complete scalar `false` or `(false)`, not a FALSE prefix. Every UPDATE/MERGE is classified; unknown or row-assignment shapes refuse, an off assignment must be exactly FALSE, and an enabled UPDATE requires the exact CSV/EXTERNAL rail predicate. Rail mutation without proven off state refuses. The former ONLY ALTER, FALSE OR TRUE and row-assignment bypasses are closed.
- **D12:** the policy retains the fixed F-01 base, seven raw-file comparisons, actual eighteen frozen trigger bindings and forty-five core tables. Actual CREATE/ALTER/DROP heads inside DO/BEGIN/IF/LOOP are classified. Frozen trigger/routine authority and relation identity changes are protected; the same protected relation set covers rename/SET SCHEMA, including the shared finance-policy trigger relation, and public/private schema rename refuses. Feature-owned additive columns remain allowed.
- **Dynamic schema FORMAT:** every recognized FOREACH array must be a nonempty complete list of simple identifier string literals. Variables, concatenation and expression elements refuse. The count of **all** `FOREACH v_table IN ARRAY` loops must equal the count of fully recognized literal-only loops, so a second unknown iterator cannot borrow another loop's classification. Iterator reassignment/INTO remains refused; all literal target names, callbacks and fixed policy relations are checked rather than granting a FORMAT-string or file-wide exception.

Fresh normal source units contain the previous default, ONLY ALTER/INSERT, FALSE OR TRUE, row-assignment, rail-mutation, inner-DDL, dynamic-target, multiple-loop and relocation negatives. Reading them was sufficient for this independent source review; this reviewer did not run them.

## Exact additive guard exceptions

The four 0718 capture guards are required by plan **B1.4 / F-06 row 11**. Earlier review wording naming F-05 row 11 was incorrect. The source header's PS-C09/F15/PS-C10 shorthand does not change the actual foundation F-15 card's DB-none scope.

| Relation | Exact trigger | Timing/events and level |
| --- | --- | --- |
| private.bpay_next_run_work | bpay_next_run_work_frozen_immutable_v1 | BEFORE UPDATE OR DELETE, ROW |
| private.bpay_next_run_work | bpay_next_run_work_frozen_no_truncate_v1 | BEFORE TRUNCATE, STATEMENT |
| private.bpay_next_run_line | bpay_next_run_line_frozen_immutable_v1 | BEFORE UPDATE OR DELETE, ROW |
| private.bpay_next_run_line | bpay_next_run_line_frozen_no_truncate_v1 | BEFORE TRUNCATE, STATEMENT |

All four call exact `private.bpay_next_effect_immutable_v1()` with empty arguments. The two Rule-B additive snapshot attachments are equally narrow: `bpay_next_run_work_query_snapshot_immutable` (BEFORE UPDATE OR DELETE, ROW) and `bpay_next_run_work_query_snapshot_no_truncate` (BEFORE TRUNCATE, STATEMENT), both on `private.bpay_next_run_work_query_snapshot` with that exact empty-argument callback. The six exceptions validate actual relation/name/events/level/callback; they replace none of the original eighteen bindings and grant no callback-wide or file-wide waiver. No Rule-B protocol investigation was reopened.

## Historical ROWTYPE handling

The retained 2018 file is byte-identical to its object at `3c91e5e6ce35b5833a5c218b77b741261eeebafa`. The gate pins that reconciliation commit, requires its ancestry and unchanged entire source, and permits only its exact historical ROWTYPE_SELECT occurrences for four enumerated signatures after each actual final definition passes current checks. Non-ROWTYPE hazards and changed historical bytes receive no waiver.

The 0316 successor contains exactly those four definitions: bind_simple_csv_destination(uuid), issue_simple_csv(uuid,uuid,uuid), receive_simple_reissue(uuid,uuid,uuid), and apply_work_collection_recovery(uuid,uuid). Independent static definition diffs against 2018 show one SELECT projection change per body (`*` to the actual `u.*`, `w.*` or `l.*` alias), preserving the remaining body and locking behavior. The registry's physical winners point to the successor. This is a bounded repair of current definitions, with the old full file preserved.

## Existing CI seam

Both existing database-release and supabase-migrate workflows include the normal source-gate and legacy-fence unit files after their existing database/coupling checks. The reviewed-range final-definer checks and normal npm test paths remain. Changed-source filters cover the relevant scripts, units, registry, Worker source and configuration. No extra workflow, fixture harness or release engine was added. Local execution/installation receipts remain root's responsibility.

## Reviewed current SHA-256 receipts

| Path | SHA-256 |
| --- | --- |
| scripts/bpay-next-source-gates.mjs | 7cbdba5eccca9c83191f872f4fc60f9199e42287d8fbbb2524772c34fa10c55c |
| tests/bpay-next-source-gates.test.js | 270978104f70e65bfa10750fa4362a152a08f3bc0519428688e7af80dfbb1098 |
| docs/bpay-next/final-definer-closures.json | c5b5a6f5790f368bc1d52062f6c847251a8fc729ecebdefd556106968912943c |
| .github/workflows/database-release.yml | 602168afac04f4ed08a826d1714d9060780742d95559408ac037eb0866b851a1 |
| .github/workflows/supabase-migrate.yml | 7f578a7e053243a1da495868287db8a290ce39a30cb5a1e64eddcbab01760a70 |
| supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql | 98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6 |
| supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b |
| supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql | 2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf |
| supabase/repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql | 0a884c8cb2568ca568ac51069fcf8fac945159a7f3f4d688d57b1731c295feae |

No remaining finite source finding in the requested D06–08/D12 scope at this snapshot. Prior reviews record history and are superseded for these current bytes.