# F-23 exact additive-authority false positive and two new ROWTYPE reads

10 October 2026. Independent read-only source findings. No gate/module/tests/SQL/runtime/fixtures/hosted execution or product edit. Rule B remains closed; this reviews its unchanged source only for the F-23 classifier. Prior reviews remain historical. Root authors any needed gate/successor changes.

## Exact Rule B false positive

`supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql:1202–1206` defines exactly:

```sql
create trigger bpay_next_run_work_query_snapshot_immutable before update or delete
 on private.bpay_next_run_work_query_snapshot for each row execute function private.bpay_next_effect_immutable_v1();
create trigger bpay_next_run_work_query_snapshot_no_truncate before truncate
 on private.bpay_next_run_work_query_snapshot for each statement execute function private.bpay_next_effect_immutable_v1();
```

These are new required immutable attachments on the additive Rule B snapshot relation, outside the original45core relations and18trigger pairs. The unchanged generic callback is legitimately reused. Original18bindings and callback body are unchanged. Current D12’s callback-name blanket classifies the CREATE as competing solely because the callback belongs to its policy.callbacks set; no frozen object is redefined or rebound.

**Smallest reviewed correction:** allow only these two exact relation/name/events/ROW-or-STATEMENT/callback/empty-args CREATE shapes, with corresponding original exact drop/recreate. Do not waive0151, all snapshot statements or all callback reuse. A general classifier can instead distinguish additive binding on a genuinely new feature-owned relation from competing binding on a frozen relation, but that is wider than this immediate need. Preserve the previously confirmed exact four0718 guard attachments independently. No snapshot redesign/Rule B reopening is necessary.

## Two genuine newly introduced ROWTYPE reads

F-01 base is `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. F-05 provenance explicitly records these as insert/replace deltas. Current final registry defines both in committed2018, whose local source was added by `fee38677` (Build NEXT SQL foundation with ordered successors and final authority gates). The source-check question is new-vs-inherited style; it is not an assertion of hosted installation.

| Signature / card row | New statement at current final2018 | Exact F-01 original | Minimal correction |
| --- | --- | --- | --- |
| `private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)` / F-05-D12; original0240 installed older origin/test420513c6 |2018:2563 `select * into strict v_worker from private.bpay_next_run_worker where id=v_original.run_worker_id for update;` |0240:31 was `perform 1 from private.bpay_next_run_worker where id=v_original.run_worker_id for update;`; there was no v_worker ROWTYPE local. F05 provenance replaced this lock-only read and added the local for the required Umbrella branch. | `select w.* into strict v_worker from private.bpay_next_run_worker w where w.id=v_original.run_worker_id for update;` |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` / F-05-D17; original1519 installed older origin/test420513c6 |2018:4295 `select * into strict v_source_line from private.bpay_next_run_line where id=v_basis.original_run_line_id;` |No v_source_line local/read existed in F-01 original1519. F05 provenance inserted that source-line read and source-book frozen linkage/replay checks. | `select l.* into strict v_source_line from private.bpay_next_run_line l where l.id=v_basis.original_run_line_id;` |

Every other SELECT* whole-row read in these bodies is inherited from the unchanged original owner and should keep its exact baseline allowance. Do not expand into a source-style cleanup or change locks/filters/read order. The two new alias.* projections read the same columns/rows and retain strictness/lock behavior; no economic or lifecycle change is indicated.

**Recommended product delivery:** a new ordered style-only successor containing only these two full current final definitions, changing only the two statements above. Preserve exact signature, language, SECURITY mode, search_path, all decisions/financial values/locks/replay, OWNER/REVOKE and final NOTIFY. Add only these two current final-root/physical/allowed/predecessor/final-closure links to the existing registry. Preserve committed2018 and old0240/1519 bytes. Do not add a money owner, journal, trigger, schema, data snapshot or business test.

The ordinary future master chains stay0240 EX-32→PS-10 and1519 FC-15→SP-09→CM-13a→CM-13→CM-21; this is a finite foundation source-style successor, not completion of those cards.

### Source-gate integration

Current D09 examines every changed physical SQL since F-01, so adding a corrected successor still leaves those two exact committed2018 statements visible. Preserve immutable2018 and admit only the exact two historical superseded occurrences/signatures/source SHA **after** their final registry points to the reviewed alias.* successor; keep checking each new final definition and every new/mutated occurrence. A final-definition-aware ROWTYPE check for this exact pair is another finite option. Do not waive all nonfinal functions or the entire2018 file, and do not manufacture them as F-01 inherited statements.

## Other closure corrections

Latest source read confirms full-array enumeration, no-v_table reassignment, public.settings_finance_windows rename/SET SCHEMA identity check and private/public ALTER SCHEMA RENAME refusal were added. Full closed-literal validation and full FALSE defaults require the exact current follow-up bytes; parent is fixing concurrently. This note does not supersede fresh final gate review or claim those pending code paths PASS.

Factual correction to earlier guard notes:0718 is **F-06 row11** (01:550/575), with B1.4 exact authority; the source comment uses PS-C09/F15 shorthand. F-15’s actual owner-status card says DB none. This does not alter the required four attachments or parent’s narrow source exception.

## Current source receipts

- `scripts/bpay-next-source-gates.mjs`: `db1a8cdd32e3cc56aff875051426ae63c9d431f6a899fc016fe3be7d801b6efb`
- `tests/bpay-next-source-gates.test.js`: `982c993bf9b54931e73472348693442479751ab0d07193c93c57800708fbb0f5`
- `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql`: `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf`
- `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`: `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`
- `supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql`: `5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714`
- `supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql`: `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988`
- `docs/bpay-next/F-05-successor-provenance.json`: `b226a2b6c03a0f3677348b97f1324a2c49cb1dd96f49902d4b70fb1398dcc520`
- `docs/bpay-next/final-definer-closures.json`: `c7a5c1d2ac41c06128cdb4587b9b373b14400b5badeaace41b0de253df361bbb`
