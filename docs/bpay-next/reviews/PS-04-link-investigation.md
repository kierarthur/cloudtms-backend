# PS-04 immutable recovery-link investigation

10 October 2026. Read full PS-04 card, Plan 06:1394–1413, all exact checklist rows, current registered defining owners and bounded actual schema/guard/post/recovery/write-off source. Read-only source investigation; no tests, gates, SQL, fixtures, runtime, network, secrets or product edits. Worker/Screen are explicitly none. Umbrella recovery shape belongs to later CM-14 and is not delivered here.

**The actual universal ordinary WORK recovery link already exists: `financial_effect.operation_item_id = case_event.id`, for `effect_kind='WORK_COLLECTION_RECOVERED'`.** The writer checks it and replay is already bounded; both rows and original paid provenance are immutable. The missing finite database enforcement is a unique recovery-event link plus an INSERT qualification of that actual identity. `source_case_event_id` is not a universal replacement: it is populated only for Source-bound original lines, is NULL on retained unbound ordinary recovery and has no unique constraint.

## Exact owner and identity map

| Object | Current authority / role |
|---|---|
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | **10102026_0316**:414–491 is the registered final owner; 1519 and 2018 are its predecessors. Ordinary recovery effect INSERT/replay and position reconciliation. Do not reinstall the earlier body. |
| `private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint)` | 0410:352–541; actual Case member/outcome/result/hold posting, invokes the current apply helper only for a real work-collection instruction. |
| `private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` | Final 2018:2298 onward calls the retained 0410 owner for CASE, separates WORK and CASH_REISSUE. |
| `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | Final 2018; keeps the originally paid basis and qualifies the actual recovery effect/event/instruction/hold before observing its remaining native WORK disposition. |
| `private.bpay_next_work_collection_guard_v1()` | 1519:160–298; original paid identity cannot change; recovery update must bind current genuine event/effect/hold/capacity/request and original collection. |
| `private.bpay_next_case_event_insert_guard_v1()` | 0310:120–157, named D02 owner; requires actual instruction/result and verifies original financial pass and exact source/target amounts. Its seven sibling guards need no unrelated rewrite. |
| `private.bpay_next_effect_immutable_v1()` | 1342:188 onward; both Case events and financial effects have BEFORE UPDATE/DELETE triggers invoking it. All existing and additive link fields are frozen automatically. |
| 1647 write-off owners | Source forgiveness only: WRITTEN_OFF event, original recovery counters unchanged and no new financial effect. Never classify this as a RECOVERED pair. |

0316:423–443 selects the actual job/event/instruction/collection and requires active NEXT epochs, actual leased settlement MEMBERS phase, same Candidate/Case/component, positive RECOVERED source, original collection policy, and exact PAYE source/target values. It also requires the event's real hold/capacity to be REALISED, capacity `realisation_event_id=event.id`, the actual outcome request/recovery transfer, and its actual immutable member instruction/result/hold. These are existing real financial facts; no new receipt or protocol is needed.

The resulting effect stores:

- `operation_id = event.operation_id = job.command_id`;
- `operation_item_id = event.id` (**the effect's item is the event ID; the event's own item is its actual Case hold ID**);
- `effect_kind = 'WORK_COLLECTION_RECOVERED'`;
- original collection `candidate_id`, `work_id`, `component_key`, `case_id` and actual original WORK Timesheet identity;
- `original_transfer_id = event.original_transfer_id`, the real transfer that realised this recovery; the originally paid transfer remains separately frozen in `work_collection.original_transfer_id`;
- source/target ex/VAT/inc values equal the negative of that single event's positive amounts; same occurrence time;
- Source-bound certificate fields copied from the genuine original frozen run line, with `source_case_event_id=event.id` only when that original line is Source-bound.

Existing effect uniqueness is `(operation_id,operation_item_id,effect_kind)`. INSERT uses that exact conflict target, then validates stored identity/amount/provenance and returns without applying the position change again on replay (0316:459–477). The Case event has its own unique `(operation_id,operation_item_id,event_kind)` where its operation item is the hold. Those constraints alone do **not** provide a cross-table FK or prevent reuse of one event ID under another effect operation/kind unless its writer's checks are obeyed. There is no universal unique recovery link in the schema currently read.

## Finite support recommendation

Use the existing actual effect operation-item link rather than adding an invented member/source field or replacing financial owners. A partial unique index on `private.bpay_next_financial_effect(operation_item_id) WHERE effect_kind='WORK_COLLECTION_RECOVERED'` closes duplicate represented recovery events, including concurrency, while preserving the existing replay conflict target. Qualify INSERT of that exact effect kind against its actual immutable event:

```sql
ce.id = new.operation_item_id
and ce.event_kind = 'RECOVERED'
and ce.operation_id = new.operation_id
and ce.case_id = new.case_id
and ce.candidate_id = new.candidate_id
and ce.original_transfer_id is not distinct from new.original_transfer_id
and ce.source_amount_ex_vat > 0
and (new.source_disposition_ex_vat,new.target_amount_ex_vat,
     new.target_amount_vat,new.target_amount_inc_vat)
    is not distinct from
    (-ce.source_amount_ex_vat,-ce.target_amount_ex_vat,
     -ce.target_amount_vat,-ce.target_amount_inc_vat)
```

Also require the actual `ce.instruction_id` to bind the genuine collection whose work/component/Candidate/Case matches this effect, and the existing actual hold/capacity/member/request proof above. A same-UUID bare event lookup, an original paid `original_effect_id` lookup or a same-amount comparison is insufficient. Null identity must refuse; it must not pass a SQL UNKNOWN comparison.

The card names the current Case event guard, not every money owner. One finite successor of that guard can retain its complete original Case-event branch and add a table-dispatched financial-effect INSERT branch, attached through a narrow BEFORE INSERT trigger for this pair qualification. This is support for the named immutable link, not a new writer. The event is inserted first and the hold/capacity is realised before 0410 invokes the effect helper, so this direction requires no deferred transaction framework or business-body reorder. Alternatively an explicit conditional generated-link/composite-FK schema design needs its own bounded evidence; it is not required merely to rename the already real link. Retain current owner signatures/bodies/ACL and all unrelated event/effect kinds. Additive evidence must not rewrite 0316, 2018, 0410, Source producers or money calculations unless an actual implementing defect is separately found.

The existing immutable guard freezes `operation_item_id`, `operation_id`, kind and every amount; no extra UPDATE/DELETE scheme is required. Preserve source-binding shape: universally setting `source_case_event_id` would violate 2010:194–200 for unbound ordinary effects.

## Original provenance and one recovery

1519 schema records unique `original_effect_id`, real original frozen run line/revision/approved line/transfer/channel/tax/policy and compound WORK/Candidate/position/revision links. Its guard rejects DELETE and any original-basis UPDATE (1519:167–184), verifies the original positive PAYROLL_SETTLED effect against that frozen line and real realised WORK hold (190–210), and retains immutable Case identity once attached. Current revision/observed caches do not replace these originals. `last_effect_id` is a current reconciliation pointer, not a replacement original and not the new permanent recovery link. Moved-work reporting must retain this exact paid origin; this card supplies no current-Contract/Client guess.

0410 performs one genuine Case recovery: it inserts one RECOVERED event with actual instruction/result/hold identity, updates component/Case principal and period once, and makes that actual hold/capacity REALISED. At 524–525 it invokes WORK recovery only when the instruction has a genuine `work_collection_id`. The linked negative WORK effect represents that same recovery's disposition, rather than a second recovery to add to Case recovered totals. The two facts can both appear in history and must be identified as one by this real event/effect link. No summary-reader or Worker/Screen rewrite is named by PS-04.

Manual/no-Timesheet debt deliberately uses its original actual Case/create-request/rule/instruction/result/hold evidence and produces only its Case recovery event. Core `financial_effect` requires real WORK and original Timesheet compound FKs; **do not fabricate them to manufacture a duplicate representation.** A missing work-collection instruction means no WORK effect is expected, not missing financial origin. Current 0410 is PAYE-only and current original collection is PAYE-only; an Umbrella recovery successor remains CM-14's explicit future dependency/refusal, not built ahead here.

2018 CASH_REISSUE posting updates only the original return-cash obligation; it does not post another payroll/Case recovery. Return semantics likewise retain original payroll disposition and recovered history. 1647 inserts WRITTEN_OFF with original-transfer/effect and instruction/result fields NULL under its real write-off request; it adjusts written-off counters, never recovered principal. These are distinct immutable histories, not extra recovery representations.

D05 should remain a finite future joined receipt: one recovery of 30 once with both genuine facts linked; original/recovery identity unchanged on replay, return and reissue; wrong/foreign/multiple link refusal; no-Timesheet genuine Case origin without fabricated WORK/Client/Timesheet; later CM-14 Umbrella shape explicitly deferred. All actual app/installed/business/Worker/queue/minute results remain NOT_RUN. No fixture/hash-proof programme is required by this item.

Read checkpoint SHA-256:

| Source | SHA-256 |
|---|---|
| 1519 collection schema | `924fe1d8feb9b3767bae3261ba711b7837b4c2479cf689c780f3e651e28875d3` |
| 1519 retained collection guards | `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988` |
| 0310 named Case guards | `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe` |
| 0410 Case post owners | `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8` |
| **Final 0316** recovery owner | `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6` |
| Final 2018 post/reconcile owners | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| 2010 Source-bound effect schema | `d2641717de3967aa30e200d75f9e4fde801b31c7eeef9cc8db84efb36d9266ea` |
| 1342 both immutable triggers | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| 1647 write-off owners | `77019761e500193ddd5d8c4147aa0db5bf389c2d1deb02d576407548c0ed4a87` |

Only this investigation document was written. No installation, caller execution or business acceptance is inferred.
