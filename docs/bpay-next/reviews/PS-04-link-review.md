# PS-04 recovery link source review

Verdict: **PASS — source only**, 10 October 2026. No open source findings. Reviewed the complete PS-04 card and six checklist objects, both new0604 SQL files, final registry, unchanged named owners, corrected BD034 scope, D05 joined acceptance and D06 owner verification. No SQL, tests, fixtures, business actions or runtime checks were executed by this reviewer.

## Unique link and qualification

The native universal mirror link is `financial_effect.operation_item_id = case_event.id` for `WORK_COLLECTION_RECOVERED`. The Case event's own `operation_item_id` is the realised Case hold. Its `original_effect_id` identifies original paid provenance; optional `source_case_event_id` belongs only to a Source-bound effect. The implementation uses the correct event identity rather than imposing uniqueness on either unrelated field.

The partial UNIQUE index on recovery `operation_item_id` prevents a second WORK mirror for the same event across operations. The existing operation/item/kind replay key remains intact. The new private BEFORE INSERT qualifier checks the positive RECOVERED event, genuine collection/instruction and physical Work/Timesheet, exact operation/Candidate/Case/recovery transfer/occurrence, and the negative source and target tuple. NULL-safe tuple comparisons reject missing or substituted values. It also checks actual REALISED hold/capacity with the event ID, immutable CASE member and outcome-request identity, and the existing leased NEXT job/module/control owner. Original Source lineage is taken from the collection's retained original run line; unbound ordinary rows retain the all-NULL Source tuple.

The event is created before hold realisation. Adding the qualification to the effect INSERT correctly checks the later realisation without changing the original0310 event guard or its ordering. The new guard is SECURITY INVOKER with a fixed search path, audited logical owner, private attachment and revoked ordinary-role function privileges. Other effect kinds pass through unchanged. There is no new public RPC, financial write, counter, invented certificate or inferred Timesheet/Client origin.

## Existing owners and once-only recovery

All eight final0310 Case guards and their signatures, attachments, security and ACL remain reused. Collection1519 retains its immutable original paid effect, run line, revision, approved line, transfer, channel, tax and policy tuple; current observation does not replace the origin. Current final0316 `bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` writes the event alias and exact negative tuple. Its existing replay validates the retained row and returns before reducing the position a second time. Final0410 creates the actual Case recovery, realises the hold/capacity and calls the mirror only for a genuine collection-backed instruction. The final2018 posting entry reaches these owners without an additional recovery writer.

Return/reissue cash processing does not create a second earnings recovery; write-off remains a distinct forgiveness event. Genuine manual debt with no Timesheet remains its real Case-origin fact, with no manufactured WORK effect. The current PAYE shape is retained; CM-14 explicitly owns the later Umbrella shape and recheck. Future summary readers must treat the native event/effect alias as one recovery, rather than sum both records.

The corrected scope and D05 accurately distinguish1342 event/effect UPDATE/DELETE immutability from the collection1519 TRUNCATE attachment. They no longer claim an event/effect TRUNCATE attachment. Existing ordinary-role table privileges remain revoked. Registry addition is limited to the new private qualifier; the existing final owners remain the recorded authorities.

## Checklist and evidence boundary

D01–D04 reuse the named collection/guard/context owners with the finite index and INSERT support required for the link. D06 now records all eight exact0310 final signatures and the1519/1342/0316 owner map, native identity distinctions and replay behavior. D05 defines the joined positive recovery, exact replay, wrong/foreign tuple, duplicate, return/reissue, moved-origin and no-Timesheet checks without claiming they ran. It keeps the later Umbrella check explicit. No Worker, screen, summary counter or report implementation is claimed by this batch.

Local installation, catalog metadata and root-run checks are separate receipts. Actual joined acceptance and app/Worker/queue/minute paths remain **NOT_RUN**; this source verdict does not certify their execution.

## Fresh source receipts

| Source | SHA256 |
| --- | --- |
| `supabase/migrations/10102026_0604_banking_pay_next_ps_c04_unique_recovery_link_v1.sql` | `cf4e57122b7ba503aa724f21dd42218d9d807461d32f033b8a20b2bb229e48d7` |
| `supabase/repeatable/10102026_0604_banking_pay_next_ps_c04_recovery_link_guard_v1.sql` | `2c0f093c8d900899b61a40b4316757958c20246d4c197001c96674bbd7785cb8` |
| `docs/bpay-next/final-definer-closures.json` | `3a287412ccf38235bd500987c25870531d071569da2f4eaa4224b78111e16c9a` |
| `docs/bpay-next/design/PS-04-SCOPE.md` | `0d7c6976e5ae9c3600feaf4ae6386e1c304f1ee06a27e64b3ee6802ac2ae0eb4` |
| `docs/bpay-next/PS-04-JOINED-ACCEPTANCE.md` | `3dcca9195f73591c6bf3d6e405e226d0f7367983b5914cd2aec775bb4921bd2c` |
| `docs/bpay-next/PS-04-OWNER-VERIFICATION.md` | `38b04ddb0a856e37ae1fb54e9112c3efdd46485de331a06b44847189a2253d35` |

Retained owner sources reviewed:0310 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`;1519 `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988`;1342 `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`;0316 `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6`;0410 `f56898dc7e992fc271d2a90c0c7fdb85d77dde4bac5e8b852e008c0da90793d8`;2018 `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b`.
