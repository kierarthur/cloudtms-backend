# PS-04 exact existing owner verification (D06)

Source-only verification after owner delivery. Actual in-app/installed/Worker/queue/minute acceptance remains NOT_RUN. No business fixture or scenario was executed.

## Named owners reused

 - `private.bpay_next_case_selection_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_selection_child_insert_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_rule_insert_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_event_insert_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_instruction_insert_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_result_insert_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_hold_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_case_capacity_use_guard_v1()` → `supabase/repeatable/04102026_0310_banking_pay_next_case_guards_v1.sql`; SHA256 `7dd5085d3307a0194b3745487cc19a952ee3b8cb11d03f096d275d23632500fe`.
 - `private.bpay_next_work_collection_guard_v1()` → `supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql`; SHA256 `5dda125e81ee4dac9e78bd87f1618b4c59635f0125536fb856cc060dc381e988`.
 - `private.bpay_next_effect_immutable_v1()` → `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql`; SHA256 `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af`.

All eight0310 guards retain their actual signature, security, fixed search path, logical owner/ACL and original attachments. Real Case-event insertion checks its original instruction/result and DRAFT/NET amount/pass; it happens before hold realisation, so no later-state requirement was added there. Collection1519 protects the actual immutable paid effect/run-line/revision/approved-line/transfer/channel/tax/policy and linked Case/component; current observation does not overwrite them. Both event/effect1342 UPDATE/DELETE guards reject mutation of their complete facts, including operation/item identities. Table privileges remain revoked for ordinary browser/service roles. Collection1519 alone has the cited TRUNCATE attachment; no nonexistent event/effect attachment is claimed.

## Current unique recovery link and producer

Actual final0316 apply_work_collection_recovery stores financial_effect.operation_item_id as the RECOVERED Case event's native id, with equal operation/Candidate/Case/recovery transfer/occurrence and the exact negative SOURCE/target tuple. The Case event's own operation_item_id is the genuine Case hold, not this effect link. Original paid collection transfer and the transfer realising this recovery are distinct retained facts. source_case_event_id is an optional Source-bound adjunct, not the universal link.

New0604 migration adds partial UNIQUE(operation_item_id) WHERE effect_kind='WORK_COLLECTION_RECOVERED'. One event therefore has one WORK mirror even under a different operation; existing operation/item/kind replay constraint remains in place. New0604 private INSERT attachment qualifies exact event/instruction/collection/physical Timesheet, realised hold/capacity/event ID, immutable CASE member/outcome request, original Source tuple and actual leased owner. It does not change or write any money counter.

Current0316 trusted writer still handles exact ON CONFLICT replay by validating original values and returning before position reduction. Current0410 owns one realised Case recovery and invokes the mirror only for a genuine work_collection instruction. Return/reissue cash owners do not invoke a second earnings or recovery; write-off is its distinct forgiveness event. No-Timesheet manual debt remains its actual Case-origin fact; a WORK effect is not manufactured. CM-14 owns the later Umbrella shape and explicit recheck. Future summary/report owners must read this exact event/effect alias as one recovery rather than add both histories.

## Evidence limits

The new index and qualifier are the only product additions. Existing named family, original collection and financial posting sources are unchanged. Local schema compilation/catalog/contract source checks are separate receipts; the joined30 example, replay/foreign/duplicate/return/reissue actual cases and complete app/Worker path remain NOT_RUN.
