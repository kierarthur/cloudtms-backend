# F-18 retained advice target proof

Read-only source investigation, 9 October 2026. No SQL, database/runtime calls, tests or product edits. F-18 is a factual bounded census; it must not invent disabled advice as a debt, revive the legacy engine, or let one sent target satisfy another. Owner start-sheet overrides switch/approval controls; this note supplies only the narrow `LEGACY_REMITTANCE_TARGETS` correction.

## Effective saved owners

The current saved definers are in `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql`: `pay_operation_remittance_scope_seed` at 70305, `pay_remittance_mark_sent` at 85700, `pay_remittance_build` at 87339, `pay_finance_payout_notice_build` at 96849, `pay_remittance_queue_commit_stage` at 98324 and `pay_finance_payout_notice_queue_commit_stage` at 100403. No later saved CREATE OR REPLACE definer for these identities was found in current repeatables. The 0550 relation writer fence and Worker LEGACY checks apply separately; do not invoke these legacy writers from the census.

`pay_remittance_build` has two distinct paths. Operation mode consumes real `banking_pay_operation_remittance_scope` rows, returns their `remittance_type` as job_kind and carries exact scope/outbox/key identities. Small compatibility mode builds `UMBRELLA_REMITTANCE`, `PAYE_REMITTANCE` and conditional `CANDIDATE_UMBRELLA_COPY_REMITTANCE` jobs. Do not collapse these naming schemes.

## Exact preference and destination predicates

For Candidate alias `c` and default alias `sd`, these are the saved small-mode owner predicates (settings_defaults id=1 at 88032–88049):

```sql
-- Candidate copy of an Umbrella payment, 90166 / 90215.
case when coalesce(c.remittance_overrides_enabled,false)
  then coalesce(c.remittance_receive_when_umbrella_paid,false)
  else coalesce(sd.remittance_receive_when_umbrella_paid,false)
end

-- Normal PAYE remittance, 89224 / 89271.
case when coalesce(c.remittance_overrides_enabled,false)
  then coalesce(c.remittance_receive_enabled,false)
  else coalesce(sd.paye_remittances_enabled,false)
end

-- Small-mode queue owner also requires a real nonblank recipient email.
nullif(btrim(coalesce(c.email,'')),'') is not null

-- Umbrella owner roster HAVING at 88156.
nullif(btrim(coalesce(u.remittance_email,'')),'') is not null
```

Use LEFT JOIN `public.settings_defaults sd ON sd.id=1`, so a missing default row resolves false rather than removing the selected Candidate. `remittances_detailed_breakdown` affects formatting, not existence; Umbrella `remittance_overrides_enabled` chooses detail formatting, not whether the Umbrella advice exists. Candidate copy preference is independent of Candidate normal PAYE preference.

The operation scope seed independently uses the same Candidate PAYE override expression, with defaults selected `ORDER BY settings_row.id LIMIT 1` at 70679–70687. Record this exact owner difference; do not claim the historical two queries are byte-identical. For a genuine retained scope, its already-captured recipient/type/item/transfer identity is the primary obligation witness; a later disabled preference must not erase admitted unsent mail/scope work. Conversely a hypothetical never-admitted copy currently disabled is not an unresolved advice obligation.

The current census at `09102026_2109...sql:303–305` unconditionally adds Candidate copy for every Umbrella target. It must add the Candidate copy preference and destination predicate. It also omits the ordinary PAYE target, which needs its own preference/destination and actual PAYE eligible-item evidence.

## Scope schema, types and exact keys

`11052026_1419_NEW_TABLES_for_splitting_payments.sql:559` creates `public.banking_pay_operation_remittance_scope`: `id`, `operation_id`, `pay_batch_id`, `pay_batch_candidate_id`, `candidate_id`, `recipient_kind`, `recipient_id`, `remittance_type`, `deterministic_outbox_key`, `payload_json`, `status`, `outbox_id`. Allowed status is **PENDING, QUEUED, SKIPPED, FAILED**; there is no SENT scope status. Unique key is `(operation_id,deterministic_outbox_key)`. SENT belongs to `public.mail_outbox`.

Current operation seed creates only these actual scope types/keys (70659–70710):

| Type / recipient | Exact deterministic key |
| --- | --- |
| `CANDIDATE_REMITTANCE` / `CANDIDATE`, recipient_id=Candidate | `remittance:batch:<batch>:candidate:<candidate>:scope:<payment_scope_key>:type:CANDIDATE_REMITTANCE` |
| `UMBRELLA_REMITTANCE` / `UMBRELLA`, recipient_id=Umbrella | `remittance:batch:<batch>:candidate:<candidate>:umbrella:<umbrella>:scope:<payment_scope_key>:type:UMBRELLA_REMITTANCE` |

`payment_scope_key` is exactly `transfer:<pay_bank_transfer_id>` when the item has a transfer, otherwise `batch_candidate:<pay_batch_candidate_id>:channel:<normalised pay_channel>` (70610–70613). `payload_json` retains `pay_batch_item_ids`, `candidate_id`, `pay_batch_candidate_id`, recipient/type and nested `payment_scope_json.scope_key/pay_bank_transfer_id/transfer_group_key`.

The current seed does **not** create a Candidate-copy scope or a payout-type scope. Do not invent such a `remittance_type`, expect it from this producer, or parse a guessed `remittance:batch:<batch>:CANDIDATE_UMBRELLA_COPY_REMITTANCE:` prefix. An actually retained unexpected scope must be described from its saved type/key/payload and genuine origin, not silently relabelled.

Operation remittance queue inserts `mail_outbox.type='REMITTANCE'`, lowercased actual recipient kind, recipient ID, `context_kind='pay_batches'`, batch context ID, **reference=scope deterministic key** and **deterministic_outbox_key=the same key** (98723–98909). Its payment_scope_json includes actual `pay_batch_id`, `operation_id`, `remittance_scope_id`, `recipient_kind`, `recipient_id`, `remittance_type`, item scope and render evidence. It links `scope.outbox_id` and preserves SENT mail identity/content on upsert. These are the exact retained identity witnesses.

Small compatibility queue returns job queue_context references **`pay_batch:<batch>:candidate:<candidate>`** (100155) and **`pay_batch:<batch>:umbrella:<umbrella>`** (997...); job kind is retained separately in `queue_context.job_kind`. This reference alone does not distinguish PAYE from Candidate Umbrella copy. The actual saved Worker `sendPayBatchRemittancesInternal` now calls SQL operation/compatibility owners; it does not manufacture the census's guessed copy prefix. For genuine earlier mail, require its captured job/type/scope evidence; a bare recipient+batch match cannot satisfy both kinds.

## Exact SENT completion predicate

For a genuine scope `s` and mail `m`, prefer the exact retained identity, then validate it rather than relying on prefix:

```sql
upper(btrim(coalesce(m.status::text,''))) = 'SENT'
and m.type = 'REMITTANCE'
and upper(btrim(coalesce(m.recipient_kind,''))) = upper(btrim(s.recipient_kind))
and m.recipient_id is not distinct from s.recipient_id
and m.context_kind = 'pay_batches' and m.context_id = s.pay_batch_id
and (m.id = s.outbox_id
     or m.deterministic_outbox_key = s.deterministic_outbox_key)
and m.payment_scope_json->>'pay_batch_id' = s.pay_batch_id::text
and m.payment_scope_json->>'remittance_type' = s.remittance_type
and coalesce(m.payment_scope_json->>'message_kind','REMITTANCE') <> 'PAYOUT_NOTICE'
and coalesce(m.payment_scope_json->>'notice_scope','') <> 'PAYOUT_NOTICE_CANDIDATE'
```

When identifying by outbox UUID, reject contradictions in retained key/recipient/batch/type/scope rather than trust a coincidental UUID relationship. Normal operation remittance reference/key must equal the original scope deterministic key even on the UUID branch; payout owner can overwrite the scope's outbox link with a different generated payout key, and that does not complete normal remittance. Actual scope queues include `payment_scope_json.remittance_scope_id`, so require that exact identity too when present. A retained positive provider conflict/UNKNOWN belongs to external/integrity review, not advice completion. The current census's permissive `coalesce(provider_status,'ACCEPTED')` is not independent proof of provider acceptance; durable SENT is the owner completion field, and contradictory external evidence must be classified explicitly.

For inferred small-mode targets without a retained scope/key, require type REMITTANCE, exact Candidate/Umbrella recipient, exact retained batch/context, exact captured job kind (`PAYE_REMITTANCE` versus `CANDIDATE_UMBRELLA_COPY_REMITTANCE` versus `UMBRELLA_REMITTANCE`), and coverage of the actual item/transfer scope. `pay_batch_candidate.remittance_sent_at_utc` is aggregate and cannot prove every distinct recipient/kind/payment-group completed. One unrelated SENT mail in the same batch must not hide another missing target; use NOT EXISTS **per target/scope** and then return its bounded Candidate identity.

## Payout notices

Do not apply PAYE/copy preference to genuine Candidate payout notice: it is a separate owner. Small `pay_finance_payout_notice_build` initially reads nonvoid `LOAN_PAYOUT`, `MANUAL_CREDIT_PAYOUT`, `UNDERPAYMENT_PAYMENT` and nonblank Candidate email; the final queue owner prunes scope before admission. Operation `...payout_notice_queue_commit_stage` at 100714–100797 actually requires:

- nonvoid item and no APPLIED PRE_BANK_CANCEL/NO_MONEY_UNWIND/SETTLED_REVERSAL correction;
- a genuine finance_case_id, or a genuine carry-forward witness for MANUAL_CREDIT_PAYOUT/MANUAL_DEBT_RECOVERY (`source_ref`, `operation_source_key`, frozen_source_basis_json or frozen_component_snapshot_json `manual_adjustment_carry_forward_id`);
- item_type in **LOAN_PAYOUT, MANUAL_CREDIT_PAYOUT, MANUAL_DEBT_RECOVERY**;
- exact frozen one-off branch: `payout_instruction_snapshot_json.routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'`, truthy `is_candidate_directed_oneoff_payout` and truthy `generates_candidate_payment_advice`; **or** the explicit genuine carry-forward branch;
- when only_confirmed, actual `_pay_rail_state_money_movement_classify(...).is_final_money_moved` for the referenced transfer, or real Candidate SETTLED/settled_at when no transfer;
- nonblank Candidate email and eligible_item_count>0 at the final valid-target step.

Thus census deriving payout advice from `routing_kind='ONE_OFF_SPECIFIED_BANK_ACCOUNT'` alone or `v_finance_cases_register.generates_candidate_payment_advice` fallback is broader than the actual frozen queue contract. Retain the complete routing/witness tuple. No current method/channel or finance-register reread may manufacture a target for a frozen item that did not qualify.

Operation payout outbox key is **`payout_notice:pay_batch:<batch>:scope:<scope.id>:candidate:<candidate>`** at 100813. It inserts `mail_outbox.type='REMITTANCE'`, Candidate recipient, batch context, reference/key equal to that generated key and payment_scope_json `notice_scope='PAYOUT_NOTICE_CANDIDATE'`, `message_kind='PAYOUT_NOTICE'`, `operation_id`, `remittance_scope_id`, candidate and exact items. Importantly this generated payout key can differ from `scope.deterministic_outbox_key`; use **scope.outbox_id plus exact payout scope identity/generated key**, not equality with the original remittance scope key. Small-mode payout reference is **`payout_notice:pay_batch:<batch>:PAYOUT_NOTICE:CANDIDATE:<candidate>`** at 101522/101822; copied scope_kind is PAYOUT_NOTICE_CANDIDATE. A batch prefix alone is insufficient, and payout notice is not a standalone mail_outbox.type='PAYOUT_NOTICE'.

## Eligibility before interpreting a missing target

Preserve original owner gates for suppression, configured ON_EXECUTION versus ON_PAYMENT_CONFIRMED timing, scheduled execution and item/correction scope. Operation seed explicitly returns SUPPRESSED/DEFERRED without creating a target (704...); queue can legitimately SKIP suppressed/no-email/no-eligible-items. Missing recipient/preferences in a hypothetical recomputation are not proof an advice was admitted. Genuine retained queued scope/outbox is evaluated from its original retained evidence, without current preference erasure.

Use the actual final-money classifier and settled scope/transfer witnesses from the producer. The current census's blanket `pay_bank_transfers.status IN ('COMPLETED','RETURNED','REVERTED')` is not the owner's normal confirmed-advice predicate; returned/reverted disposition and cancellation notices have their own retained facts/categories. Do not classify a no-money terminal transfer as a new positive advice obligation solely from that status.

The narrow safe census change is: derive exact eligible target kinds with their preference/destination/suppression/frozen-route predicates; union genuine retained scope/mail targets independently so later preferences cannot delete them; test exact SENT coverage per real key/scope; classify damaged identity or unknown external evidence explicitly. Remain STABLE/read-only and bounded. No old work fixture or owner carry-over choice is required when these genuine categories are empty.

## Saved source hashes

| File | SHA-256 |
| --- | --- |
| `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql` | `8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f` |
| `supabase/repeatable/09102026_2109_banking_pay_next_cutover_readiness_page_v1.sql` | `257ce7856adc6425ab35e275d115f8ff0b2c7c62f47b45e4c5c9847fdc54c67e` |
| `supabase/migrations/11052026_1419_NEW_TABLES_for_splitting_payments.sql` | `1017aa8e704765915ed6d1c6b3d748a86d2127d82bb0a9ae71a8fcdb48b7dcaa` |
| `broker/src/index.js` | `91f6327ed3ba135c1055fd06da94c5f5dec64df9bad8612277e5581ed603af34` |

Re-read current saved source before root's edit/review; hashes are this investigation's observations, not runtime proof.
