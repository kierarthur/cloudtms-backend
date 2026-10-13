# F-25 System-hours completion build map

10 October 2026. Read-only finite design; root alone edits product. Read current saved 0020 completion schema, full original direct acceptance body (1534 Query delivery:642–745), full bulk body (1534 read projections:4949–5386), current producer lifetime 2357, current 0017 witness/incident staging and the prior delivery-completion design. No SQL/runtime/tests/fixtures/hosted execution. Scope is DIRECT_SYSTEM_HOURS and bulk ACCEPT_SYSTEM_HOURS + EXPLICIT only; no whole-F25 or other bulk action claim.

## Exact entry and final tails

Direct `public.weekly_source_query_accept_system_hours_atomic_v1(jsonb)` already has pure service auth, shape/parsing, sorted original `v_requested`, plain publication guard and **scoped** Office permission at 660–683. `weekly_source_query_current_publication_v1` and `weekly_source_office_authority_v1` are plain SELECT/STABLE helpers, with no business/advisory locks. Keep their exact bodies and scoped arguments. Replace the admission line 684 with producer entry immediately before incident FOR UPDATE 685–687. Preserve every original per-incident recheck, update, event INSERT, exact count assertion and audit. At 743 assign the unchanged business reply to a local result; validate original event pairs and, if true root, persist DIRECT completion and finish; nested child validates the same real pairs and leaves only, then returns the original two-field reply.

Bulk `public.weekly_source_office_bulk_query_action_atomic_v1(jsonb)` preserves all original shape/parsing/selection normalization through 5113. At the plain guard 5116, for ACCEPT_SYSTEM_HOURS only, invoke the existing scoped permission **before** producer entry:

```sql
perform private.weekly_source_office_authority_v1(
  v_actor,'ACCEPT_SYSTEM_HOURS',(v_guard->>'source_group_id')::uuid,null,
  (v_guard->>'finalisation_week_ending')::date
);
-- Enter SYSTEM_HOURS_ACCEPT here, before cycle FOR UPDATE 5120–5121.
```

Retain original locked publication/workspace/permission rechecks 5122–5131 and complete group/incident proof checks 5160–5208. The genuine branch makes **one** child call for all selected UUIDs at 5209–5212. Child entry must inherit the existing producer via one explicit SYSTEM_HOURS_ACCEPT → SYSTEM_HOURS_ACCEPT edge added to the full current 2357 successor, retaining every existing edge. Child leaves after its original audit. Parent then validates unchanged child `ok/resolved_count` against selected count, inserts one BULK completion at root stack depth one, finishes the parent and returns the **unchanged** original bulk reply 5213–5216. Do not add producer entry/finish to ASK_CANDIDATES or SEND_MANAGER_NOW in this batch. Keep the entire full bulk body, including their exception and final aggregate paths, intact.

Stable non-NEXT entry yields null producer: preserve original business behavior and skip protocol completion. A true outer token must never be acquired for the first time by the already-locked bulk child. No completion INSERT or clock in an incident loop; no independent child metadata command.

## Root and child hashes

Use `private.weekly_source_sha256_jsonb_v1` with explicit version domains. Suggested direct action domain `BPAY_NEXT_SYSTEM_HOURS_ACTION_V1` over the typed object below:

```sql
jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
 'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_requested))
```

Use the same action derivation for the bulk child's typed actor/cycle/publication/sorted `v_incident_ids`. Keep root SYSTEM_HOURS_ACCEPT/cause kind SYSTEM_HOURS_ACCEPT. Cause keys are bounded `SYSTEM_HOURS_DIRECT:<hash hex>` and `SYSTEM_HOURS_BULK:<hash hex>`; no invented business UUID. Direct `request_sha256=child_action_sha256=action SHA`. Bulk `request_sha256` is a separate `BPAY_NEXT_SYSTEM_HOURS_BULK_V1` digest binding **all** accepted root intent:

```text
actor, cycle, publication, normalized action, expected_workspace_version,
selection: normalized mode; sorted group_keys/excluded_group_keys/incident UUIDs;
original validated filters; defaulted sort_key; lower-case defaulted sort_direction;
normalized selection_proof; group_selection_proofs sorted by group_key
```

Use actual original variables/defaults from 5047–5076: do not replace filters with a guessed query interpretation or drop sort/proof fields. Keep exact proof text and unknown-field validation. Root request SHA must equal the producer's immutable entry SHA; bulk child action SHA must equal the same direct-action derivation from completion actor/cycle/publication/selected UUIDs. **Never require bulk root SHA to equal child SHA.** The completion record deliberately does not store raw request/full view; its root SHA is bound to the real entry, while preserved actual business validators prove selected intent.

Do not DISTINCT direct `v_requested`: the original sorted array retains duplicates, and original final count check 734–736 refuses duplicates/missing IDs. De-duplicating would change accepted behavior. Successful metadata selection is sorted, one-dimensional, no-null and duplicate-free; require it only after the unchanged original count assertion. Bulk already rejects duplicate UUID/group keys at 5096–5098. Bound actual successful selection/stages to the schema's 1000 maximum; a protocol-cap refusal must roll back the whole action.

## Exact original immutable pair witnesses

For each actually locked original incident and its original immutable comparison, retain both existing INSERTs and ON CONFLICT DO NOTHING exactly. Retrieve the resulting **real existing row IDs** using their unique `(event_kind,idempotency_key)` identity; missing RETURNING on conflict is not missing evidence. Read/verify every field against the original local incident/comparison/actor/publication values:

| 0020 witness kind | Real event kind | Exact original key |
| --- | --- | --- |
| OFFICE_ACCEPTED | OFFICE_ACCEPTED | `OFFICE_ACCEPTED:<incident UUID>:<comparison material fingerprint hex>` |
| OFFICE_RESOLVED | RESOLVED | `OFFICE_ACCEPTED_RESOLVED:<incident UUID>:<comparison material fingerprint hex>` |

Both must have the same original incident, issue episode, requested projection publication, expected comparison fingerprint, actor kind OFFICE and original actor_user_id. **Actual payload key is `bounded_payload_json->>'resolution'='OFFICE_ACCEPTED_SYSTEM_HOURS'`.** `resolution_kind` is the incident column, not this event payload key. Preserve original immutable event JSON rather than changing it to carry a new command/hash.

Exactly two distinct real event IDs per selected UUID, exactly 2N witness members overall; deterministic ordinal order is incident UUID ascending, then OFFICE_ACCEPTED/ OFFICE_RESOLVED. Set each witness row `comparison_revision_id=NULL`, `discrepancy_event_id=real ID`, `evidence_sha256=typed original-event digest`. No extra COMPARISON witness row: 0020 requires original_witness_count exactly 2N. The source proof nevertheless binds the stage's **original old** `current_comparison_revision_id` to the immutable comparison for that incident and its real material fingerprint. Both pair events bind that fingerprint. Do not read today's incident pointer/state to qualify old proof.

Preserve publication carry-forward: original publication guard can accept a later publication cycle within the exact authorized Source group/client/week scope. Completion `source_cycle_id` is original request `v_cycle_id`; event publication is requested `v_publication_id`. Do not demand publication.source_cycle_id=request cycle or add a comparison publication equality absent from the original acceptance owner. Original comparison table has no episode column; qualify original episode in the child against its original incident and later require pair equality, not a guessed comparison.issue_episode field.

For nested bulk, child validates/collects real pairs before leave but inserts **no root completion**. Parent can rederive the same pairs from its exact selected UUID set and each owned stage's old immutable comparison ID after child leaves; the child need not add protocol fields to its public business reply. A private pure collector/assert helper can be called by both tails; it must require the producer's OPEN transaction and correct SYSTEM top kind, not create or seal a producer.

## Exact 0020 field map and completion assertion

| `private.bpay_next_query_root_completion` field | Actual value/required proof |
| --- | --- |
| id/preparing_transaction_id/module_epoch | Existing producer ID/xid/epoch; witness UUID passed to finish is this producer ID. |
| owner_kind/root_variant | SYSTEM_HOURS_ACCEPT; DIRECT_SYSTEM_HOURS or BULK_SYSTEM_HOURS. Root stack must be exactly one at record insertion. |
| request_sha256/child_action_sha256 | Original entry/root SHA and typed direct action SHA as above. |
| source_cycle_id/projection_publication_id/actor_user_id | Parsed real original action identity, never today's replacement pointer. |
| input_identity_count/input_identities/input_identities_sha256 | N, complete exact sorted selected incident UUID array, typed domain-bound digest of that exact array. |
| omitted_incident_count/ids/sha | 0, empty `uuid[]`, canonical typed empty-array digest. |
| new_incident_count/changed_comparison_count/unchanged_incident_count | All 0 for system acceptance. |
| resolved_incident_count | Actual original `v_count=N` / checked child's resolved_count=N. |
| actual_stage_count/first_stage_ordinal/last_stage_ordinal/raw_stages_sha256 | Whole producer-owned raw staged set, from pure 0030 raw-stages helper; exact stage/selected set proof below. |
| original_witness_count/original_witnesses_sha256 | 2N and digest of complete deterministic witness ordinal/kind/real event ID/evidence SHA set. |
| completion_sha256 | Pure typed 0030 completion-header digest; never hashes itself. |

Before inserting record/members, require same xid/module, producer BUSINESS_OPEN/STANDALONE_QUERY, input transaction OPEN, root SYSTEM kind/one stack frame and stable NEXT owner epoch. Require all owned staged events belong to this exact module/producer, `owner_relation=public.weekly_discrepancy_incidents`, UPDATE old/new-present, OPEN → RESOLVED and all other canonical gate fields unchanged. Sorted distinct stage incident IDs must equal the complete selected UUID set and actual_stage_count=N. For each stage, bind its old immutable comparison pointer and original event pair as above. No extra unrelated Candidate/incident/comparison gates may hide in the aggregate. Actual raw stage hashes must cover full stage/owner/gate tuple identities without using completion-dependent `bpay_next_query_event_digest_v1`.

Insert header and all real witness FK rows before calling existing metadata finalizer: their cycle/publication/actor/event/comparison FK locks are preclock. Guard must freeze the complete header+members against later inserts/changes as well as existing UPDATE/DELETE/TRUNCATE refusal; witness assertion must enforce exact count/ordinals/member digest and disallow append after producer tail/finish. A valid FK is identity linkage, not evidence digest equality: explicitly recompute/compare typed event/member/raw/header digests. Extend the latest **full** witness assertion, preserving current manual, response and actual APP_SUBMIT branches, with SYSTEM_HOURS_ACCEPT → this exact immutable header+member proof. Existing `finish_standalone_v1` then receives `(producer,'SYSTEM_HOURS_ACCEPT',producer,completion SHA)` and creates one zero-job metadata command for the nonempty material set.

### Zero stages and original repeat behavior

The generic 0002 zero-stage finish remains unchanged. This specific successful NEXT owner cannot legitimately have zero stages: selected nonempty OPEN incidents are all changed to RESOLVED and current 0017 canonical row hook necessarily stages one each. Zero stages here indicates an incomplete/missing hook or invalid certificate and must refuse; do not create a successful zero-stage replay. Neither direct nor bulk acceptance has an original command UUID/immutable whole-reply replay. A repeat over already-resolved incidents keeps the original stale/workspace refusal. ON CONFLICT only concerns the two real event INSERTs during an otherwise valid original action; it does not authorize skipping the original OPEN/count/current-publication checks.

## Minimal source successor order

0020 schema → 0030 pure typed evidence helpers → finite completion qualification/insertion guard + full witness assertion successor → full current 2357 producer-enter successor with one SYSTEM→SYSTEM edge → full direct acceptance and full bulk owners/unchanged ACL → exact final-definer registry and schema-cache notify. Current 0017 APP/response witness and incident hook must remain selected; do not reintroduce an older witness assertion. No new producer enum, public request/reply field, calculator, fixture or second clock is needed.

Source hashes at read:

| File | SHA-256 |
| --- | --- |
| `10102026_0020_banking_pay_next_query_root_completion_v1.sql` | `bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf` |
| `15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `15092026_1534_weekly_source_read_projections_v1.sql` | `dace15a8df0b37decf261beb3cd99ac5c59ea362f22114b382c1c0148d4bf0e4` |
| `09102026_2357_banking_pay_next_query_producer_lifetime_v1.sql` | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| `10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |

This is finite design evidence only; no installation/runtime pass is claimed.
