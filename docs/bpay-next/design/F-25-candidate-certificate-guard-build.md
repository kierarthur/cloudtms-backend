# F-25 Candidate certificate INSERT guards and owned save lifetime

10 October 2026. Read-only source design for the five applied0111 tables. No SQL, runtime, tests, fixtures or product changes were executed. Root implements. This supplements `F-25-candidate-protocol-schema-build.md`; it does not certify the pending Candidate owner successors or the whole F-25 protocol.

## Current authorities and ordering

Use the final producer entry from0032, producer leave/query stage from2357, standalone finish from0002, and completion-witness assertion from0058. The original Candidate owners remain15092026_2203 for Materialise/private compare/SELF and0017 for the current APP parent. The new0121 typed digest helpers exist on disk;0125 was still a generated placeholder at this read. Do not use that placeholder as an installed owner authority.

`bpay_next_query_producer_enter_v1(text,text,text,bytea,text)` admits the true root before its first business/session/advisory/FK lock. Nested calls preserve the same producer/root cause/hash and push a finite owner stack. `bpay_next_query_producer_leave_v1` only pops the exact top child; it does not finish or clock. `bpay_next_query_version_stage_v1` accepts ordinary stages only while the producer is BUSINESS_OPEN and the input transaction OPEN. A standalone finish first asserts its original witness, then stores the witness, then either seals an empty input transaction without a command or performs the one metadata command. Candidate INSERT guards must refuse both cases after that boundary. The Source post-clock TAIL_PREPARED exception is not a Candidate certificate permission.

Insert order at each real owner tail:

1. Private compare: finish the original complete comparison loop, omission loop and final open count; insert its header, then exact input/omission rows; assert the complete child set; leave QUERY_SYNC.
2. Materialise: finish the original submission-complete child and final workflow cached-response UPDATE; insert/assert its actual Materialise certificate; leave MAT when inherited, otherwise continue to its root adjunct.
3. SELF: after inherited MAT returns, insert/assert the SELF root adjunct, then true-root finish with zero Query stages.
4. APP: after inherited MAT and any actual CandidateResponse child have left, finish original draft retirement/projection/FINAL_SUBMIT receipt insertion; insert/assert its root adjunct attached to that actual immutable receipt, then finish once.

All original business FK and certificate FK locks must occur before the agency clock. Do not append any certificate or comparison child after root finish. Existing append-only UPDATE/DELETE/TRUNCATE guards remain unchanged.

## Common BEFORE INSERT predicate for all five tables

Resolve producer identity from NEW.producer_id; comparison child rows additionally resolve their exact `(compare_completion_id,producer_id)` header. Require the following with explicit null-safe predicates:

```sql
owned.id = NEW.producer_id
AND owned.preparing_transaction_id = pg_catalog.pg_current_xact_id()
AND owned.phase = 'BUSINESS_OPEN'
AND owned.mode = 'STANDALONE_QUERY'
AND owned.completion_witness_kind IS NULL
AND owned.completion_witness_id IS NULL
AND owned.completion_witness_sha256 IS NULL
AND owned.metadata_command_id IS NULL
```

Require the exact current input transaction, matching module epoch, `phase='OPEN'`; module row1 must have `active_owner='NEXT'` and `owner_epoch=owned.module_epoch`. For header/MAT/root rows, NEW.xid/epoch must equal the producer triple. For child rows, the existing header triple supplies that binding. Require `completed_at_utc IS NOT DISTINCT FROM transaction_timestamp()` for rows possessing that field, their own typed row SHA, and no already-inserted Candidate root adjunct. The original entry owns the module SHARE lock already; guard reads must not acquire a new admission/module/clock or unrelated business lock.

Use an explicit five-table whitelist if sharing a trigger function. Unknown TG_TABLE_NAME must refuse, never fall through into an omission branch. All private helper/trigger functions retain security/search_path/owner/ACL contracts; no service/browser direct INSERT exposure. A FK to a producer from an older transaction is insufficient evidence of an active owner. No guard may infer authorization from enum membership, a caller-supplied boolean, or merely a top owner in a broad list.

The real schema fields are `owner_stack`, `owner_kind`, `cause_kind`, `cause_key`, `request_sha256`, `preparing_transaction_id`, `module_epoch`, `phase`, `mode` and the completion-witness fields. Compare exact text arrays, including cardinality, rather than just `owner_stack[cardinality(owner_stack)]`.

## 1. `private.bpay_next_candidate_compare_completion`

For currently implemented MATERIALISE callers the only legal complete stacks are:

| True root | Exact owner_stack at INSERT | Header requirement |
|---|---|---|
| CANDIDATE_MATERIALISE | `{CANDIDATE_MATERIALISE,QUERY_SYNC}` | caller_kind MATERIALISE, call_ordinal1 |
| CANDIDATE_APP_SUBMIT | `{CANDIDATE_APP_SUBMIT,CANDIDATE_MATERIALISE,QUERY_SYNC}` | caller_kind MATERIALISE, call_ordinal1 |

Require root owner_kind/cause_kind to match that first element. Generation and actual membership must be nonnull. Require no already-inserted Materialise/root certificate. SELF has no compare branch in2203: the generic producer SELF→MAT→QUERY edge does not establish a real SELF comparison owner.

At the actual compare tail require `end_event_ordinal=input_transaction.next_event_ordinal`, the captured genuine entry begin ordinal, and `actual_stage_count=end-begin=3*new_count+2*changed_count+resolved_count`. Recompute0121 raw-range SHA and typed header SHA. Canonical arrays must be native UUID ordered, duplicate-free, no nulls, exact declared cardinalities; generated/omitted counts are at most1000, witnesses at most4000. Original result SHA binds exactly the original result JSON and its counters/outcome/open count; do not add coverage/currentness conditions to the original final open-count selector.

The call request SHA must bind actual generation/publication/membership/Timesheet/revision/hash/call time and caller/ordinal using the typed private compare request digest, rather than the root APP request hash. The actual source group/cycle/profile/Candidate/client/Contract/week scope is checked preclock against the original inputs already owned by the business function. Do not require comparison publication=current header publication for historical evidence: an unchanged comparison may legitimately originate earlier.

PREFINAL is an explicit uninstalled-owner refusal now. A future SourceUpload successor can enable only its exact registered root stack/mode and actual per-loop ordinal, with generation/membership null. Neither generic SOURCE_UPLOAD→QUERY nor SOURCE_ROWS_APPLY→QUERY entry permissions prove that root has been completed. Refuse PREFINAL before the original private comparison's first lock until that true Source root is installed; do not fabricate a standalone Candidate certificate for it.

The header BEFORE INSERT cannot assert complete persisted child sets: those rows do not exist yet. It validates the planned finite manifest, original request/scope and all locally supplied/transient evidence. The save helper inserts children and then performs the full retained assertion before leaving QUERY_SYNC. No reverse/deferred FK to a future MAT/root certificate is necessary.

## 2. `private.bpay_next_candidate_compare_input_completion`

Require the common predicate, exact header and exact same stack as the header's current MATERIALISE branch. The header end ordinal must still equal the current stage tail; no MAT/root certificate may yet exist. Require work_event_id in the header.generated_work_ids and current child count less than header.generated_issue_count. PK/unique scope plus exact set cardinality prevents extra or duplicate rows; the complete saved assertion checks all rows, not just each individual INSERT.

At this preclock owned tail, require actual incident/work/episode/current_comparison_revision_id and comparison.incident_id to bind the supplied IDs. The work's actual Candidate/client/first_source_group and date bind header scope. For NEW, CHANGED and UNCHANGED_OPEN require actual OPEN state and incident.source_cycle_id=header.source_cycle_id. The supplied original recheck event must bind that actual incident/episode/comparison/publication/fingerprint with original SYSTEM actor and `origin='CANDIDATE_TIMESHEET_SUBMISSION'`; CHANGED also binds the actual prior comparison identity. Keep the0039 immutable comparison/recheck proofs and0121 Candidate origin digest.

For UNCHANGED_RESOLVED preserve the real resolved-decision helper: latest incident by source_group/work, allowed original resolution kind, exact actual comparison narrow terms and real immutable decision event. **Do not add source_cycle_id=header.source_cycle_id here:** the original historical same-work resolution lookup does not filter cycle. Preserve same source group/work/Candidate/client/episode/comparison; use the actual prior historical comparison and decision IDs.

The generated issue SHA is the genuine original generated15-field issue parsed by0038 (optional expected fingerprint absent/NULL), not a reconstructed full-row snapshot. Historical matched-facts SHA covers only the real narrow comparison terms. Their relationship must be checked inside the original branch while actual issue JSON is available; two supplied one-way SHAs cannot prove it later. Retained assertions recheck immutable IDs/digests and branch-complete evidence, without today's incident state/pointer requirement.

## 3. `private.bpay_next_candidate_compare_omission_completion`

Require common/exact comparison stack and header tail; current count below header.omitted_count; incident ID in header.omitted_incident_ids, work ID absent from generated_work_ids. Bind actual incident episode/pointer to the supplied comparison and actual Candidate/client/source group/**same original cycle**/Contract/work-date week. This cycle predicate is required for omissions because2203's original omission selector includes it.

At INSERT the original owner has genuinely resolved it SOURCE_MATCHED. Require the exact immutable RESOLVED event with SYSTEM actor/null actor ID/null expected fingerprint, actual episode, current call publication, dedupe key `SOURCE_MATCHED:<incident UUID>:<publication UUID>`, and payload resolution_kind SOURCE_MATCHED plus origin CANDIDATE_TIMESHEET_SUBMISSION. Original event helper SHA alone does not cover origin; retain0121's extra origin-bound digest. Final retained assertion must not demand the incident still be resolved today.

Original PREFINAL-only coverage selection belongs in that future registered owner. Do not impose it on normal MATERIALISE omissions or the original whole-week final open count.

## 4. `private.bpay_next_candidate_materialise_completion`

The exact legal current stacks are:

| caller_kind | True owner_kind | Exact stack |
|---|---|---|
| DIRECT | CANDIDATE_MATERIALISE | `{CANDIDATE_MATERIALISE}` |
| CANDIDATE_SELF_SUBMIT | CANDIDATE_SELF_SUBMIT | `{CANDIDATE_SELF_SUBMIT,CANDIDATE_MATERIALISE}` |
| CANDIDATE_APP_SUBMIT | CANDIDATE_APP_SUBMIT | `{CANDIDATE_APP_SUBMIT,CANDIDATE_MATERIALISE}` |

Require no prior MAT/root adjunct and exact current producer. Current original delegates pass the same real request UUID as the root cause key. Direct MAT root request SHA equals the original Materialise request SHA. SELF root SHA should bind its exact server-built delegated Materialise request and same key. APP root request SHA is a different original APP hash; never require it to equal the Materialise child SHA.

FRESH: save only after2203:2747–2748 final original cached reply UPDATE. Validate the scoped workflow/account/Candidate/environment and actual result generation/state, last request key/hash, returned full original reply SHA, actual Timesheet revision/hash and signature-copy immutable workflow/generation/lineage/content. Use actual request immutable-submission SHA. SUBMIT_TIMESHEET requires the actual submitted membership and already-complete compare certificate ordinal1, same original scope and comparison result SHA; CHECK_HOURS and SELF_SUBMIT require no compare and no MAT Query stages. SELF's generation/publication/request scope/membership tuple remains null. APP CHECK_HOURS can have real CandidateResponse stages later; MAT's zero-stage requirement is not an APP root zero-stage rule.

EXACT_LAST_REPLAY: the original2301–2309 scoped key/hash/cache branch precedes current generation/state/Timesheet business validation. A pure pre-entry replay returns without producer/certificate. A locked race or inherited replay must return the same original cached reply with its original idempotent_replay behavior, save an actual zero-new-stage MAT replay certificate only if needed by the parent, then leave MAT; it must not create a child metadata command. Do not demand today's WORKER_SUBMITTED/current generation/current Timesheet/current membership/current publication as a replay condition. Derive immutable-submission SHA from the exact request object whose original request SHA matched the cache, not today's workflow snapshot. Do not adopt an older comparison into this transaction: replay compare reference stays NULL.

Replay scope refinement: original cached result contains workflow/result generation/Timesheet revision/hash/signature-copy ID/request kind/comparison/submission-complete result/materialisation request SHA; it does not contain Contract/CWeek/week/environment/submission SHA.0111 currently requires Contract/CWeek/week.07082026_2120:2473–2489 does retain creation-derived Contract/CWeek/week in immutable creation_identity_json, but its creation guard freezes that JSON/hash, not the mutable workflow scope columns. Thus creation identity proves original creation scope, not automatically the scope of every later Materialise invocation. For an owned replay, prefer a genuinely prior immutable FRESH certificate bound to exact workflow/key/request/cache result where available; otherwise preserve exact cached-reply authority and explicitly treat any extra fields as replay context, never as original submitted terms. If downstream assertions require these fields to be historical Materialise terms for pre-protocol caches, a narrow replay-branch schema refinement is needed; adding current scope validation would change the original replay contract. This gap concerns historical term attribution, not permission to refresh accepted financial inputs.

## 5. `private.bpay_next_candidate_root_completion`

Require exact singleton stack `ARRAY[owned.owner_kind]`; owner_kind/cause_kind one of MAT/SELF/APP; root request key/SHA/Candidate bind the actual root identity. Require no existing root adjunct, no witness assigned, input OPEN and BUSINESS_OPEN. Verify all already-saved child certificates before the root row INSERT; its row digest then binds the complete union.

DIRECT_MATERIALISE requires actual MAT certificate with caller DIRECT and same producer, original request/result SHA and complete actual Query range. SELF_SUBMIT requires actual MAT with caller SELF, no comparison and zero root Query stages. Their private root witness ID may be the producer UUID and SHA the root adjunct digest, without claiming a new original business receipt.

APP_RECEIPT_ATTACH requires the real immutable public.weekly_candidate_app_mutation_receipts FINAL_SUBMIT row: exact receipt UUID/root key/original APP request SHA/Candidate/full actual response SHA. WHOLE_WEEK_REVISION has the actual APP-called MAT certificate; RESPONSES_ONLY has no MAT. Response child evidence is the exact original immutable response/event set under the actual same Candidate/root response key, not a fresh reconstruction from mutable incident lifecycle. CandidateResponse has already left by this root tail, so do not require top CANDIDATE_RESPONSE here. The final raw Query stage manifest must equal the complete union of the actual comparison range and actual response stages; no extra stage outside it is allowed. Root witness extension must successor the latest0058 full assertion and preserve its existing manual/response/System/public QuerySync branches.

An APP private composite SHA may bind its original receipt response SHA plus the adjunct SHA. Preserve the actual business receipt payload/hash/ID unchanged. Insert the adjunct only after the original projection/draft-retirement and receipt tail, not inside MAT or before CandidateResponse returns.

## Save/assert APIs and insertion versus retained checks

Keep planned private typed save/assert APIs; helpers receive only exact owner-local typed values plus transient request/issue/result JSON needed to verify existing SHA, never stored full raw input snapshots. Compare save inserts header/children, asserts complete input/omission manifests/counters, original typed witnesses and raw range, then returns header ID/SHA. MAT save binds actual request/result/original copy and compare certificate at its own true tail. Root save binds complete actual child union and actual APP receipt where applicable.

Each save starts with the same exact current owner-stack/lifetime assertion as the trigger; triggers protect direct accidental INSERT too. Retained assert helpers must be usable after commit without requiring current xid, OPEN phase, NEXT active owner, current workflow state, current incident pointer or current source publication. These live predicates prove legal INSERT timing only. Retained proofs bind immutable producer identity, schema triple, recorded raw stages and original immutable evidence. A complete original0-stage SELF or replay is valid and finishes without creating a metadata command.

## Source receipts

SHA-256 observed before this note (full-file source reads; no executable validation):

| Source | SHA-256 |
|---|---|
| 10102026_0111 Candidate schema | `2f6c039ad7ac175e0d6aa486baa888dbe9d18f0292b80d577ac4a4925ad5df8a` |
| 10102026_0032 producer entry | `ab774f951ba157ad7705f31e461db145d4a9ff2ca4d6155e58a87853814a44f1` |
| 09102026_2357 producer lifetime/stage | `996ccbb22641cd41ced5f0d4ff15d88633e2f768c4b1f7def6a4c1b3885792bf` |
| 10102026_0002 standalone finish | `a253e956f0f55ed3fc0e03dafbe97c690d800a05bf532f402c51c8e81641b759` |
| 10102026_0058 final witness assertion | `8027ed7810e0345b4370a897551ddca3fae0868119728253ab9fa4abe5b09ce8` |
| 10102026_0017 current APP/response owners | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| 15092026_2203 original Candidate owners | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |

These receipts do not authorize use of a later concurrently changed source without a fresh reread. No final installation/runtime or whole-owner completion verdict is claimed.
