# F-25 QuerySync original immutable decisions review

10 October 2026. **PASS — bounded source-only helper review.** Reviewed fresh saved `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql`, 6,453 bytes, raw SHA-256 `f5847e3e3fb28a8745d3ee8c29d0dd7dc09740d7c23627b380f358f762c985af`. No product edits, SQL/runtime/tests/fixtures, hosted calls or business execution. This is the uninstalled immutable evidence validator only; full004x QuerySync body, latest-incident/original match selection and whole true-tail completion are pending separate work. No behavioral/installation pass is claimed.

## Fixed finding and fresh disposition

The first reviewed bytes (`2456308f9200b1acf1bc555a70ae88b8d8c76842c97473933ae201751bf3cb03`) allowed a manager/Candidate resolution key with a trailing colon or empty third segment plus later suffix, because split_part(key,3)='' alone did not prove exact key length. Root corrected fresh33–35 to require exact `IS DISTINCT FROM expected_prefix||original.id::text` after selecting the real immutable parent. This closes suffix/trailing-segment acceptance and matches genuine original1534 `MANAGER_CONFIRMED:<event UUID>` / `CANDIDATE_CORRECTED:<event UUID>` keys. No remaining actionable source finding identified in this helper scope.

## Actual original owners and qualified relationships

Compared full original event creation/conditional resolution branches in `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql`:

| Original evidence | Original creation | Fresh0039 qualification |
|---|---|---|
|Office accepted + resolved pair|712–731; event kinds OFFICE_ACCEPTED and RESOLVED, OFFICE actor/actual actor UUID, payload resolution OFFICE_ACCEPTED_SYSTEM_HOURS, exact incident/fingerprint keys.|18–25 exact resolved key, nonnull Office actor UUID, real unique OFFICE_ACCEPTED counterpart and same resolution;61–65 exact pair tuple.|
|Manager response + conditional resolution|1184–1196 immutable MANAGER_RESPONDED with actual requestUUID/semanticHash/itemCount/reviewItemUUID/response fingerprint;1201–1227 conditional SYSTEM_CORRECT or CANDIDATE_DID_NOT_WORK resolution MANAGER_CONFIRMED_SYSTEM_HOURS keyed by genuine responseUUID.|26–57 actual parent UUID, exact resolved prefix+parent UUID, MANAGER_RESPONDED/MANAGER, correct resolving response kinds, canonical request/review keys and bounded request/hash fields; coherent complete set;61–65 exact pair tuple.|
|Candidate response + conditional resolution|2694–2707 CANDIDATE_RESPONDED immutable event;2708–2727 CANDIDATE_WRONG resolution CANDIDATE_CORRECTED keyed by genuine responseUUID.|26–57 actual parent UUID, exact resolved prefix+parent UUID, CANDIDATE_RESPONDED/CANDIDATE, selected choice CANDIDATE_WRONG, canonical request key/fields and coherent complete set;61–65 exact pair tuple.|

The immutable pair tuple is incidentUUID, original issue episode, original projection-publicationUUID, expected material comparison fingerprint, actor kind and actor UUID. `IS DISTINCT FROM` across the whole row correctly treats matching nullable actor UUIDs as equal, and rejects one null/one value. Manager/Candidate original writers omit actor_user_id (NULL); Office supplies actual actor UUID. A global nonnull actor UUID requirement would wrongly reject genuine original Manager/Candidate evidence; the fresh source limits it to Office.

11–17 selects the actual immutable comparison and resolution by IDs, requiring RESOLVED, matching incident, exact comparison fingerprint using IS DISTINCT FROM and nonnull actual historical event publication. No today's lifecycle/source/timesheet/report/policy/head/module read occurs. It deliberately does not force event publication to equal comparison publication or current Sync publication: an unchanged repeat import can preserve an older comparison while a genuine Office/Candidate response references the later accepted publication. Original parent/resolution publication must match each other, as checked61–65.

The manager's conditional candidate-assertion exclusion at original1201–1209 is not re-executed against today's mutable candidate state. Genuine immutable RESOLVED event and parent chosen resolving response prove the actual earlier branch. This is correct historical evidence use. No invented SOURCE_RECHECKED or manual command is substituted.

## Nullable guards and finite input behavior

Both input UUIDs select INTO STRICT; missing/null IDs fail with PostgreSQL strict lookup error, never succeed as an empty proof. Private helper caller supplies required certificate IDs; this is not a public business API promising original error shape.

* Comparison and event IDs/incident/kind/actor/idempotency key have original schema NOT NULL constraints. `resolved.expected_comparison_fingerprint IS DISTINCT FROM comparison.material_comparison_fingerprint` rejects missing expected fingerprint. The actual comparison hash is schema-required32 bytes.
* Missing/unknown bounded payload resolution goes to final ELSE58–59. No NULL predicate falls through into a valid owner branch.
* Office resolution payload comparison uses IS DISTINCT FROM; Office actor UUID explicitly nonnull; both exact canonical keys rely on mandatory stored strings and mandatory comparison fingerprint. Selected original counterpart is unique under original(event_kind,idempotency_key), and final pair equality binds its episode/publication/actor/hash.
* Manager/Candidate selected parent's event/actor and Candidate choice use IS DISTINCT FROM; Manager response_kind uses coalesce before NOT IN. Missing request UUID/count/semanticHash/response fingerprint become empty strings and fail anchored regexes. Casting UUID/integer occurs only after shape guards. Count regex1..1000 prevents overflow and imposes the current finite protocol cap.
* Manager review-item UUID is coalesced before regex; key uses IS DISTINCT FROM even when payload is NULL. Candidate key derives mandatory stored original incident UUID. Selected parent resolution key now uses exact full equality33–35.
* Every event in the same actual requestUUID/kind set must have same actor, identical itemCount and identical semanticHash; payload fields use IS DISTINCT FROM, so missing/different count/hash cannot pass. count(*) must equal selected original requestCount. Count is nonnull and zero cannot equal guarded positive count.
* Final whole pair equality binds nullable and mandatory members consistently. It rejects a real original response from another incident, episode, publication, fingerprint or actor even if its request fields look valid.

Finite1000 cap can refuse older larger genuine response requests under NEXT; this is a bounded unsupported-evidence refusal, not permission to truncate or alter original business history. No such data/execution was inspected.

## Scope of complete response-set proof

The helper validates the **selected resolving parent** fully and requires an actual complete same-kind requestUUID set with coherent semanticHash/itemCount/actor. It does not reconstruct the old whole response JSON or rehash it from later mutable review/draft rows. Genuine immutable original writers are the authority for their retained request semantic hash. This is sufficient for the bounded historical decision being asserted: the selected real parent caused the exact real resolution, and its original request set is not incomplete or mixed-hash.

It does not separately shape-check every nonselected sibling response's item key/choice/fingerprint or prove a new caller-supplied requested item set. Do not describe0039 as a whole manager/Candidate completion receipt for a new request; current0017 response-owner completion handles that different root. No additional sibling-shape refusal is required for this helper's stated selected-parent/coherent-count scope. Such broader checking would not make a one-way original semanticHash a reconstruction of omitted raw request fields and is not necessary to qualify this immutable old resolved decision.

## Declaration, ACL and registry closure

Sole definition is `private.bpay_next_query_resolved_evidence_assert_v1(uuid,uuid)`, returns VOID, PLPGSQL STABLE SECURITY INVOKER, fixed search_path pg_catalog/private/public. It performs only SELECT/qualification/RAISE; no SQL writes, locks, clocks, mutable admission, audit/view side effects, business rows or protocol finalizer. It preserves original writer definitions and original semantic/fingerprint owners without replacing any original function.

68–69 logical owner postgres and explicit EXECUTE revoke from public/anon/authenticated/service_role. Calls occur from actual privileged private/public-definer source owner; this helper supplies no exposed service/browser RPC.

Current `docs/bpay-next/final-definer-closures.json` has exact signature, finalRoot/finalPhysical/allowedPhysical/finalClosures all selecting `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql`, no historical predecessor (new helper), owner F-25 immutable original resolved decision qualification. Source-only physical mapping is consistent; source gate/runtime was not executed by this review.

Pending owner responsibilities remain explicit: original latest incident selection/order and original narrow resolved-match predicate at4663–4687; comparing parsed input to matched facts; binding actual workUUID and captured episode to durable sync_input certificate; complete input/omission sets/counters and raw stage closure; full original auth/locks/restart/outreach/audit true tail and no-event finish.0039 alone does not perform or claim any of these.

## Source provenance

Fresh saved raw hashes at review checkpoint:

| Source | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql` | `f5847e3e3fb28a8745d3ee8c29d0dd7dc09740d7c23627b380f358f762c985af` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql` | `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d` |
| `docs/bpay-next/design/F-25-query-sync-completion-build.md` | `e78e73c208b29aef2e12ae4531959e70d74e38846c92c6b1221ac42128a0714c` |
