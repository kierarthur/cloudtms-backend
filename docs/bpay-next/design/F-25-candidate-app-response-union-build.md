# F-25 APP actual CandidateResponse union build

10 October 2026. Read-only finite design against applied0111, additive0128, current0017 APP/response bodies and0121 range/digest helpers. No product edits, SQL, runtime, tests or fixtures. Suggested helper names are design APIs. Root alone implements. This is one APP child/manifest closure, not whole F25 or Source closure.

## Actual finite branches and zero-stage distinction

Current0017 APP invokes CandidateResponse exactly once in RESPONSES_ONLY at1042–1048 and in WHOLE_WEEK_REVISION/CHECK_HOURS at1113–1119. Both pass `request_idempotency_key=v_key`, Candidate `v_candidate_id`, generation `p_request_id`, publication `v_cycle.current_projection_publication_id`, and the actual `v_internal` packet. The latter packet has its expected_timesheet_hash replaced with the actual MAT return at1105–1112 before the child call. WHOLE_WEEK_REVISION/SUBMIT_TIMESHEET has no response callback and requires original public responses empty. No APP→SELF edge exists.

CandidateResponse orders its response items by native incident UUID at0017:490–492, hashes the original request semantics at497–503, and checks its immutable response-event replay at506–525 before generation/current publication/Timesheet/incident validation or nested producer entry. A pure child replay can therefore return under the existing singleton APP stack without pushing/popping CANDIDATE_RESPONSE. Fresh child work pushes `{CANDIDATE_APP_SUBMIT,CANDIDATE_RESPONSE}`, finishes all original notice/cohort/due-intent work, then leaves at838. Its original API/result shape stays unchanged.

Only choice CANDIDATE_WRONG changes the canonical incident state and produces one Query stage. CANDIDATE_CORRECT/NEITHER_CORRECT change other original workflow/outreach fields but not the canonical state/pointer tuple. Thus actual response callback work may have **positive immutable response count and zero Query stages**, including a genuine exact child replay. APP_EXACT_REPLY differs: the original locked APP receipt branch returns before any callback, and its root adjunct must have no response key/count/children/stages. Do not infer callback presence from stage count or from globally existing events sharing the root key.

## Minimal private API and true-root live operands

Suggested APIs (no new business receipt, command UUID or raw snapshot):

```sql
private.bpay_next_candidate_app_response_evidence_v1(
 p_request_key uuid,p_candidate_id uuid,p_generation_id uuid
) returns table(
 item_count integer,publication_id uuid,request_semantic_sha256 bytea,
 resolved_pair_count integer,response_set_sha256 bytea
);

private.bpay_next_candidate_app_response_stage_assert_v1(
 p_producer_id uuid,p_request_key uuid,p_candidate_id uuid,
 p_generation_id uuid,p_expected_item_count integer,p_expected_set_sha256 bytea
) returns integer; -- actual response stage count outside certified compare ranges

private.bpay_next_candidate_app_response_live_assert_v1(
 p_producer_id uuid,p_child_request jsonb,p_child_result jsonb
) returns table(item_count integer,response_set_sha256 bytea);
```

All retain private invoker/STABLE/ACL conventions as appropriate; live assertion checks exact current-xid OPEN/BUSINESS_OPEN/module epoch/singleton APP stack after the child has returned. Retained evidence/stage assertions do not require an OPEN producer or today's mutable lifecycle. A finite child callback flag may be local to the original APP body, never a new user input or authorization substitute.

At each existing actual call seam save the exact server-built child request JSON in a local variable, call the same original child with it, and immediately verify returned original child result plus immutable event manifest. Preserve original packet normalization and expected Timesheet-hash replacement. Actual packet/result JSON is transient only. ROOT cause/request SHA stays the original APP hash, distinct from CandidateResponse semantic SHA.

The live helper must parse exactly the original CandidateResponse fields/types; normalize the actual responses with native incident UUID ordering, no added text/hex/time normalization. Compute exactly:

```sql
private.weekly_source_sha256_jsonb_v1(
 'WEEKLY_CANDIDATE_RESPONSE_REQUEST_V1',
 jsonb_build_object('candidate_id',actual_candidate,
  'candidate_generation_id',actual_generation,
  'projection_publication_id',actual_publication,
  'responses',actual_native_uuid_ordered_responses)
)
```

Require child key=root cause UUID, Candidate=root/actual receipt Candidate, generation=actual APP p_request_id/receipt.candidate_generation_id, actual callback item count1..1000, no duplicate/null incident IDs, exact immutable event incident set equality with that actual packet, each event choice equals the original normalized item choice, actual publication equals the call operand, and every event's request hash equals this exact child hash. Fresh child result must have request key and answered_count equal the exact count; resolved_count equals actual CANDIDATE_WRONG/pair count. Preserve original replay result's smaller shape: replay=true has key/answered_count but no resolved_count/notices/intent fields. Never demand these absent fields or pretend the replay generated new stages.

After the actual callback returns record positive item count/key/set SHA even when new stage count0. Noncallback SUBMIT_TIMESHEET uses explicit key NULL/count0/domain empty-set SHA. APP_EXACT_REPLY also uses the empty response manifest and prohibits any actual callback or compare/MAT metadata. Branch knowledge comes from the genuine original APP body and its current private save tail, not a scan that silently adopts earlier global work.

## Exact immutable response-event manifest

`public.weekly_discrepancy_events` is IMMUTABLE_APPEND_ONLY in the actual1534 ACL contract and gets UPDATE/DELETE/TRUNCATE guards; its unique `(event_kind,idempotency_key)` is the original identity. Draft/draft-item tables are STATEFUL_SERVER_OWNED and must not become retained historical proof.

The exact complete response set is all events with:

```sql
e.event_kind = 'CANDIDATE_RESPONDED'
AND e.bounded_payload_json->>'request_idempotency_key' = p_request_key::text
```

Do not filter malformed rows away and then count the survivors. Count the full selected set, require1..1000, and reject any row that fails qualification. Every row requires:

```sql
e.actor_kind = 'CANDIDATE' AND e.actor_user_id IS NULL
AND e.projection_publication_id IS NOT NULL
AND e.expected_comparison_fingerprint IS NOT NULL
AND octet_length(e.expected_comparison_fingerprint)=32
AND e.idempotency_key = 'CANDIDATE_RESPONSE:'||p_request_key::text||':'||e.incident_id::text
AND (e.bounded_payload_json->>'request_item_count') IS NOT DISTINCT FROM actual_count::text
AND coalesce(e.bounded_payload_json->>'request_semantic_hash','') ~ '^[0-9a-f]{64}$'
AND coalesce(e.bounded_payload_json->>'response_fingerprint','') ~ '^[0-9a-f]{64}$'
AND coalesce(e.bounded_payload_json->>'choice','') IN
 ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT')
```

Require one exact common semantic SHA and one actual publication UUID, distinct incident count=event count. Bind actual immutable incident identity Candidate (0017 incident identity guard freezes Candidate/client/work/cycle/group); do not read its state/episode/pointer as retained lifecycle evidence. Bind each actual comparison by unique material_comparison_fingerprint and incident_id, using real immutable comparison rows. Its own projection publication may be earlier than the response publication; do not add equality. At live assertion also require each item expected_comparison_fingerprint to equal the actual event bytes and selected immutable comparison.

Keep the actual response_fingerprint bytes as original immutable evidence, not a guessed digest. The real Candidate body computes them by the existing six-argument `weekly_source_query_response_fingerprint_v1(incident_id,comparison_id,candidate_id,draft_version,request_key,item_sha)` call, where item_sha has domain WEEKLY_CANDIDATE_RESPONSE_ITEM_V1. The single existing function signature's parameter/domain labels are also used by Manager response; preserve this exact original call and bytes. Do not introduce a renamed/reinterpreted Candidate fingerprint formula. Live fresh owner-local proof can compare actual computed bytes; retained proof hashes actual immutable event bytes without looking up today's mutable draft version/items.

For every CANDIDATE_WRONG event require the exact actual conditional original RESOLVED pair:

```sql
r.event_kind = 'RESOLVED'
AND r.idempotency_key = 'CANDIDATE_CORRECTED:'||e.id::text
AND r.incident_id=e.incident_id AND r.issue_episode=e.issue_episode
AND r.projection_publication_id=e.projection_publication_id
AND r.expected_comparison_fingerprint=e.expected_comparison_fingerprint
AND r.actor_kind='CANDIDATE' AND r.actor_user_id IS NULL
AND r.bounded_payload_json->>'resolution'='CANDIDATE_CORRECTED'
```

Require exactly one by actual original unique key. For the two other choices require no event with that conditional key; an unrelated later resolution event is neither imported nor prohibited. The pair is genuine historical original evidence even for a pure child replay; do not infer a new stage solely from pair existence.

Concrete response-set SHA: SHA256 UTF8 of a JSONB array with domain `BPAY_NEXT_CANDIDATE_APP_RESPONSE_SET_V1`, actual request UUID, Candidate UUID, generation UUID, actual response publication UUID, common semantic SHA hex, exact item count, and native event-ID ordered manifest. Each manifest item includes e.id,incident_id,issue_episode,publication,expected fingerprint hex,event_kind,actor_kind,actor_user_id,idempotency key,request count/hash,response fingerprint,actual choice,actual immutable comparison ID; conditional pair is NULL or typed array `[r.id,r.incident_id,r.issue_episode,r.projection_publication_id,fp_hex,r.actor_kind,r.actor_user_id,r.idempotency_key,resolution]`. These terms match original0017 response digest's evidence while adding explicit APP child scope; no contact/raw answer/corrected-hours JSON is stored. Use an explicit domain empty-set array for key NULL/count0, never SQL NULL aggregate or a fabricated response request hash.

The current `bpay_next_query_response_completion_digest_v1(uuid)` cannot be directly called for APP: it requires root owner MANAGER_RESPONSE/CANDIDATE_RESPONSE and compares owned.request_sha256 with the child hash. APP root SHA differs. Extract/reuse a finite scoped pure evidence core or author the APP-specific helper above; preserve full standalone Manager/Candidate behavior and latest0058 witness assertion branches.

## Stage classification outside genuine compare ranges

Start from the full actual owned stage set: event_owner→input_version_event→query_input_version for this producer. Validate no missing joins, PAY_QUERY kind, exact xid/epoch, no original_command_id, relation/row agreement and old/new tuple SHA integrity through0121 raw-range checks. Whole root starts at ordinal0 and ends at the exact captured actual transaction tail; count equals next_event_ordinal<=1000. Do not hide extra stages by filtering before the census.

Each compare child range is exact `(begin_event_ordinal,end_event_ordinal]` from the already-complete same-producer typed compare certificate. Validate child count/set/digests, nonoverlap and MAT linkage first. The response set of stages is precisely the complement:

```sql
NOT EXISTS (
 SELECT 1 FROM private.bpay_next_candidate_compare_completion c
 WHERE c.producer_id=p_producer_id
   AND stage.transaction_ordinal>c.begin_event_ordinal
   AND stage.transaction_ordinal<=c.end_event_ordinal
)
```

For every complement stage require exactly the genuine CandidateResponse incident transition:

```sql
stage.owner_relation='public.weekly_discrepancy_incidents'
AND stage.old_present AND stage.new_present
AND q.old_gate_tuple->>'state'='OPEN'
AND q.new_gate_tuple->>'state'='RESOLVED'
AND (q.old_gate_tuple-'state') IS NOT DISTINCT FROM (q.new_gate_tuple-'state')
AND q.candidate_id=p_candidate_id AND q.root_timesheet_id IS NULL
```

Bind `stage.owner_row_id=e.incident_id`, actual old/new tuple Candidate and immutable comparison pointer to the selected comparison ID whose fingerprint equals the actual response event's expected fingerprint. Require that event choice CANDIDATE_WRONG and its exact conditional RESOLVED pair above. Count at most one complement stage per actual response incident. Require every complement stage to match one event/pair and no residual relation/callback. No current incident state/pointer or publication lifecycle check belongs in this retained proof.

Fresh callback: actual complement stage incident set equals its exact CANDIDATE_WRONG event set, and stage count=child result.resolved_count. Pure child replay: actual complement stage set empty even if historical pairs exist. The trusted original call's returned replay branch decides this live insertion distinction. Later retained proof permits either genuine empty complement or a complete response transition set consistent with recorded raw stages: `response_stage_count=0 OR response_stage_count=resolved_pair_count`, and when positive its incident set must equal the entire exact CANDIDATE_WRONG set. A partial positive subset refuses. It does not pretend historical pairs are same-transaction writes. The root certificate hashes the full exact stage manifest, response manifest and child compare manifest, preserving that distinction cryptographically without a new business receipt.

Current finite APP branches normally separate further: RESPONSES_ONLY and WHOLE_WEEK_REVISION/CHECK_HOURS have no private compare; WHOLE_WEEK_REVISION/SUBMIT_TIMESHEET has its private compare and no CandidateResponse. Implement the full union algebra to catch any genuine unexpected callback, not to broaden the original branch set.

## True root receipt and retained authority

After all children leave, retain original draft supersession, actual projection, seven-field original full result and actual FINAL_SUBMIT INSERT RETURNING id at0017:1122–1150. Save the adjunct only after that real receipt tail and before standalone finish. Bind receipt Candidate/generation/key/mutation kind/request hash to actual original root operands; original response_json and SHA remain unchanged. ROOT result_sha256 binds actual returned full result (fresh equals stored receipt response). APP_EXACT_REPLY needs separate actual returned idempotent_replay=true SHA versus earlier immutable original receipt response SHA; never equate them accidentally.

APP_RECEIPT_ATTACH saves actual MAT reference where required, callback key/count/response-set SHA, compare-child count/set SHA and full raw stage count/endpoints/SHA. Empty stages require NULL root endpoints; positive response callback count remains independent. APP_EXACT_REPLY saves actual existing receipt and submission kind with canonical empty manifests and no child rows/owned stages. Pure pre-entry APP replay has no certificate.

A private APP witness composite can bind producer ID, actual receipt UUID, original receipt response SHA and root adjunct SHA, retaining actual receipt UUID as witness ID. Successor the full latest0058 completion-witness assertion, not0017's obsolete earlier definition. Retained assertion rebuilds complete immutable response/compare/raw manifests and receipt proof only; no current generation state, current incident lifecycle, current Timesheet or current Source head selects terms. All certificate/business FK proofs complete before the one root clock; no child command and no post-clock callback/append are admitted.

## Source receipts

| Source | SHA-256 |
|---|---|
| Current0017 APP and CandidateResponse owners | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| 0121 typed Candidate/range digests | `ed7afd09a2ef94717dc403e4686d764c35d152fc9f83c540bd7a5eda5177d5b7` |
| 0128 exact APP replay additive branch | `c1e32e41a2cd8fefd095927f822c2394ef2cfcfe02c226cfb7d7b16670e9c53c` |
| Original1534 delivery/fingerprint owner | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| Original1534 schema | `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d` |
| Original1534 ACL/immutable event contract | `5cae1d5dafd9fce93d8a3d9f5ee008e67abe14e099087388d656ae7e21956da5` |

No schema execution, compiled-body proof, actual owner-successor review or whole F25 completion claim accompanies this design.
