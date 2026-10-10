# F-25 APP response helper field and SQL seam inventory

10 October 2026. Finite source-only implementation inventory supplementing `F-25-candidate-app-response-union-build.md`. Freshly read0017 response/request/results, actual1534 schema/ACL,2153/2244/2352 stage schema and0121 raw helper. No product edits, SQL, tests, runtime or fixtures. Root implements.

## 1. Exact original child request and result semantics

`public.weekly_source_candidate_response_submit_atomic_v1(jsonb)` at0017:426 accepts exactly five keys: candidate_id, candidate_generation_id, projection_publication_id, request_idempotency_key, responses. IDs/key cast to UUID. Responses must be a nonempty array and are sorted by **native `(value->>'incident_id')::uuid`**, retaining each original item JSON unchanged. Do not uppercase choices, rewrite timestamp text, remove JSON-null keys or normalize fingerprint case before the original request SHA.

At497–503 it calls `private.weekly_source_sha256_jsonb_v1('WEEKLY_CANDIDATE_RESPONSE_REQUEST_V1', jsonb_build_object(...))` with Candidate UUID, generation UUID, publication UUID and that exact ordered responses array. Request UUID is deliberately excluded from semantic hash. The actual shared primitive hashes UTF8 `domain || chr(31) || jsonb::text` via extensions.digest SHA256; it is **not** bare SHA256(JSONB), and not a new JSONB array wrapper. Reuse the exact helper.

Each original response item allows incident_id, expected_comparison_fingerprint, expected_timesheet_hash, choice, corrected_start_at_local, corrected_end_at_local, corrected_break_minutes. At608 choice validation uppercases/trims a local value **after** request hash computation. Item SHA at645 uses the unchanged actual item JSON and domain WEEKLY_CANDIDATE_RESPONSE_ITEM_V1. Preserve exactly the existing six-argument response-fingerprint function call and bytes; its manager-oriented parameter names/domain do not license changing Candidate behavior.

The exact pure replay selection is all CANDIDATE_RESPONDED events whose payload request_idempotency_key equals canonical parsed UUID text. At508–524 original replay checks count/min recorded item count/min semantic hash, then returns only:

```text
ok=true; replay=true; request_idempotency_key=actual UUID;
answered_count=existing event count
```

No resolved_count, office_notification_count or manager_message_intent_id exists in that result. It returns before generation/cohort/publication/incident/Timesheet checks and before nested producer entry. Do not manufacture defaults for absent fields or run fresh validation to certify this branch. At a live APP call seam, verify the actual packet hash/count/set using immutable originals; retain the original child replay result shape.

Fresh result at828–832 is exactly:

```text
ok=true; replay=false; request_idempotency_key=actual UUID;
answered_count=v_answered; resolved_count=v_resolved;
office_notification_count=v_notices; manager_message_intent_id=v_intent_id
```

v_answered is checked equal actual response array length at733. v_resolved increments once for each CANDIDATE_WRONG; notices/intent are original workflow results, not Query stage counts or immutable response-set cardinality. Fresh child leaves the inherited owner only after all original cohort/notice/intent work at838. Its return schema remains unchanged.

## 2. Real immutable event/comparison fields and guarantees

| Row | Actual fields used | Existing guarantee / additional helper check |
|---|---|---|
| weekly_discrepancy_events | id UUID; incident_id UUID; issue_episode integer; projection_publication_id nullable UUID; expected_comparison_fingerprint nullable bytea; event_kind text; actor_kind text; actor_user_id nullable UUID; bounded_payload_json JSONB; idempotency_key text; occurred_at_utc timestamp | PK id; unique(event_kind,idempotency_key); episode>=1; optional fingerprint NULL-or32; JSONB object. Event kind/actor enums and FKs exist. Actual Candidate response must explicitly require nonnull publication/fingerprint, actor CANDIDATE/null actor_user_id and exact key/payload; schema alone does not. |
| weekly_issue_comparison_revisions | id UUID; incident_id UUID; revision_number integer; projection_publication_id UUID; material_comparison_fingerprint bytea; optional candidate_timesheet_id UUID/revision | PK; unique(incident_id,revision_number); **global unique(material_comparison_fingerprint)**; fingerprint NOT NULL/32; publication NOT NULL/FK. Actual fingerprint therefore resolves one immutable comparison; require comparison.incident_id=response incident. No publication equality with response event. |
| weekly_discrepancy_incidents | id UUID; candidate_id/client_id/work_event_id/source_cycle_id/source_group_id UUID; episode_number; state; current_comparison_revision_id; lifecycle/action/resolution fields | Identity six-tuple is frozen by current0017:854–856. Row is stateful. Retained proof may check that fixed Candidate/work identity, never current state/episode/current comparison pointer/action flags. Comparison pointer FK alone does not bind comparison.incident_id to this incident. |
| weekly_candidate_app_mutation_receipts | id UUID; candidate_generation_id UUID; candidate_id UUID; mutation_kind text; idempotency_key UUID; request_hash bytea; response_json JSONB; created_at_utc | PK; idempotency_key globally unique; additional generation/kind/key unique; immutable append-only; request SHA NOT NULL/32; response JSONB object. Require actual FINAL_SUBMIT kind, root key/hash/Candidate/generation and full original response SHA explicitly. |

1534 ACL contract marks events, comparison revisions and APP receipts IMMUTABLE_APPEND_ONLY and dynamically installs UPDATE/DELETE/TRUNCATE guards. Incident, generations, memberships, response drafts and response draft items are STATEFUL_SERVER_OWNED. Do not use today's draft_hash, draft_version, submitted state or corrected-hours rows as retained completion authority. Event issue_episode is the original recorded episode; current incident episode_number does not select historical terms.

Candidate response payload has exactly these original evidence fields:

```text
choice (original normalized local choice)
response_fingerprint (original computed32 bytes encoded lowercase64 hex)
request_idempotency_key (actual UUID encoded by JSONB)
request_semantic_hash (actual original request SHA lowercase64 hex)
request_item_count (actual packet array length)
```

No Candidate generation ID, draft version, corrected hours, Timesheet hash or raw item is separately stored in this immutable event payload. The **live** packet hash proves those actual request inputs before saving the root certificate; retained event/set SHA preserves that proof. It cannot reconstruct the original request JSON from the event afterward. No extra full-row/raw request snapshot is needed.

## 3. Common immutable evidence extraction

A reusable private scoped core can accept finite kind MANAGER_RESPONSE or CANDIDATE_RESPONSE, actual request UUID and explicit expected child semantic SHA. Do not read APP producer.request_sha256 as the child semantic SHA. Existing0017 `bpay_next_query_response_completion_digest_v1(uuid)` binds root.kind/key/hash and refuses APP; calling it under APP without a full finite scoped extension is incorrect.

For Candidate select the **complete** immutable set by event_kind=CANDIDATE_RESPONDED and payload.request_idempotency_key=actual UUID text. Count first, then reject any malformed row instead of filtering away malformed evidence. Require exact count1..1000 and distinct incident count equal count. Null-safe rejection clauses include:

```sql
e.actor_kind IS DISTINCT FROM 'CANDIDATE'
OR e.actor_user_id IS NOT NULL
OR e.projection_publication_id IS NULL
OR e.expected_comparison_fingerprint IS NULL
OR e.idempotency_key IS DISTINCT FROM
 ('CANDIDATE_RESPONSE:'||actual_key::text||':'||e.incident_id::text)
OR (e.bounded_payload_json->>'request_item_count') IS DISTINCT FROM actual_count::text
OR (e.bounded_payload_json->>'request_semantic_hash') IS DISTINCT FROM encode(actual_child_sha,'hex')
OR coalesce(e.bounded_payload_json->>'response_fingerprint','') !~ '^[0-9a-f]{64}$'
OR coalesce(e.bounded_payload_json->>'choice','') NOT IN
 ('CANDIDATE_WRONG','CANDIDATE_CORRECT','NEITHER_CORRECT')
```

When retained core derives child SHA from immutable payloads rather than receiving the original packet SHA, require every row's SHA be canonical lowercase64 and exactly the same, not just MIN(hash). Resolve one comparison by global unique fingerprint and bind incident ID; bind original fixed Candidate identity separately. Require one actual publication UUID for this original single callback. Do not cast malformed payload count/hash first; reject through exact text/hex predicates before casts.

For CANDIDATE_WRONG fetch the one RESOLVED event by exact key `CANDIDATE_CORRECTED:<actual CANDIDATE_RESPONDED event UUID>`. Existing event-kind/key unique guarantees at most one, **not its existence or correct fields**. Require actual matching incident/issue_episode/publication/fingerprint, CANDIDATE actor/null actor_user_id, payload.resolution CANDIDATE_CORRECTED. The other two choices require no such conditional key. Do not import unrelated later resolution events. Return original response-event UUID/comparison UUID and nullable exact pair UUID/fields, exact counts and digest, not today's incident lifecycle.

Current standalone digest manifest at0017:45–54 is native response-event UUID ordered and records response ID,incident,episode,publication,expected fingerprint,kind,actor,key,payload request count/hash,response fingerprint,review_item_id,response_kind,choice plus nullable typed resolution array. Its domain and layout remain unchanged for existing standalone callers. A new APP-specific domain can additionally bind fixed Candidate/generation/publication and full child semantic SHA. Extracting a common row-proof core must preserve original standalone SHA bytes and all existing Manager behavior, including its actual review-item key and MANAGER_CONFIRMED conditional pair. No broad producer-kind allowance or conversion of root APP hash is sufficient.

## 4. Exact raw Query schema and event-owner fields

| Relation | Exact relevant fields / types and constraints |
|---|---|
| private.bpay_next_query_event_owner eo | event_id UUID PK/FK to query_input_version.event_id; producer_id UUID FK; unique(event_id,producer_id). One stage event cannot be assigned to two producers. No child owner kind or callback marker is stored here. |
| private.bpay_next_input_version_event e | id UUID PK; preparing_transaction_id xid8; module_epoch bigint>0; input_kind text; owner_relation text; owner_row_id UUID; original_command_id nullable UUID; old_present/new_present NOT NULL booleans with at least one true; old_sha256/new_sha256 NOT NULL bytea32; staged_at_utc; **transaction_ordinal bigint1..1000**, unique(preparing_transaction_id,transaction_ordinal) added2244; actual input-transaction FK. |
| private.bpay_next_query_input_version q | event_id UUID PK/FK; owner_relation NOT NULL text; owner_row_id NOT NULL UUID; generated input_kind PAY_QUERY; candidate_id nullable UUID; root_timesheet_id nullable UUID; old_gate_tuple/new_gate_tuple NOT NULL JSONB objects <=16384 bytes each. Two compound FKs bind event ID/relation/row and event ID/input kind/row. |
| private.bpay_next_query_producer p | id UUID; unique xid8; module_epoch; actual owner_kind/cause_kind/key/root SHA; mode/phase; owner_stack; witness fields; metadata_command_id; composite unique(id,xid,epoch). Phase/stack can change operationally; retained manifest binds original immutable identity and actual event associations, not a current OPEN lifetime condition. |

0121 `bpay_next_query_raw_stages_range_v1(uuid,integer,integer)` proves exact contiguous `(begin,end]` count, no concealed missing event/query joins, actual PAY_QUERY/xid/epoch, original_command_id NULL, e/q relation-row agreement and raw tuple SHA equality. It hashes domain BPAY_NEXT_QUERY_RAW_STAGE_RANGE_V1,producer,begin,end and native ordinal-ordered arrays of actual event ID/xid/epoch/ordinal/relation/row/presence/old-new SHA/Candidate/root/old-new tuples. Existing range empty manifest is explicit `[]`, count0/end=begin, first/last NULL. Preserve these exact bytes when reusing it.

Its immutable canonical incident tuple has only candidate_id,client_id,work_event_id,source_cycle_id,source_group_id,state,current_comparison_revision_id. Episode/action/resolution/time fields are absent. Retained response transition proof therefore uses event-recorded episode and actual immutable conditional pair; it cannot demand an episode key inside q.old_gate_tuple.

Outside actual completed compare intervals, require exact incident OPEN→RESOLVED with both present, unchanged tuple apart from state, q.Candidate actual root Candidate and root_timesheet_id NULL. Actual old pointer resolves comparison whose ID/incident/fingerprint bind the original CANDIDATE_WRONG event; pair fields bind original resolution. No full original current incident SELECT is needed. Validate full root stage census before filtering to this complement, reject duplicate/overlapping compare intervals or residual stage relations. Empty complement is allowed for genuine child replay/nonresolving response work; positive complement must equal the complete CANDIDATE_WRONG incident set, not a partial subset.

## 5. APP live insertion seams and finite authority

At0017 RESPONSES_ONLY1042 and WHOLE_WEEK_REVISION/CHECK_HOURS1113, capture the exact existing server-built request into a local variable, invoke unchanged child, then check packet/result/common immutable evidence. For the latter preserve actual post-MAT expected_timesheet_hash replacement at1106–1111. Do not recompute from the earlier public response packet or today's Timesheet after return.

The fresh root's final original receipt tail is1145–1150, after draft retirement and final projection. Existing result seven fields and actual full JSON stay unchanged. Save root adjunct there with actual response manifest/count/key when a genuine callback occurred; original SUBMIT_TIMESHEET has none. Positive count with0 stages is an actual response callback certificate. APP_EXACT_REPLY is the earlier locked receipt race at993–1004: exact prior receipt, no actual callback, canonical empty manifests/no child metadata, no generation/current business checks. Pure APP replay980–982 creates no producer/certificate.

Live guard requires current xid/open input/BUSINESS_OPEN/NEXT epoch/singleton APP stack, existing genuine receipt and complete current child/stage union before clock. Retained helper uses only immutable events/comparisons/receipt/certificates/raw stages plus fixed incident identity; no active generation/current publication/Timesheet/lifecycle requirement. Root receipt response SHA and returned exact-replay result SHA are different where idempotent_replay is changed. Only the private composite witness is extended; no original business receipt/payload/hash/result or metadata clock is fabricated.

## Source receipts

| Source | SHA-256 |
|---|---|
| 0017 actual response/APP | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| 0121 raw/typed digests | `ed7afd09a2ef94717dc403e4686d764c35d152fc9f83c540bd7a5eda5177d5b7` |
| 1534_00 exact hash primitive | `116b4cab99eddf64663a84865f40bbad18fa486101a93e02d14d23a84cd62bf0` |
| 2153 input/stage foundation | `0289b53ad745d5ff5c3a3130ce596e0b5a155e3645a84f1c57b154f8774a5182` |
| 2244 actual ordinal schema | `4fe014fe836d3474c18d77670788814754384a6fab7b889b557a6af55e0e2191` |
| 2352 producer/event owner | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |

No helper/owner implementation, compiled/runtime validation or whole F25 completion verdict is claimed.
