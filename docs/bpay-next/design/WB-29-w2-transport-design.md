# WB-29 W2 native transport design

12 October 2026; sequence140. **DESIGN FINDINGS COMPLETE — required implementation joins only; not product SOURCE PASS or runtime acceptance.** Reuse the existing `/api/banking/pay/next/workbench-command` POST, existing public `bpay_next_workbench_command_v1(jsonb)`, native immutable2320 receipt and eight-second Worker deadline. No new route, Worker job/financial command kind, proof, queue or calculator is required. Root alone authors product.

## Existing seams and smallest changes

| Current source | Exact seam / necessary change |
| --- | --- |
| broker/src/bpay-next-workbench.js:845–891 | Existing command RPC already accepts `options.action` and sends `{p_request:{actor_user_id,action,args,readback}}`. Add the two resolution branches before existing snooze/retry dispatch; use dedicated strict resolution args/receipt validators and native exact-pair faults. Preserve original branch schemas, deadlines and route classes. |
| broker/src/index.js:199158–199172 | Existing authenticated route and RPC dependency suffice. Its retry wake tests `receipt.status==='READY'`; W2 receipt has no status/job_id and must not be disguised as a retry receipt. Native publishers create actual POSITION_APPLY children; no synthetic queue message or invented job is needed. |
| broker/src/bpay-next-resolution.js | Currently W1 validation only. Add action-specific command args/common-context/receipt/fault validators here, reusing decimal/UUID/reason helpers and exact native fault pairs. |
| FE js/bpay-next-resolution.js | Mirror the same closed W2 schemas. Add a small headless original-request journal/controller in this existing module for the later owning callers; do not build WB30–34 screens early. |
| FE js/bpay-next-workbench.js:756–805 | Existing retry controller supplies actor/frame guard, storage and original-byte pattern. Do not use its retry receipt schema or blindly copy its automatic send-after-absent-readback behavior. |
| FE js/bpay-next-snooze-window.js:279–295 | Same command route/three-key envelope is established. Its snooze args, ACK/progress and receipt types remain unchanged. Resolution is a separate closed action branch, not a snooze variant. |
| SQL2241 / SQL2320 |2241 is the raw scope/intent and native common context authority.2320 accepts exact original args, qualifies currentness server-side and returns the immutable publication/decision/adoption receipt. |

## Closed request

Worker JSON keys exactly `{action,readback,args}`: action `RESOLUTION_SAVE` or `RESOLUTION_CLEAR`, readback boolean. Actor comes only from the current authenticated active admin; never accept actor, agency, money, Source units, Source shares, quote, child command IDs or a proof in the caller envelope. Full Worker body retains its32768-byte limit. The native SQL envelope has its own32768-byte JSONB limit; do not assume a compact Worker body proves that native re-encoded envelope fits. Keep the native bound/refusal; no larger general transport ceiling.

Args have exactly six keys:

```text
request_id: lowercase UUID
candidate_id: lowercase UUID
context: exact14-field native common offer context
works: nonempty array, <=100 distinct Work IDs
reason: null or original UTF-8 text <=2000 bytes
zero_confirmed: boolean
```

Each Work has exactly `{work_id,expected_revision_id,rows}`. Each row has exactly `{approved_line_id,rate_family,rate_code,intent,expected_decision_id,expected_decision_version}`. Total submitted rows across Works <=100. UUIDs are lowercase semantic UUIDs; versions are positive bigint decimal strings <=9223372036854775807. Both expected-pair members are null together or both present.

`STANDARD` codes are DAY/NIGHT/SAT/SUN/BH; `ADDITIONAL` is native trimmed uppercase code, <=245 Unicode characters; `ADJUSTMENT` is the exact native component code,1–256 characters. Never reconstruct a code from its display label. SAVE allows each logical line/family/code once and requires non-NULL intent. Its expected pair is W1's current compatible `expected_decision`, or null/null when absent. CLEAR requires NULL intent and the disclosed exact non-NULL owner pair: it may clear an incompatible owner, and may contain multiple different owner IDs for the same logical bucket. Reject duplicate CLEAR owner IDs across the entire request. Do not impose SAVE's logical-identity deduplication on CLEAR.

An intent has exactly `{resolution_mode,manual_replacement_rate,manual_amount_ex_vat}`. SUGGESTED_EQUIVALENT_BASIS requires both operands null. MANUAL_REPLACEMENT_RATE requires rate text and null amount; MANUAL_AMOUNT requires amount text and null rate. Manual modes require nonblank bounded Reason. Preserve original decimal strings, including admitted trailing zeroes, in the immutable body; no Number/parseFloat/toFixed coercion or browser valuation:

```text
rate:   ^(0|[1-9][0-9]{0,11})(\.[0-9]{1,6})?$
amount: ^(0|[1-9][0-9]{0,15})(\.[0-9]{1,2})?$
```

These are nonnegative finite text operands, below10^12 and10^16 respectively, with no exponent/sign/grouping/space/NaN/Infinity. Explicit zero consent is carried only by `zero_confirmed`; the server determines whether a resulting zero requires it. W1's preview/suggestion remains the server's result, not a W2 supplied amount.

## Exact14-field common context

Use the already validated W1 `payload.context` and remove **only** its `works` property. W1 response context has15 keys; SQL2320 equality compares its supplied context with2241's14-key `offer_context_v1`. Passing the full W1 context would therefore always fail stale. Do not reconstruct or silently refresh fields when retrying:

| Field | Closed type / qualification |
| --- | --- |
| agency_id / actor_user_id / candidate_id | UUID; actor equals authenticated/current caller; Candidate equals args and W1 scope. |
| owner_epoch / metadata_version | Positive bigint decimal string. |
| financial_view_revision / workbench_revision / adoption_revision / last_command_sequence | Nonnegative bigint decimal string. |
| target_pay_channel | PAYE or UMBRELLA. |
| target_umbrella_id / target_umbrella_vat_chargeable | PAYE: null/null; UMBRELLA: UUID/boolean. |
| target_umbrella_enabled | PAYE:null; UMBRELLA:true. |
| official_pay_date | Real ISO date from native official context; internal qualification only, not a pre-execute UI pay date. |

The common context is <=4096 encoded bytes in2241. Exact nested keys, integer bounds, nullable semantics and Candidate/actor equality are validated in both mirrors. Native W1 Work/row facts and current/source/Position/decision schemas stay their existing independent read contract; none is copied as a supplied money authority.

## Native receipt and validation

SQL returns exactly `{request_id,candidate_id,received,replay,result}`; Worker success wraps it as exactly `{ok:true,action,receipt}`. Use the existing131072-byte command-response ceiling **only for the new resolution branch**; native result is additionally <=65536 bytes and Work output <=60000 bytes. Existing retry/single Snooze16KiB paths remain unchanged.

Absent readback is only `{received:false,replay:false,result:null}` with the original request/Candidate. A mutation response with received=false is invalid and cannot settle the journal. A received receipt can be fresh (`replay:false`) or original replay (`replay:true`), including when repeating the same mutation finds a prior receipt.

Result keys exactly `{action,candidate_id,owner_epoch,works,processing,adoption}`. Action/Candidate/owner_epoch must equal the original request; `processing` is the SQL-authored true. `adoption` is exactly `{workbench_revision,adoption_revision,adoption_state}`, revisions nonnegative bigint strings, state one of native UNSEEDED/SEEDING/CURRENT/ERROR. These values describe the original committed operation, including queued processing. Never compare historical receipt revisions/state with a freshly read current screen, overwrite them, or interpret processing/adoption CURRENT as proof every Work is Ready.

Each result Work has exactly `{work_id,based_on_revision_id,published_revision_id,command_id,agency_sequence,changes,carried_decisions}`. Match the **exact submitted Work ID set**, not caller array positions:2320 stages/publishes in UUID order. Require sorted unique Work IDs, predecessor equal that Work's original expected revision, new/published and child command UUIDs, positive bigint agency sequence and unique actual child identities. Do not add job_id, status, Run, fabricated completion or money fields.

Each change is exactly `{approved_line_id,rate_family,rate_code,decision_id,decision_version}`. It matches the corresponding original requested logical identity. SAVE versions are1 when no expected pair, otherwise expected version+1; CLEAR echoes the exact requested owner pair. A SAVE request with null pair creates a real new decision; do not infer its UUID from request_id or child command_id.

Each carry is exactly `{previous_decision_id,previous_decision_version,decision_id,decision_version}`; positive strings, new version=old+1, different real UUIDs. Bound all carry entries across Works<=100; reject duplicate previous/new identities and conflicts with requested cleared/replaced IDs or new change IDs. The server carries **all** mapped ACTIVE sibling owners, including untouched rows not present in requested W1 rows. Do not reject a truthful carry merely because its prior ID was outside that partial W1 request; the receipt supplies its actual identity. Where a pair was disclosed, compare its original version. Carried rates/amounts/balances are not reconstructed in transport.

## Exact fault mapping

Keep the existing command fault envelope exactly `{ok:false,error_code,retryable,outcome_unknown}`. W1's read fault envelope/reload_required must not be substituted. Reuse existing W1 exact SQLSTATE/message pairs where reachable, and add only actual W2 pairs below. Known native refused transaction -> outcome_unknown:false; do not match a substring or blanket23514/55000 to turn a structural error into a business refusal.

| Actual pair family | HTTP / handling |
| --- | --- |
|22023 + BPAY_NEXT_RESOLUTION_REQUEST_INVALID / DUPLICATE_SCOPE / IDENTITY_INVALID; BPAY_NEXT_WORKBENCH_COMMAND_INPUT_INVALID |400, definite, retryable:false. |
|23514 + BPAY_NEXT_RESOLUTION_REPLAY_CONFLICT / DUPLICATE_SCOPE / ZERO_CONFIRMATION_REQUIRED / RESIDUAL_AMBIGUOUS |409, definite, retryable:false; replay conflict never permits a new intent over retained unresolved bytes. |
|23514 + BPAY_NEXT_RESOLUTION_STALE / POSITION_BASIS_CHANGED / BASIS_CHANGED / PUBLICATION_INCOMPATIBLE and existing typed identity/bucket/policy/Source/quote refusals |409, definite, retryable:false; stale/basis copy calls for an explicit fresh W1 read after original request recovery, not automatic reselection. |
|42501 + BPAY_NEXT_COMMAND_FORBIDDEN / BPAY_NEXT_WORKBENCH_FORBIDDEN |403, definite. |
|55000 + BPAY_NEXT_MODULE_NOT_ACTIVE / BPAY_NEXT_BINDING_NEXT_REQUIRED |409, definite. |
|55000 + BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE / BPAY_NEXT_RESOLUTION_CONTEXT_UNAVAILABLE |503, definite, retryable:true; retryable means permitted manual next step, never an automatic write. |
|54000 + BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE / existing BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE |413, definite, retryable:false. |
|55P03 + BPAY_NEXT_CANDIDATE_FINANCIAL_FENCE_BUSY |409, definite, retryable:true; same original request may be checked/retried deliberately. A raw unrelated PostgreSQL lock message is not this named pair. |
|55000 + BPAY_NEXT_SOURCE_BOOK_CONTROL_REQUIRED / SEED_BASIS_UNAVAILABLE / SIGNED_SEED_UNAVAILABLE / TYPED_WITNESS_REQUIRED / ZERO_BASIS_UNAVAILABLE / ZERO_COMPONENT_UNAVAILABLE |409, definite, retryable:false; preserve the actual closed refusal. Do not invent zero, Source attribution, binding or automatic seed witness. |
|23514 + BPAY_NEXT_SOURCE_BOOK_PAID_WITNESS_MISMATCH / HELD_WITNESS_MISMATCH / COMPONENT_MISMATCH / SEED_REPLAY_CONFLICT |409, definite, retryable:false; retain Source's original qualification meaning. |
|22023 + BPAY_NEXT_SOURCE_BOOK_SEED_ID_REQUIRED |400, definite. |

Treatment payload/guard failures, unexpected constraints, arbitrary database messages, malformed/truncated/inconsistent receipts, missing Response, network failure, abort or deadline are not reclassified by a broad prefix matcher. Use the existing safe unavailable/invalid fault without database prose; if the original mutation was attempted, keep outcome_unknown:true. Parent/helper Source reviews remain the authority for their finite guard semantics; add other expected pairs only with actual source evidence.

## Original-request recovery and ownership

Before first send, validate and persist the complete original mutation envelope bytes with a qualified `{agency_id,actor_user_id,owner_epoch,candidate_id}` namespace and request UUID. Use existing storage dependency and native current actor/NEXT/frame/selection owner guards; no global journal scan, mutable shared object or freshly adopted context. Invalid retained bytes refuse rather than regenerate. Store failure before send prevents dispatch.

On timeout/malformed response/unknown, retain that exact UUID/action/args/context/manual text/expected pairs. The next explicit Check/Refresh reads the same request with only `readback:true`; it never sends a new request. If not received, retain bytes and expose the permitted deliberate retry of that same original mutation; **do not automatically resend within readback or use a new UUID**. A newer Work/target/date does not rewrite the pending request. A readback transport error with outcome_unknown:false refers only to that read and cannot clear the earlier unknown mutation.

Only a fully typed received original receipt settles success. A recognized definite refusal of the actual original mutation may clear its pending marker; a replay conflict/failed recovery remains preserved and cannot silently authorize a replacement intent. Actor/frame invalidation stops presentation and dispatch without deleting the retained original record. Future callers can read fresh native adoption/job progress separately after receipt acceptance; failure to refresh does not undo or change the original historical receipt.

## Source identity and limits

| Read source | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-workbench.js |44B22BCB1AA4A9787944F2943AB5C821CE54C752BBBCEC8C9AE2AD14E3533341 |
| BE broker/src/bpay-next-resolution.js |E0A063D9865AC23F8336825D0B69F0B18658EA738AE3218B3716004389686BE6 |
| BE broker/src/bpay-next-snooze.js |A07499C7B5B2D960A15C7EA85F13038BE484EF7D2AD95B36982164BB0FC3F084 |
| FE js/bpay-next-resolution.js |D3AE4D6206F4286D5F3E32157BC7E96F46555765A8618ACF2689A2F27B25C720 |
| FE js/bpay-next-workbench.js |329B23B912C69D06DFAA1063725E6E650667D3A3F53CBF2755203E035073EF24 |
| SQL2320 component_resolution |1DD72479BA41C1997439F80B8A2546D285F29CD1F791BBDF28DC2FC615385CE2 |
| SQL2241 resolution_context |098EF7EE8E20B889B85D9702A374CB381A172D07CB47692E56FE20F7CD9D650E |

Read-only source inspection and this Markdown write only. No product, database, business/RPC, fixture, hosted, test, render, timing or installation action occurred. Concrete implementation still needs independent source review. Whole WB29 and later screen/accepted-consumer/saved-rate Snooze joins remain open.
