# F-13 current scope and existing evidence owners

Read-only design refinement, 10 October 2026. Read the full F-13 card (`01_BUILDER_RULES_AND_FOUNDATION.md:748–767`), checklist D01–D03 (`IMPLEMENTATION_CHECKLIST.md:438–452`), current F-11 consumer/modules, F-18 census and existing audit schema/transport. No product edits, execution, tests, fixtures, SQL/DB, network or secret operations.

## Exact object boundary

F-13 has exactly three objects: D01 exact envelope classifier, D02 durable terminal/reconciled retirement receipt, D03 DLQ/transport-exhaustion procedure. Card DB is **none**. Its shared-queue option A is already fixed; no alternate queue, wrangler/resource change, reverse activation, old-engine fallback, legacy parity work or switch screen belongs here.

The earlier `F-13-retirement-and-external-barrier.md` is historical analysis. Its external tickets, OPEN/CLOSING/QUIET barrier, new admission owner and sender rewiring are not named F-13 objects and must not be imported into this card. F-12-D05/F-17/F-18/F-19 ownership is separate. BD019 remains binding; no producer/resolver/PIN/enroller SQL or parked F-25 file is a dependency.

| Object | Minimum authorized implementation |
| --- | --- |
| D01 | Pure strict classification of the three actual legacy producer families and existing NEXT contracts; fresh current owner at the shared consumer; no normalization/defaults that manufacture a valid family. |
| D02 | Establish real terminal/reconciled facts for the exact durable identity, then commit and verify an idempotent bounded transport retirement audit receipt before ACK. Retrying unknown/nonterminal/missing evidence remains mandatory. It does not complete a financial operation. |
| D03 | Named existing DLQ runbook: bounded classification, native evidence lookup, qualified receipt-before-ACK, durable NEXT rediscovery/requeue, preserved unknown obligations. No destructive cleanup or external execution. |

## D01 exact source inventory

Current index SHA `29cfa90d…` has these producer/reference seams:

| Family | Exact saved fields and constraints | Durable identity |
| --- | --- | --- |
| LEGACY_OPERATION_V1, `buildBankingPayContinuationMessage`, 40489–40549 | Ten fields: `version,operation_id,operation_type,pay_batch_id,root_operation_id,successor_relation,phase,run_after_utc,source,enqueued_at_utc`. Version number 1; six exact operation kinds; native UUIDs/null pairs; relations SELF/CHILD/ROOT/NONE as qualified by original source policy; phase <=80 closed token; valid saved timestamp/null; <=2048 UTF-8 bytes. Preserve original `source` and tuple restrictions in `bankingPayContinuationSourcePolicy` at 40451–40486. CHILD only settlement/remittance; delayed timestamp only SELF. | Actual operation plus its bound batch/root/child relationship. A terminal settlement/remittance child can still require a PAYMENT_EXECUTE root wake (40907–40910). |
| LEGACY_CERTIFICATE_V8, `parseBankingPayWorkbenchCertificateContinuationV8`, 40552 onward | Eight fields: `message_type,contract_version,continuation_sequence,session_id,actor_user_id,certificate_uuid,lease_owner,enqueued_at_utc`. Exact message type and numeric version 1; sequence integer 1..1024; native UUIDs; lease owner nonempty <=512, no control characters; original timestamp. | Exact certificate/session/sequence and linked durable build/work scope. Lease text is never logged. |
| LEGACY_WORKBENCH_WAKE_V1, `parseBankingPayWorkbenchDrainWakeMessage`, 40644 onward | Thirteen fields: `message_type,contract_version,wake_depth,wake_id,wake_reason,origin,scope_kind,requested_profile,previous_drain_invocation_id,session_id,candidate_id,actor_user_id,enqueued_at_utc`. Numeric version 1/depth 1..32; reason PRE_ENTRY_COMMITTED_WORK/POST_PASS_MORE_DUE/LEGACY_MORE_DUE; scope GLOBAL/CANDIDATE/SESSION; profile NUDGE; bounded nullable UUID/text fields as actually produced. | Wake UUID is transport identity, not an operation. Null/global scope needs positive scope closure, not one guessed operation or an absent lookup. |
| NEXT metadata/financial | Keep exact ENROLL/EXPIRY and financial CLAIM/STEP schemas owned by `bpay-next-background.js`, `bpay-next-financial-queue.js` and `bpay-next-job-step.js`. Match version/discriminator/complete allowed field set/types/kind-specific phase/cursor shape; adapter remains authoritative before any RPC. | Durable job/run ID. Existing handler preserves lease nonce/epoch/cursor; no legacy receipt can substitute for it. |

The old parsers/builders coerce strings/numbers, fill defaults, and may replace timestamps. They are references, not proof that the received envelope had the exact original schema. Strict raw checks must precede using their structural rules. Extra/missing/coerced/mixed family fields remain UNKNOWN. The classifier may return only bounded typed IDs and an envelope digest; do not echo payload, lease, JWT, financial data or exception text.

Add the classifier and receipt call only at the existing shared consumer after one-message/interface validation and fresh owner admission, before the NEXT adapter. Under NEXT, a positively qualified legacy receipt permits ACK and reason `BPAY_NEXT_LEGACY_ENVELOPE_RETIRED`; otherwise retry. DISABLED/LEGACY unexpected NEXT deliveries retain the current retry behavior, with no reverse-owner policy inference. No old operation claim/advance/drain path is restored.

## Existing evidence is useful but not a universal disposition

`readBankingPayContinuationOperation(env,id)` at 40888–40891 already uses the service-only PostgREST table route for `public.banking_pay_operations`. It can establish that the exact operation actually exists and binds type/batch/root/status. Its broad `input_json/config_json/result_json` fetch must not be logged or treated as a retirement packet. A new bounded lookup should select only the necessary typed identities/status evidence. **Missing row is UNKNOWN.**

The descriptor's terminal enumeration COMPLETE/FAILED/CANCELLED/REVIEW_REQUIRED is an execution-routing decision, not proof that money/delivery obligations are reconciled. Do not use it as an ACK predicate. F-18's final `public.bpay_next_cutover_readiness_page_v1(jsonb)` in 09102026_2109 provides real service-only, bounded native-ID category pages and current classification. It checks provider/settlement/hold/advice facts through existing helpers; it creates no disposition or retirement row.

Its LEGACY_OPERATIONS selection (lines 168–180) includes nonterminal operations, active leases and operations with unresolved transfer/settlement scope. `OBSOLETE_BOOKKEEPING` is narrowly derived for DRAFT_CREATE/PREVIEW_REFRESH with no used batch, transfer scope, provider attempts or pending/failed settlement. Other selected operations are GENUINE_OBLIGATION or INTEGRITY_REVIEW. Completed rows can be omitted; **absence from this category is not evidence that a supplied operation exists or is terminal**. Workbench sessions/jobs categories (450–456) classify existing rows as obsolete bookkeeping; they do not certify a supplied certificate or global wake's complete linked scope.

The public census accepts `{section,category,after_id,limit}` under `p_request`, UUID keysets and limit <=100. It returns owner/epoch, category counts/classes/items/has_more; ALL is a summary, not a per-operation witness. Its explicit `external_quiet_window:'UNPROVEN',activation_ready:false` remains accurate. Private readiness helpers are not service-callable RPCs. Do not build a generic SQL endpoint or call them through an invented route.

No existing public retirement/disposition RPC was found in current registry or saved SQL. Existing operation-finish/provider/correction/result owners are business writers, not an alternative queue receipt API, and must not be invoked to manufacture terminal state. Candidate/Source-specific retirement functions are unrelated owners.

## D02 can reuse the existing audit table without schema changes

An existing durable **transport** is available: service-role PostgREST SELECT/INSERT on `public.audit_events` (`/rest/v1/audit_events`). Structural baseline lines 617–632 define its UUID ID and audit fields; finishing source supplies UUID/time defaults and `audit_events_pkey` on ID. ACL baseline lines 330–337 grants service SELECT/INSERT. Existing 21092026_1817 adds database-owned authoritative event sequence. This is source evidence of compatibility, not a fresh hosted/catalog probe.

`writeAudit` at index 151620–151708 is unsuitable for D02: it uses return-minimal, swallows failed POST/exception and returns no verified identity. Use a narrow fixed audit transport with the current service headers, bounded deadline/reply and explicit verification; do not change generic writeAudit or other callers.

Concrete receipt flow, conditional on **already established real disposition facts**:

1. Validate exact family and complete raw envelope, establish exact native operation/scope facts, and require fresh NEXT admission. Derive a domain-separated canonical-envelope SHA and deterministic audit UUID from family/schema/digest; never use a random replay ID. Include the native identity in the digest, preserve original timestamps, and exclude transient delivery attempts/owner epoch from the uniqueness key.
2. GET that ID selecting only ID/object/action/reason/after_json/event-sequence fields. Existing receipt permits replay only if the complete bounded receipt packet matches the expected envelope/native disposition evidence. An ID collision or mismatching row is refusal, not ACK.
3. If absent, INSERT the supplied deterministic ID, `object_type:'bpay_next_legacy_envelope_retirement'`, `action:'BPAY_NEXT_LEGACY_ENVELOPE_RETIRED'`, bounded object ID/reason, null actor ID/system role, and a closed <=4096-byte after_json packet containing format version, family, envelope SHA, actual native IDs and real disposition evidence reference/digest. Omit database-owned time/sequence. Do not store raw payload/lease/bank/provider data. No upsert/merge, UPDATE or DELETE.
4. Await a verified successful insert/representation or fresh matching re-read. A collision response alone, lost reply, HTTP success without the exact receipt, or failed read does not permit ACK. On retry, lookup of the deterministic ID resolves the same actual audit event. Revalidate qualification; do not rely on an arbitrary historical audit string as disposition.
5. ACK only after the receipt is verified. The unique durable audit event is the once-only ID/reason log. A supplemental console message can be emitted for confirmed creation; exactly-once console delivery across a crash cannot be promised. Do not claim the receipt changed money state.

This is the exact D02 receipt object using an existing table/PK, not a new DB money owner or external barrier protocol. Audit rows are not globally immutable: current source grants service UPDATE/DELETE and has unrelated Candidate audit updates. This new transport must be insert/read only, validate exact packet on every replay, and must not claim an immutability constraint that does not exist.

**Remaining genuine evidence dependency:** the audit transport stores a qualified retirement; it cannot supply qualification. Current F-18 census has no exact certificate/global-scope completion witness, and no per-envelope terminal/reconciled owner API was found. Those families require real existing linked native facts or a named F-18 actual retained-disposition evidence decision before automatic retirement. Do not stub RETIRABLE, guess that an EMPTY/zero-blocking summary covers a missing identity, invent a closure row, or add an unrelated SQL protocol to satisfy D02. Known nonterminal/unknown operations remain retained. A clean launch with no old work needs no seeded fixture or fabricated disposition; it also does not make arbitrary supplied envelopes eligible for ACK.

## D03 procedure content

Document the existing queue/DLQ and max-retry behavior unchanged. DISABLED may exhaust deliveries to the DLQ; job rows remain authoritative and F-11 READY/LEASE_EXPIRED/EXPIRY lanes rediscover eligible NEXT work after the separate F-26 epoch treatment. DLQ/transport exhaustion is neither financial completion nor evidence of absence.

The later authorized recovery procedure enumerates bounded DLQ pages, classifies exact schemas, looks up actual native identities, verifies qualified retirement receipts before acknowledgement, and uses current durable NEXT wake owners for eligible work. Unknown/missing/nonterminal old obligations stay preserved and are reported with bounded IDs/reasons for their real F-18/F-19 owner. Do not purge, run old recovery engines, repeat provider/mail actions, adopt stale tokens, release holds or change financial status to empty the DLQ. No queue/hosted command is executed as part of this source card.

## Saved-source receipts

| File | SHA-256 |
| --- | --- |
| `broker/src/index.js` | `29cfa90d56aa10b6d86c50c687445a4649c876d3799d64a7287b8a6162d623ec` |
| `09102026_2109_banking_pay_next_cutover_readiness_page_v1.sql` | `980a79151e8cc49f3db8c5c4e1f8cd99aac6039600405e25ec1803d3b7c91add` |
| structural baseline | `aec4d8985fed34c08cfd61521db4ad27b273a85e90251ccd8007eee0489b7730` |
| structural finishing | `367d70659e4ab05a7f18a86fd29b3d74d018d3f916a1e1e38f23e63243bfb304` |
| ACL baseline | `4f6067e78a8a760a1974c9cfb5a49cedf4d46f6cba1ffebfdd5e7fb46f032aa2` |
| `04102026_0550_banking_pay_next_legacy_writer_fence_v1.sql` | `de1314ab23759fe8b584e6757bdca49addb18267d060aab544d98fc015bb0145` |
