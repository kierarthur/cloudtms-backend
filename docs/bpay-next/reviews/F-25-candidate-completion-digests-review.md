# F-25 Candidate completion digest foundation review

10 October 2026. **PASS for saved0121's ten private digest/range/witness functions and registry entries. No actionable source defect found in this bounded foundation.** This does not certify any Candidate whole owner, installation or runtime behavior.

Reviewed BUILD-BE `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-build-backend-20261009`, baseline `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. Scope is complete0121 against the saved0111 schema, Candidate protocol schema design, actual2203 comparison event writer,0030/0043/0059 conventions and actual ordinal/owner schema. No SQL, tests, runtime, fixtures, hosted or network action, secret read or product edit was performed. Only this review file was saved. Parent's statement that0111 is applied is attributed to the parent; this review read its saved schema only.

## Exact typed field coverage and hash domains

Compared each0111 table's typed columns with the complete explicit `p_row.field` list in its0121 digest. There are no unknown or omitted semantic columns:

| Typed table | Columns | Explicit fields hashed | Deliberately excluded |
| --- | ---: | ---: | --- |
| candidate_compare_completion |43|41|completion_sha256; completed_at_utc|
| candidate_compare_input_completion |13|12|evidence_sha256|
| candidate_compare_omission_completion |8|7|evidence_sha256|
| candidate_materialise_completion |36|34|completion_sha256; completed_at_utc|
| candidate_root_completion |24|22|completion_sha256; completed_at_utc|

Compare header binds private call UUID and actual producer/xid/epoch, ordinal/caller/request SHA, six original call operands including call-now, actual Timesheet revision, Candidate/client/Contract/week/group/cycle/profile scope, complete generated and omitted UUID sets and generated SHA, all branch counters, real whole-week open count/outcome/result SHA, typed child digests, original witness count/SHA and actual raw range. Materialise binds every original workflow/request/generation/account/environment/scope/Timesheet/signature/immutable submission/result/comparison child field and actual range. Root binds actual owner variant/request/Candidate, actual Materialise and APP receipt/response identities/digests, complete comparison child census, whole raw-stage count/ordinals/SHA and resultSHA.

Each row/input/omission/request/set/range/original-event suffix uses a distinct explicit V1 domain, positional typed JSON array and UTF8 SHA256. Bytea values are lowercase hex with SQL NULL retained as JSON null; UUID/date/integer/text and xid8::text retain their declared representations. Typed composites exclude their own computed digest, and audit `completed_at_utc` is excluded while semantic `call_now_utc` is included. These choices match0030/0043 conventions without overriding them or changing existing System/public QuerySync domains.

The compare-header digest and original-call request digest both explicitly SET timezone='UTC'. Consequently actual nullable timestamptz `call_now_utc` hashes deterministically in UTC in both domains, rather than depending on the caller's session timezone. Other new typed row digests have no semantic timestamptz fields. Original comparison date/time fields come through the unchanged0030 immutable evidence owner. No current wall-clock read is substituted for the real call operand.

## Range census and noncircular raw proof

0121 lines6-41 define range `(p_begin,p_end]`, with nonnull actual producer, nonnull integer endpoints,0<=begin<=end<=1000. Actual producer SELECT INTO STRICT fails a missing identity. The count uses event-owner plus actual event before the query tuple inner join; it must equal end-begin. The actual2244 UNIQUE(preparing_transaction_id,transaction_ordinal),2352 event-owner primary key and checked same-xid/epoch mean this exact count cannot hide ordinal gaps by duplicating another ordinal. A missing or foreign-owned actual event inside the interval reduces count; a duplicate-ordinal substitute is not legal.

Before final aggregation, LEFT JOIN census rejects any owner row with missing event, and in-range rows with missing canonical query tuple, wrong PAY_QUERY kind, producer xid/epoch mismatch, nonnull original command, owner relation/row mismatch, old/new tuple SHA mismatch or false-presence nonempty tuple. The final inner join cannot silently discard those invalid rows. Its manifest hashes actual event identity/xid/epoch/ordinal, owner identity, presence/digests and Candidate/root/old/new canonical tuples in actual ordinal order, with producer and endpoints bound. Empty begin=end produces count0, NULL first/last and a domain-bound empty JSON manifest.

This range proof intentionally covers only the caller's declared child interval, not every other legitimate child stage under the same producer. Whole-root actual bounds, every child interval, no overlap, no omitted stages and the classification of APP response stages still require the planned owner save/assert integration.0121 does not discover the true entry/tail endpoints or confer an owner scope.

The raw proof never includes completion witness SHA, completion header SHA or receipt finalization. Child row digests bind raw-stage SHA and child aggregates; parent Materialise/root bind completed child SHA afterward. This is an acyclic dependency direction. Existing final protocol event digest can subsequently include a completion witness without feeding it back into these raw ranges.

## Typed child sets and complete immutable witnesses

Child set helper0121 lines190-204 hashes count plus native UUID-ordered `(workUUID,inputDigest)` and `(incidentUUID,omissionDigest)` manifests, including all branch fields via the typed row functions. Empty actual arrays give explicit domain empty-set digests. This is the supplied-composite form of0043's retained-table set helper and analogous to0059's supplied proof. It performs no live comparison/omission query and retains no raw issues. It is a digest primitive: later save/assert must validate nonnull finite1D arrays, uniqueness, exact original expected sets, branch shapes and digest equality before accepting supplied composites. Duplicate-key invalid arrays are not a supported accepted certificate.

Witness helper0121 lines220-235 is finite UNION DISTINCT of all actual current comparisons, CHANGED prior comparisons, omission comparisons, NEW/CHANGED SOURCE_RECHECKED events, UNCHANGED_RESOLVED genuine original resolution events and omission SOURCE_MATCHED events. Shared physical evidence is represented once in the set, never truncated. Deterministic order is fixed kind rank COMPARISON/RECHECKED/RESOLVED_DECISION/MATCHED followed by native immutable UUID; returned witness ordinal is row_number over that same order. Immutable0111 branch constraints make the selected IDs nonnull for valid supplied rows. Full certificate count/witnessSHA and later identity/scope/evidence assertions remain mandatory.

Candidate original-evidence helper first calls exact0030 evidence digest, preserving genuine comparison/event IDs, publication/episode/fingerprint/actor/idempotency/finite business payload semantics and existing event-kind checks. For SOURCE_RECHECKED and SOURCE_MATCHED only, it additionally reads the actual immutable event's origin and requires exact `CANDIDATE_TIMESHEET_SUBMISSION`, then hashes a separate suffix domain over the original evidence SHA plus actual origin. The actual original2203 writer supplies this field at lines1841 and1888.0030 omits origin; this suffix explicitly closes that field rather than pretending the base digest already includes it. Missing or different origin refuses. COMPARISON and genuine Office/manager/Candidate historical RESOLVED_DECISION retain the unchanged base digest; no Candidate origin is incorrectly required of those original decision parents.

This helper does not prove latest incident or today's lifecycle, and does not manufacture missing ON CONFLICT rows. Actual owner code must select real returned/conflicted event identity and later assertions must enforce call publication/current comparison/episode, old resolved-decision evidence and actual expected omission selection. Hashing finite immutable originals is not a replacement for that ownership proof.

## Source declaration, ACL, registry and completion boundary

All ten functions are private SECURITY INVOKER with explicit pg_catalog/private paths; the original-evidence helper additionally needs public for real immutable comparison/event row types. Pure row/set/request functions are SQL IMMUTABLE; actual retained-row evidence/range/witness functions are STABLE, correctly reflecting reads of actual stored rows. Every declared signature has exact ALTER owner postgres and REVOKE ALL from PUBLIC,anon,authenticated,service_role. No public route, GRANT, trigger, producer/clock/business mutation, raw request table, dynamic SQL or generic JSON replacement is added.

All ten exact signatures, including timestamptz's canonical `timestamp with time zone` request signature and `(uuid,integer,integer)` range, have one new0121 finalRoot/finalPhysical/finalClosure with no predecessors, and exactly0121 allowedPhysical. Existing0030/0043/0059 final definitions are not replaced.

PASS remains **Candidate typed digest/witness foundation only**. INSERT guards, evidence/stage bijections, actual child call endpoints and original result binding, true MAT/SELF/APP owner seams, APP response union closure, historical assertions and actual final clock remain separate forthcoming work. PREFINAL still needs the real SourceUpload parent and is not made supported by this helper. No whole-owner or whole-F25 claim follows from this review.

## Saved raw source checkpoint

Raw saved-byte SHA256, not normalized text hashes:

| Path relative to BUILD-BE | Bytes | Raw SHA-256 |
| --- | ---: | --- |
| `supabase/repeatable/10102026_0121_banking_pay_next_candidate_completion_digests_v1.sql` | 15740 | `ed7afd09a2ef94717dc403e4686d764c35d152fc9f83c540bd7a5eda5177d5b7` |
| `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` | 19298 | `2f6c039ad7ac175e0d6aa486baa888dbe9d18f0292b80d577ac4a4925ad5df8a` |
| `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql` | 6901 | `4be599c83fe18bb3b8761fecebb3e0a9785d2d69a716b83225787f8cc431dd54` |
| `supabase/repeatable/10102026_0043_banking_pay_next_query_sync_completion_digests_v1.sql` | 5347 | `79e3787728d2550a423f9552cd3b4e3b670c292e4715607b0a792ee9ff9f77eb` |
| `supabase/repeatable/10102026_0059_banking_pay_next_query_sync_true_completion_v1.sql` | 11615 | `08d6e2310e383644a15beb78fd890c378a02957e0cbefa13faf2b1190fdb2136` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | 149249 | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/migrations/09102026_2244_banking_pay_next_input_transaction_v1.sql` | 1968 | `4fe014fe836d3474c18d77670788814754384a6fab7b889b557a6af55e0e2191` |
| `supabase/migrations/09102026_2352_banking_pay_next_query_producer_protocol_v1.sql` | 19936 | `d6dfda1236deeddf6f7f6f1f720ff95ff4521d605dd93260c574307cf1842be1` |
| `docs/bpay-next/design/F-25-candidate-protocol-schema-build.md` | 23517 | `2b6279e1a8d057fbd175d27e68e0967d866ecf46c95144a38cc4d195ef011473` |
| `docs/bpay-next/final-definer-closures.json` | 757900 | `ccbd0be1183aa12bc2feb57809ff316078faaa47aa93a5cc07c7034269de7d00` |
