# F-25 QuerySync typed input digest source review

10 October 2026. **PASS for the bounded three pure helpers and their final-source registry entries.** No blocking source defect found. This review read saved source only; no SQL, database, runtime, test, fixture or hosted operation was executed. It does not certify QuerySync's later owner, certificate insertion guard, finalizer or installation.

Reviewed all of `supabase/repeatable/10102026_0038_banking_pay_next_query_sync_input_digests_v1.sql`, the complete `10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` migration, original delivery source at `15092026_1534_weekly_source_query_delivery_v1.sql:4360–4374,4626–4689,4780–5004`, the saved completion design's input-digest contract, and all three corresponding final registry rows.

## Exact issue parsing and identity

`private.bpay_next_query_sync_issue_digest_v1(jsonb)` rejects null/non-object input and unknown keys. Its allowed keys are exactly the original 16: work event ID; candidate timesheet ID/revision/shift fingerprint; source row/link IDs; contract ID; issue family/presence; candidate start/end/break; system start/end/break; expected material comparison fingerprint. Every allowed field participates once in the fixed domain-qualified digest array; there is no raw JSON retention or omitted optional field.

Mandatory work ID is typed UUID and null is refused. Optional UUID/integer/timestamp values use the original `nullif(value,'')` casts. Family/presence use the original normal issue path's `upper(btrim(coalesce(value,'')))`. Both hash fields use the real existing `weekly_source_query_hex32_v1` and its original error names, then canonical hex encoding. That owner requires exactly 64 lowercase hex characters; this helper does not silently accept uppercase or padded hashes. Source row/link identity and expected fingerprint are included even though the original economic material-comparison fingerprint omits them: this is a separate complete request-identity digest, not a replacement comparison fingerprint.

`STABLE` is appropriate for this parser's text-to-timestamp conversion. It has no live-row query, lock or business mutation. It retains original timestamp-without-time-zone semantics rather than introducing UTC/timezone conversion.

The original historical resolved-decision branch at4882 precedes expected-fingerprint equality at4952–4959. The pure helper parses the optional expected hash for request identity; it does not perform that equality check. Later wiring must preserve the original branch and must not infer full issue equality from the narrower historical match. This review does not claim byte-identical malformed-input error order for a future preparse call.

## Narrow historical facts and full root reconstruction

`private.bpay_next_query_matched_facts_digest_v1(uuid,text,text,timestamp,timestamp,integer,timestamp,timestamp,integer)` binds exactly the original resolved-match subset: contract, family, presence, and the candidate/system start/end/break terms. It excludes upload/publication, current pointers, timesheet identity/revision/shift fingerprint, source row/link and expected fingerprint. Its scalar typed arguments preserve null values; the genuine caller must pass the actual original normalized terms or immutable comparison terms. The helper itself does not replace the original boolean match or authenticate that relationship.

`private.bpay_next_query_sync_request_digest_v1(uuid,uuid,uuid,uuid[],bytea[])` requires actor/cycle/publication, both arrays, equal cardinality, at most1000 inputs, no null IDs/digests, and exactly32 bytes per digest. Nonempty arrays must be one-dimensional with lower bound1. The work array must equal its native-UUID sorted distinct reconstruction, so duplicates and noncanonical ordering are refused. The paired SHA remains attached by the same validated index. The root digest binds actor/cycle/publication and the entire ordered `(workUUID,issueSHA)` manifest; manifest length itself binds cardinality. Empty input has the explicit `[]` manifest, permitting the genuine no-input/no-material-event root without inventing a business event.

Migration0036 retains each complete issue SHA and permits the separate matched-facts SHA only for `UNCHANGED_RESOLVED`; the other three outcomes require it null. Mandatory IDs and branch-specific event/prior IDs, primary/unique keys,32-byte checks, RLS/private ACL and immutable-update/delete/truncate triggers are present. These relationships permit later immutable reconstruction of the root request. The schema and pure hashes alone do not qualify original events, certify complete branch sets or establish the true outer tail; those remain the explicitly pending owner/assertion/guard successors.

## Authority and saved provenance

All three functions are `SECURITY INVOKER`, have explicit `pg_catalog,private` search paths, repository logical owner `postgres`, and revoke execution from public/anon/authenticated/service_role. They read only supplied values and the existing pure hash parser. No public RPC, grant or business-table write is added. Each exact signature has one final registry row selecting0038 as root, physical file, allowed physical file and final closure, with no predecessor.

Fresh SHA-256 of saved raw bytes at this review checkpoint:

| Source | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0038_banking_pay_next_query_sync_input_digests_v1.sql` | `fe3e53a9f76c30b52cdcb44c88151fa47d4b102a83bb5ce69905f36986cc8a65` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `3e737df47f08028a42e79bf4a6d7efbae85e704ae9af915a72fe0c7635769d71` |
| `supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql` | `dcfa67862e75cff20e06d0694910ea603d329f3f0dc20fd351fca07cf598332d` |
| `docs/bpay-next/final-definer-closures.json` | `0b523c2fe2e8872d742e5bf8aa933be159dc5470a9e27375bedea0f22d80967d` |

The registry hash is a checkpoint of the shared saved file; unrelated concurrent later rows do not alter the reviewed three0038 definitions. No SQL compile or behavior result is asserted.
