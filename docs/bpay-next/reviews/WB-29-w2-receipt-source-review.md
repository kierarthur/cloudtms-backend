# WB-29 W2 receipt migration source review

12 October 2026. Independent read-only review of `supabase/migrations/12102026_2217_banking_pay_next_resolution_command_receipts.sql` against the reviewed W2 design and current2150→2259→2311 receipt schema. **SOURCE PASS for this schema migration.** No product edits, SQL/catalogue/business invocation, tests, fixtures, hosted actions or commits were performed. Root's installation/check results are separate; this is not whole WB-29 acceptance.

## Findings and exact retention

No concrete source defect found. Lines15–24 discover CHECK names by each complete original audited `pg_get_expr` expression, require exactly one catalogue match and the explicitly observed name, then lines37–38 use static DDL. This fails closed on missing/duplicate/different expressions or names; it does not blindly guess a generated suffix or drop other checks. Lines25–29 separately require the current2311 null-safe PREPARE Work CHECK's exact deparsed definition before replacing it. The source expects PG17's observed representation; this reviewer has not executed or independently observed that catalogue.

The new NOT NULL action/default at34–35 preserves historical and unchanged retry inserts as RETRY_WORK_JOB. Request/result object checks at42–47 retain4096/8192 for retry and permit32768/65536 only for the two closed resolution actions. Existing non-null request/result/actor/Candidate/module fields, native Work FK, PK, five-field execution CHECK, private owner/RLS/policy and immutable UPDATE/DELETE/TRUNCATE guards are untouched.

The variant CHECK at50–64 is explicitly `IS TRUE`, so missing/null result action or single-Work identities cannot pass through SQL UNKNOWN. Retry still requires a non-null actual job; the replacement Work CHECK at48–49 preserves exactly `work_id IS NOT NULL OR (result->>'retry_kind') IS NOT DISTINCT FROM 'PREPARE'` within its retry arm. Resolution requires a null job and all five retry revision/identity fields null. Its guarded CASE evaluates array length only for actual arrays, requires1…100 request Works and matching result length, keeps the real Work FK/ID for one Work, and permits null Work only for a genuine array with more than one member. No dummy job, command or economic identity is introduced.

This DDL is deliberately a finite **receipt shape** boundary. The upcoming resolution owner still must reject duplicate/request/result membership mismatches, foreign Candidate/Work pairs and incompatible native facts, compare exact action/actor/Candidate/epoch/request on replay, and return actual complete publication/decision outputs. Matching array lengths alone is not proof of those business identities; the reviewed design assigns them to admission/immutable receipt generation rather than this CHECK.

Full-envelope limits remain an implementation condition:32768 is the stored args ceiling, while public0312 already bounds the complete trusted-actor RPC request to32768.65536 is the stored result ceiling, not the wrapped HTTP reply size. Resolution decoding must use its own bounded larger envelope and check final bytes; original retry decoding/4KiB request/8KiB result behavior remains unchanged. No result truncation is authorised.

## Current bytes

| File | SHA-256 |
| --- | --- |
| `12102026_2217_banking_pay_next_resolution_command_receipts.sql` | `d8284aedeafd2259f768a217bd05ff5ac666a9faea1bd4aacce87714e2e5a6cc` |
| `design/WB-29-w2-native-command-design.md` | `dd9e0a5bfcd187c4b487da24cb2354b3911ad4bff2cb8a18e15c41df5a207744` |
| Original2150 receipt migration | `fba527a2a935213329c321c7713c97394e7f75a86216772978ea497b3b6cae0e` |
|2259 retry support migration | `383dd587f7783662fa980871b85b597562544c12ab4a1e924325a025aca0d8f3` |
|2311 PREPARE null guard migration | `5db2d3bb2522a1352fb45e5f25dd6d0f85c651d45c0df6179bf581dd1ce48f61` |

Source scope is the existing receipt's conditional variant only. New W2 implementation and joined/runtime acceptance remain NOT_RUN by this reviewer.

## Final source-gate amendment

**SOURCE PASS retained.** Reviewed current `scripts/bpay-next-source-gates.mjs:1909–1916`. `receiptExpressionCensus` classifies only a non-dynamic statement with exactly one dollar-literal BODY, anchored complete `SELECT array_agg(c.conname...) INTO v_names` over this exact private receipt table and CHECK type. The literal has only the two audited object/byte expressions, with backreference requiring the same column in both terms and an additional exact pairing check (request_body4096 or result8192). It permits metadata expression comparison, not EXECUTE, arbitrary SQL text, other table/column/bound or dynamic-DDL exemption. The existing dynamic-SQL rejection condition still independently rejects dynamic execution. Migration2217's unique observed-name assertions/static CHECK replacement remain as previously reviewed.

Gate SHA-256: `949b52698658bf12261406fe67010736849fd966bd1c6804086e50df5b02c058`. Migration2217 remains `d8284aedeafd2259f768a217bd05ff5ac666a9faea1bd4aacce87714e2e5a6cc`. This amendment covers the two metadata-classification literals; the separate incoming/persisted quote routine registration visible in the same diff is not a receipt-schema behavior change and requires its own SQL source review.

Root reports sole local PG17 load2217/2218, exact owner/receipt metadata, private ACL/RLS/immutable guards, existing browser-isolation DOs and contract export/seal/coupling (`630366ec…`) plus normal source gates passed. These are attributed Root checks, not independent execution or joined business acceptance by this reviewer. No additional action was run.
