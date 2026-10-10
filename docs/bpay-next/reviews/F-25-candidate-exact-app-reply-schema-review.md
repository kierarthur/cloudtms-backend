# F-25 exact APP replay additive schema review

10 October 2026. **PASS for the bounded0128 additive schema and recorded BD018/replay semantics.** No blocking source defect found. Read the complete29-line migration against unchanged0111 and the actual0017 APP receipt branches. Source-only: no SQL/catalog, runtime, tests, fixtures or product edits. Root reports the existing catalog constraint names verified; this reviewer did not query a catalog. Pending actual owner save/assert/guard implementation is outside this schema verdict.

## Additive branch and NULL semantics

0128 replaces exactly the existing root variant enum CHECK and owner/variant branch CHECK. It adds APP_EXACT_REPLY while reproducing the three original branches:

- DIRECT_MATERIALISE still requires its same-producer Materialise reference and excludes APP fields.
- SELF_SUBMIT still requires its Materialise reference and zero comparison children/stages, with no APP fields.
- APP_RECEIPT_ATTACH still requires an actual APP receipt and submission kind. Fresh WHOLE_WEEK_REVISION requires its Materialise reference; RESPONSES_ONLY has none. Response key remains null exactly for zero response items and nonnull for positive items.

APP_EXACT_REPLY requires owner CANDIDATE_APP_SUBMIT, actual nonnull receipt ID, actual nonnull submission kind, no Materialise ID, no response request key, and zero response-item count, comparison-child count and actual-stage count. The inherited submission-kind CHECK restricts it to the two actual original kinds. No special case converts WHOLE_WEEK_REVISION into RESPONSES_ONLY to obtain an empty branch.

All discriminators/counters are already NOT NULL. All optional ID/key operands use explicit IS NULL/IS NOT NULL, so SQL CHECK's acceptance of UNKNOWN does not bypass this branch. The unchanged Materialise ID/SHA-pair CHECK additionally forces materialise_completion_sha256 NULL when ID is NULL. The unchanged zero-stage CHECK forces first_stage_ordinal and last_stage_ordinal both NULL. Existing32-byte set/result/completion SHA guards, producer triple FK, real receipt FK, private ACL/RLS and immutability are unchanged. The original0111 file remains byte-identical;0128 makes no business row/receipt or owner-body changes.

Schema count0 alone does not assert physical absence of another certificate row or equality of an empty-set SHA. The upcoming INSERT/save/root assertion must require no same-producer MAT/compare certificates, no actual response stages/receipts admitted by this transaction, and exact typed empty response/comparison/raw manifests. That is the planned cross-row closure, not a missing claim of this additive CHECK migration.

## Exact original replay parity

0017:979–982 performs the scoped actual FINAL_SUBMIT receipt probe before producer entry. It returns the original stored response with the original idempotent_replay=true change; this branch creates no producer or completion adjunct.

The locked race branch at0017:993–1004 repeats that same original receipt probe after original locked session/account/Candidate revalidation and key advisory lock. A true outer producer already exists, but no generation/current scope validation, Materialise child, CandidateResponse child or Query stage has run. The actual receipt is selected by original Candidate, Candidate generation request, FINAL_SUBMIT mutation kind, UUID idempotency key and original request hash. It returns before the generation FOR UPDATE/current ACTIVE/deadline checks at1007 onward.

The new APP_EXACT_REPLY adjunct therefore describes this transaction's zero work while retaining the genuine earlier receipt and actual original submission kind. It must not import the earlier receipt's children as newly completed same-producer work or require fresh-generation/MAT/current-business predicates. The original whole-week receipt remains unchanged; its existence does not mean this new transaction materialised it. Upcoming root save/assert must bind actual immutable receipt ID/key/hash/full original response SHA and hash the actual returned replay reply separately where that certificate field represents returned result. Original JSON replay behavior remains unchanged. Standalone finish then seals zero staged input without receiving a metadata command.

## BD018 and Materialise replay scope

BUILD_LOG BD018 accurately states the original pure/locked split and distinct empty APP adjunct. Its recorded fresh WHOLE_WEEK_REVISION rule remains intact in0128.

The following Materialise EXACT_LAST_REPLAY paragraph resolves the historical attribution caveat from the guard design: exact request key/hash and returned cached reply are replay authority; retained current Contract/week fields are explicitly context only. Only FRESH certificates assert actual fresh owner terms/comparison work. Neither a replay certificate nor its retained assertion may derive the former fresh financial origin from today's workflow scope, or add today's generation/Timesheet/publication/state requirements to the original scoped replay. The explicit variant remains zero-stage with no comparison certificate. This semantic choice is compatible with unchanged0111's mandatory context columns and original2203's cache return preceding generation/state validation; actual helpers must enforce this distinction before a whole-owner verdict.

## Saved raw-byte checkpoint

| Source | SHA-256 |
|---|---|
| `supabase/migrations/10102026_0128_banking_pay_next_candidate_exact_app_reply_v1.sql` | `c1e32e41a2cd8fefd095927f822c2394ef2cfcfe02c226cfb7d7b16670e9c53c` |
| Unchanged `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` | `2f6c039ad7ac175e0d6aa486baa888dbe9d18f0292b80d577ac4a4925ad5df8a` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |
| `docs/bpay-next/BUILD_LOG.md` at BD018 read | `4adc36d340f3dde6e32ad44792ba12e024566d6e444666f81951cb1a02465931` |

No installed schema, compilation, runtime or whole F25/Candidate/Source completion verdict is claimed.
