# F-25 Candidate query completion schema review

10 October 2026. **PASS for the bounded five-table additive schema.** No blocking source defect found. Read the complete236-line `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` against the saved schema design and actual current originals. This was source-only: no SQL, catalog/database, runtime, tests, fixtures or hosted calls; no product edits. The migration is reported uninstalled/unlocked. This review does not certify owner completion, INSERT guards, helper bodies, compilation or installation.

## Actual keys and insertion order

Producer links use actual `uuid,xid8,bigint` types and the existing2352 unique `(id,preparing_transaction_id,module_epoch)` key. Compare header additionally supplies the exact unique `(id,producer_id)` target used by its input/omission children and the later Materialise certificate. Root's singleton Materialise ID must equal root producer ID when present; both independently bind that producer's actual xid/epoch. Same-xid/current-stack semantics remain the explicitly pending INSERT guards, rather than being claimed from FK existence alone.

Business FK targets match existing source IDs: Candidate account/workflow/component, Candidate/client/Contract/Contract week, Timesheet `timesheet_id`, Source group/cycle/publication, outreach generation, actual submission membership, work event, incident, immutable comparison/event and actual APP receipt. Foundation2055 and Source schema1534 confirm UUID primary keys and the existing Timesheet reference;2352/0036 already use the same protocol/evidence targets. The actual APP receipt has UUID `id`, UUID idempotency key, bytea request hash and immutable response authority.

Opaque `request_scope_id` correctly has no guessed membership FK. `submission_membership_id` is a distinct actual membership FK. Child compare header is inserted before its typed input/omission rows; Materialise can then reference the existing compare header; root adjunct references the existing singleton Materialise and actual APP receipt. There is no reverse child-to-future-Materialise FK or new deferred RI constraint.

## Branches, bounds and null safety

Compare caller MATERIALISE requires exact ordinal1 and real generation/membership IDs; PREFINAL requires both null, matching original null-generation scope construction. Original publication/Timesheet/hash and actual Candidate/client/Contract/week/group/cycle/profile remain separate typed fields. All four input branches have explicit nullable prior/recheck/decision/matched-facts conditions, real evidence FKs,32-byte guards, and per-call work/incident uniqueness. Omission keys prevent duplicate actual incident/work/event identity for that child.

Compare counters require `new+changed+unchanged=generated` and `resolved=omitted`. MATCHED iff actual whole-week open count0; open count has no invented1000 ceiling. Input/omission arrays have exact cardinalities/no null elements and valid nonempty dimension/base. The actual ordinal interval is nonnegative and at most1000, stage count equals end minus begin and the genuine `3*new+2*changed+resolved` formula.4000 witness count permits the full historical-input plus omission envelope. Sorted-distinct set identity, actual generated/seen equality and event qualification belong to pending typed insertion/assertion helpers, not ordinary CHECK expressions.

Materialise comparison ID/SHA is an explicit null pair; present requires both nonnull and SHA32. Fresh SUBMIT_TIMESHEET requires actual compare ID and submission membership; CHECK_HOURS/SELF_SUBMIT and exact-last replay have no compare child. SELF request scope/generation/publication/membership are null. Exact-last replay has zero stages, and only SUBMIT_TIMESHEET can have positive Materialise Query stages. Result generation is positive without a false replay `expected+1` requirement. Genuine request/result/signature/submission SHA values are scalar retained terms, not copied workflow/request/financial JSON.

Root Materialise ID/SHA pair now explicitly requires nonnull ID and matching producer when present, closing the possible SQL CHECK-null acceptance path. Owner/variant branches explicitly require the relevant MAT/APP operands and exclude unrelated APP fields for direct MAT/SELF. SELF requires zero child comparisons/stages. APP WHOLE_WEEK_REVISION requires its real MAT certificate; RESPONSES_ONLY has none. APP response key is null exactly for zero response items and nonnull for positive items. APP's actual response count/digest remains separate from comparison children, allowing original CHECK_HOURS CandidateResponse work without attributing it to MAT/private compare.

Root zero stages require both ordinal endpoints null; nonzero stages explicitly require both nonnull, positive bounded ordered endpoints and sufficient interval size. Later exact raw-stage/call/witness census must prove no missing/extra/interleaved stage; this schema does not claim that proof. Caller-kind versus actual producer/root, request hashes, Candidate identity, child/header SHA and actual APP FINAL_SUBMIT receipt kind/key/hash/response relationships are likewise explicit pending cross-row owner/assertion checks.

## Private immutable scope

All five tables use logical owner postgres, enable RLS, revoke all public/anon/authenticated/service_role privileges, and attach BEFORE UPDATE/DELETE plus statement-level BEFORE TRUNCATE immutable guards. No public RPC, direct service write grant, altered existing owner enum or new clock is introduced. The final comment honestly leaves same-xid/top-owner insertion guards and full evidence assertions pending before owner successors; PREFINAL still requires its actual SourceUpload root. There is no retained raw submission, request, contact, credentials or financial snapshot.

## Saved raw-byte provenance

| Source | SHA-256 |
|---|---|
| `supabase/migrations/10102026_0111_banking_pay_next_candidate_query_completion_v1.sql` | `2f6c039ad7ac175e0d6aa486baa888dbe9d18f0292b80d577ac4a4925ad5df8a` |
| `docs/bpay-next/design/F-25-candidate-protocol-schema-build.md` | `2b6279e1a8d057fbd175d27e68e0967d866ecf46c95144a38cc4d195ef011473` |
| `supabase/repeatable/15092026_2203_weekly_source_candidate_app_contract_v1.sql` | `2f28f6b60ef929617e2a2e5075e0b63875e21d0eea096d9ab63bcc501e3840d6` |
| `supabase/repeatable/10102026_0017_banking_pay_next_query_response_owners_v1.sql` | `08c95f0067da9f7d9d9ef3277584102011f75d73d6761bd9d6a8a0fb2ed2e270` |

These are saved-source checkpoint hashes, not installed-body or runtime evidence. Whole F25/Source owner closure is not claimed.
