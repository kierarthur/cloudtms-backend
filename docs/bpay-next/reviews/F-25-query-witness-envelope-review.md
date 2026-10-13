# F-25 QuerySync complete witness envelope source review

10 October 2026. **PASS.** Migration0057 closes the inherited3000-witness dependency finding in `F-25-query-sync-certificate-evidence-review.md`. Read-only saved-source review; no SQL, catalog, database, runtime, tests, fixtures or hosted calls were executed. No product edits or installation/compilation claim.

Read the entire `supabase/migrations/10102026_0057_banking_pay_next_query_witness_envelope_v1.sql` against current0020/0036 schema,0051 complete witness generator, and0047 expected-stage formula. Searched current `supabase`, `scripts` and `broker` SQL/JavaScript source for literal3000/3_000 and completion/witness references; inspected all relevant completion results.

## Exact bounded successor

The transaction drops/replaces only the root header's `bpay_next_query_root_completion_original_witness_count_check` and the member table's `bpay_next_query_root_completion_witness_witness_ordinal_check`. Bounds become count0–4000 and ordinal1–4000. The named constraints match0020's automatically named checks; root reports that the actual local catalog names were separately verified. This reviewer did not query that catalog.

0020's input identity, omitted incident, outcome counter, actual stage and stage ordinal limits remain1000.0036's typed per-input/per-omission keys, branch checks, digest length checks and immutable/private restrictions remain unchanged. Exact System-hours `original_witness_count=2*input_identity_count` remains enforced, so its real maximum is still2000. No API, business terms, additional canonical mutation capacity, original evidence row, receipt or digest domain changes.

The whole migration is within BEGIN/COMMIT and uses exact named drops without silently ignoring a missing expected constraint. It follows0020 in the dated migration chain; a fresh NEW installation receives the original table then this explicit successor. Old migration0020 legitimately retains its historical3000 text.

## Full finite maximum

Let N=NEW inputs, C=CHANGED inputs, O=UNCHANGED_OPEN inputs, H=UNCHANGED_RESOLVED inputs, R=omissions. The existing input bound is `I=N+C+O+H<=1000`. Current0047 requires actual stage count `3*N+2*C+R<=1000`; R separately remains at most1000.

Before legitimate UNION DISTINCT reductions,0051's complete witness count is:

`W=2*N+3*C+O+2*H+2*R = 2*I+C-O+2*R`.

From the stage bound, `C+2*R<=2000-6*N-3*C<=2000`. Therefore `W<=4000`. The maximum is attainable within all retained bounds:1000 distinct historical-resolved inputs and1000 different omissions, zero NEW/CHANGED,1000 actual omission stages. Each input contributes its actual comparison/decision and each omission contributes its different comparison/SOURCE_MATCHED event, yielding4000 real retained witnesses. Distinct identities must remain distinct; the migration permits their complete proof rather than evidence truncation.

## Remaining runtime cap census

No current completion helper or caller has an additional literal3000 witness cap. Relevant witness consumers use bigint counts/ordinals, exact aggregate counts/SHAs or row-number ordinals.0051 generates the complete finite manifest;0030/0043 bind supplied/reconstructed counts and digests;0032's System-hours helper retains its own exact2*N invariant.0047's1000 stage bound remains intentional.

Other3000 literals found in current product source are time/claim/budget milliseconds, invoice source-number offsets, or Unicode U+3000 normalization; none limits QuerySync witness count. The two old0020 checks are precisely the constraints0057 supersedes. This source census is at the saved checkpoint; the pending full QuerySync writer, insertion guards and final assertion must continue to consume the whole manifest without introducing a new3000 truncation/cap.

## Saved raw-byte provenance

| Source | SHA-256 |
|---|---|
| `supabase/migrations/10102026_0057_banking_pay_next_query_witness_envelope_v1.sql` | `121eafa88611f7bd79c24ee5d2325f1077b6afa43d7e4ceec57a92323695b958` |
| `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql` | `bd9920f0440406a7710f713391db25646ebc475fa906d7faac6fad03aa133768` |
| `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql` | `bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb` |
| `supabase/repeatable/10102026_0047_banking_pay_next_query_sync_stage_closure_v1.sql` | `d8f0e2f689eeb22ba480f039318a01b90187dbec5fbe098190f12428155b059e` |

Verdict covers the source successor and finite envelope; full QuerySync owner closure remains a separate pending review.
