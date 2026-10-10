# F-25 QuerySync certificate evidence source review

10 October 2026. **PASS for the two0051 helper definitions and final-source registry entries. One inherited witness-envelope dependency finding remains for the pending full writer.** Read-only source review; no SQL, database, runtime, tests, fixtures or hosted operations executed. No product edits. This does not certify current-xid insertion guards, the full QuerySync writer, final completion assertion or installation.

Read the entire89-line `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql`, current0036 branch schema,0020 root schema,0043 child/header digests,0039 historical original-decision qualification,0030 original-evidence digest, original1534 Sync comparison/recheck/omission branches, and both exact final registry entries. Stage closure0047 was independently reviewed elsewhere; this note does not repeat or claim its execution.

## Dependency finding: complete witness set can exceed inherited3000 cap

The existing0020 root header caps `original_witness_count` at3000 and each retained witness caps `witness_ordinal` at3000. A valid typed shape within the existing other bounds can require4000 distinct witnesses:1000 `UNCHANGED_RESOLVED` inputs each retain comparison plus actual historical decision, and1000 separate omitted work incidents each retain comparison plus SOURCE_MATCHED event. Input count1000, omitted count1000, new/changed0, unchanged1000, resolved1000 and stage count1000 otherwise satisfy those bounds. The historical comparisons and omission comparisons belong to different incidents/work, so UNION DISTINCT does not reduce this case.

Resolve the finite envelope before full-owner closure, preferably allow4000 retained witness rows/count/ordinals consistently with the exact complete manifests. If a narrower intentional admission limit is chosen, make it explicit and truthful before business work; do not truncate, omit a historical decision, or conflate different witnesses to fit3000. This is an inherited schema/next-owner dependency, not an incorrect0051 manifest implementation.

## Certificate assertion

`private.bpay_next_query_sync_certificate_evidence_assert_v1(uuid)` selects exactly a QUERY_SYNC/QUERY_SYNC root. It reconstructs native-UUID sorted work IDs with their aligned complete issue SHAs, sorted omitted incident IDs, and all original new/changed/unchanged counts. Those sets/counters, child cardinalities and recomputed child digests must equal the retained header; the complete typed root request SHA is recomputed with0038 from actual actor/cycle/publication and aligned input certificate SHAs. Omitted work cannot overlap supplied work.0036 primary/unique keys prevent duplicate per-root work/incident identities and branch checks constrain nullable prior/recheck/resolution/matched-facts fields.

Each input/omission row SHA is independently recomputed from its full typed certificate using0043. Comparisons must actually belong to the retained incident. NEW requires revision1; CHANGED requires the real retained prior comparison of that incident, distinct fingerprint and a lower revision. The actual prior-pointer/stage relationship belongs to the independently reviewed stage closure and genuine writer, rather than a guessed current comparison.

NEW/CHANGED recheck events bind exact incident, captured episode, original Sync publication, comparison fingerprint, SOURCE_RECHECKED/SYSTEM/null user, original unique key, and exact comparison UUID/family/presence payload. The original schema has a unique material comparison fingerprint, so a newly inserted NEW/CHANGED comparison cannot quietly reuse an earlier comparison with that fingerprint. Original ON CONFLICT event discovery must still retrieve the real event and satisfy these immutable relationships.

UNCHANGED_RESOLVED recomputes only the nine original narrow historical terms: contract, family, presence, candidate start/end/break and system start/end/break. The actual retained resolved event must have the captured episode, then0039 qualifies its real immutable decision.0039 binds comparison incident/fingerprint, genuine Office accepted/resolved pair or original manager/Candidate response key/event, exact response choice/kind/request-set metadata, and matching pair incident/episode/publication/fingerprint/actor. It admits only OFFICE_ACCEPTED_SYSTEM_HOURS, MANAGER_CONFIRMED_SYSTEM_HOURS or CANDIDATE_CORRECTED; SOURCE_MATCHED cannot masquerade as the historical business-facts decision.

Omission events bind their actual comparison's incident, captured episode, original current Sync publication, null comparison fingerprint, RESOLVED/SYSTEM/null user, exact `SOURCE_MATCHED:<incident>:<publication>` key and `resolution=SOURCE_MATCHED` payload. This mirrors the original1534 omission event insert. It does not re-run today's OPEN omission selector or synthesize an event.

The helper never reads the mutable incident lifecycle/current comparison pointer, current work/timesheet/policy or live source mapping. Comparisons may legitimately retain earlier publication IDs: there is no false comparison-publication equality with today's header. Historical pair publication remains its original publication; only newly generated Sync recheck/omission event publication is bound to the real Sync header.

## Complete expected witness manifest

`private.bpay_next_query_sync_expected_witnesses_v1(uuid)` builds precisely the finite UNION DISTINCT of all input comparisons, CHANGED prior comparisons, omission comparisons, NEW/CHANGED recheck events, historical resolved decisions and actual SOURCE_MATCHED events. It excludes fabricated or unrelated publication-wide evidence. Equal `(kind,actualID)` witnesses are legitimately retained once; distinct original identities remain distinct.

Explicit kind rank followed by native UUID provides deterministic contiguous `row_number()` ordinals. Each SHA comes from0030's typed actual immutable comparison/event reader, including finite event-kind checks and strict row lookup. Empty certificates yield an empty witness result, while0043/0038 use explicit empty manifests. Callers must aggregate/order using the returned ordinal; incidental SQL row-delivery order is not an ordering contract.

Both0051 functions are STABLE SECURITY INVOKER with explicit paths, logical repository owner postgres, and execution revoked from public/anon/authenticated/service_role. The exact two signatures each select0051 as final root/physical/allowed/final closure with no predecessor. The assertions qualify retained evidence only; the pending true-tail writer must bind actual original branch identities/input terms/omission selection before inserting certificates, and current-xid guards/finalizer must establish producer ownership, complete stages and exact retained witness/header SHA. No trusted SHA alone reconstructs the unhashed original issue or proves its relationship to matched facts.

## Saved raw-byte provenance

| Source | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_0051_banking_pay_next_query_sync_certificate_evidence_v1.sql` | `bd9920f0440406a7710f713391db25646ebc475fa906d7faac6fad03aa133768` |
| `supabase/migrations/10102026_0036_banking_pay_next_query_sync_branch_evidence_v1.sql` | `9bf794bcb6ac1964922403bbdf4eab91a088a6248af5188b1c7b50ede31477fb` |
| `supabase/repeatable/10102026_0039_banking_pay_next_query_sync_original_decisions_v1.sql` | `f5847e3e3fb28a8745d3ee8c29d0dd7dc09740d7c23627b380f358f762c985af` |
| `supabase/repeatable/10102026_0043_banking_pay_next_query_sync_completion_digests_v1.sql` | `79e3787728d2550a423f9552cd3b4e3b670c292e4715607b0a792ee9ff9f77eb` |
| `supabase/repeatable/10102026_0030_banking_pay_next_query_completion_evidence_v1.sql` | `4be599c83fe18bb3b8761fecebb3e0a9785d2d69a716b83225787f8cc431dd54` |
| `supabase/migrations/10102026_0020_banking_pay_next_query_root_completion_v1.sql` | `bf36da5429faebb2d02995ab24f1dc8538e83e8587a7273ab8acb62aaa6c60bf` |
| `docs/bpay-next/final-definer-closures.json` | `08030368970e7e14990f727af767375160a266f89e8da965bdb8c86a85f3701b` |

Hashes describe the shared saved-source checkpoint, not installed bodies or successful compilation.
