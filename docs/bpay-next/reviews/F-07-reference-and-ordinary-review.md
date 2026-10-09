# F-07 reference and ordinary successor source review

Independent completed-batch source review, 9 October 2026. **PASS after builder corrections; no remaining source finding in this bounded F-07 review.** No SQL/database, runtime, tests, fixture, rehearsal or hosted calls were performed. This review does not establish installation or app/financial acceptance.

## Reviewed scope

Re-read the exact F-07 card (`01_BUILDER_RULES_AND_FOUNDATION.md:593–617`), checklist F-07-D01–D04, latest-wins/final-case-page prerequisites and applicable Source certification addendum. Reviewed saved `docs/bpay-next/imported-20261007/MANIFEST.md`/`.json`, all110 chosen copies against their absolute E source files, official UMM2 `SOURCE_MATERIAL.json`/overlay, raw RESET0549/PBW0510 and the ordinary successor against current0360. Root alone changed product SQL in response to findings.

Reviewed ordinary file: `supabase/repeatable/09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql`.

Final reviewed raw ordinary SHA-256: `C6F7D426FA60D3C6CF5DB4D5C386AEB8111F974E455BD475A10DCFECF0E5851B`.

Manifest JSON SHA-256 at this read: `080DC96307CCFEB718EB0E90CE96615C04D89D6B0A7EE6805D1560D50292BF52`. Later metadata-only edits require their own recorded hash; chosen source byte identities remain separately pinned.

## Findings corrected during review

1. Both original Source refusal probes referenced nonexistent `bpay_next_work.source_head_id`; that column belongs to WORK revision. The initial design note had suggested the wrong relation. Builder replaced both with existing immutable `work_kind='SOURCE'`, whose identity is protected by1342 `BPAY_NEXT_WORK_IDENTITY_IMMUTABLE`. The conservative discriminator also covers genuine initial TSFIN without a common HEAD. The design note was corrected.
2. Resumed capture initially depended on `work_collection_origin_v1` returning a qualified row; stale Source collections could instead enter a generic ordinary review path. Resumed ALLOCATE also bypassed Source refusal. Builder added an exact selected-revision collection→WORK Source probe for every resumed state before stage writes (ordinary113–124), and moved the per-item immutable Source probe before the ordinary qualification reader (ordinary264–273). New-state probe still precedes week/allocation-state writes. Successful DONE and lost-reply replays retain their existing read-only behavior.

Re-read the corrected saved SQL after both fixes. Named SQLSTATE55000 `BPAY_NEXT_COMPARISON_NOT_INSTALLED` propagates; no catch converts it to successful financial qualification.

## Reference integrity and selection

- All110 chosen records have unique relative paths, existing source and copy, and matching raw SHA-256; no byte mismatch or missing copy. No chosen file is marked promoted.
- Independently hashed all450 recorded DDL declarations from saved source at their recorded offsets:343 functions plus107 schema statements, all matching. This is lexical source inspection, not native SQL grammar or catalogue validation.
- All53 ordered unit paths/body/closure hashes/input origins match official UMM2 material; all186 recorded function winners match its exact name/argument/root/hash records. Final-reference selections point to the last recorded declaration for the same exact source/signature.
- MANIFEST.md contains every198 selected reference object identity:186 functions, six tables, six indexes. Schema extensions/71 UMM2 schema statements are retained with ordered provenance. Every selected object carries `needs rebuild by Contract-move (CM-02 onward)`.
- Planner V4 is selected; stale J2 planner V3 is not silently preferred. UMM2 governs per-object winners. PBW0510/raw0549 identical file hash `9E2488857DC7AD0823B172E2C4EFBC9EEF6687C0300392ED812E7A99E88F027E` supplies the final future comparison-capacity reference, superseding the earlier UMM2 0157/0241 copies.
- 55 hash-only V4/V5/Pilot B reference records were independently checked against saved source with no mismatch. The 28 reservation-correction object records remain `NOT_SELECTED_UNREVIEWED_CM03_ONLY`; none are selected latest-reference objects. The three after bodies are retained as unreviewed references, not promoted because of a newer folder date.
- Folder current/superseded/held/unreviewed distinctions are explicit. JEX remains an unresolved card alias, recorded with the actual FX5 NOT CLEAR material rather than an invented approved routine. This is a documented source limitation, not financial sign-off. Fixture helpers/runners/data/tests and preimages/inverses are excluded from chosen forward source. No chosen path is a fixture/test/runner/driver path.

## Ordinary successor correctness

The0360→ordinary1958 diff changes applicability/refusal only:87 added/six replaced lines at final review. Existing ordinary PAYE restrictions, policy valuer, instruction amounts, allocation function, period/capacity equations, result/hold/effect flow and projection arithmetic are unchanged. No second calculator, VAT bridge, Umbrella money branch or comparison certificate was introduced.

The successor defines exactly the existing case-page signature, SECURITY INVOKER and `search_path=pg_catalog,private,public`, retaining owner/ACL scaffolding. It contains no comparison receipt/mapping rowtype, composite parameter/helper definition, current/carry/basis/expiry helper, selected quote snapshot, comparison binding/approval/book columns or captured-work VAT state. The entire raw0549 comparison-capacity function is outside live release paths. Existing ordinary allocation-result VAT columns remain because0360 already uses them.

The NOT_AFFECTED port matches raw0549's ordinary applicability semantics:

- Initial admission requires a nonempty exact SEALED selected count and every joined Case/component tuple `(OVERPAYMENT,OVERPAYMENT,RECOVERY,GROSS_DEDUCT)`. Missing joins cannot qualify.
- Only that set skips prior/unresolved weekly contribution guards and WEEK_CAPTURE. All other cases retain0360's missing-prior-fact refusal; no unknown prior fact becomes zero.
- Per-item capture rechecks exact tuple when NOT_AFFECTED. Allocation rechecks frozen `(OVERPAYMENT,RECOVERY,GROSS_DEDUCT,GROSS_RECOVERY)` provenance, selected-item link, run/worker/preparation/selection revision, SEALED selection and expected/captured/worker counts.
- Frozen allocation proof does not reconstruct monetary history or read current pricing. Existing state schema already admits NOT_AFFECTED; comparison schema was not added to make this compile.
- Existing NEXT owner/epoch/lease/cursor/lock/replay guards remain. The new behavior is dormant under LEGACY.

Source refusal now uses actual immutable WORK identity for new and resumed selected revisions, before ordinary qualification or WEEK_CAPTURE/INSTRUCTION_CAPTURE/ALLOCATE writes. It deliberately refuses Source-origin case collections until the later CM chain; ordinary/manual ordinary cases retain0360 behavior.

## Release boundary and remaining verification

Release inventory is still limited to `supabase/migrations` and `supabase/repeatable` through canonical `inventory()`/`sqlDateKey()`. Imported references live under docs. Saved source census found no released comparison table/function, quote-snapshot helper or source-slice helper. F04 final chain selects ordinary1958 rather than raw0549.

This is a source review only. Local PG17 compilation/source gates are root's separate evidence; no fixture/first-use behavior or in-app refusal proof was run here. Future app acceptance remains NOT_RUN under the owner start-sheet limits.
