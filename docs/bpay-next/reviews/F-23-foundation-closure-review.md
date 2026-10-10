# F-23 foundation case/verifier/workflow closure

10 October 2026. Independent saved-source follow-up at HEAD `3c91e5e6ce35b5833a5c218b77b741261eeebafa`. Fresh-read the complete changed statements and their current enclosing case/helper contracts in the three existing tests, the verifier diff and reader ACL evidence, and both complete modified workflows. No test/module, SQL/catalogue, fixture, runtime, CI, hosted or deployment action was executed. Root alone edited product. Parent's reported 47-case PASS remains parent-run evidence.

**Verdict: PASS for the three existing test adaptations, the exact two-reader verifier update, and the two workflow wiring changes at the hashes below. No open finding remains in this reviewed slice.** This closes F23-TEST-01 from `F-23-foundation-tests-review.md`; acceptance of the separately authored F-23 source-gate implementation belongs to its own review receipts, and whole F-23 is not marked complete here.

The existing shared-wake case now feeds uppercase `legacy.session_id` and `legacy.candidate_id` to the actual preserved parser and explicitly requires `ids.session` / `ids.candidate` lowercase output. Both IDs contain alphabetic hex digits, so the assertion exercises real case normalization. Unexpected-key and depth32/depth33 negatives remain. The case still requires one exact NEXT ENROLL with no delay option, and LEGACY/DISABLED no additional sends. No new case or fixture was added.

The other test hashes are unchanged since the preceding review. Retired debounce/inline drain/old scheduled continuation assumptions are replaced by the actual registered NEXT wake, owner-gated consumer and bounded recovery seams. Original SQL economic/coalescing authority, native lower-level worker/lane/claim/execution identities, actual lifecycle success/bulk suppression/correction-pair/context flow, parser refusal and awaited draft hooks remain. Relevant exact ACK/retry/uncertain-send/expiry behavior is retained in the existing NEXT background/fence/F-13 cases; these source assertions do not substitute for those behavior cases or justify runtime scenarios.

The browser verifier hash is also unchanged since the preceding PASS. Its only executable change is complete non-Candidate routine count824→826 and exact MD5`10128c0eb0ada910eea370f487d5f214`; service_missing remains76 and browser_executable remains0. Inventory predicates, earlier relation/sequence checks and exception semantics remain unchanged. Actual additions are the readiness and payment-history public jsonb readers, with fixed search paths and owner/service_role execution only. This source review does not independently measure a physical catalogue or claim protected installation evidence.

## Workflow wiring

- `database-release.yml` adds the exact normal source-gate and legacy-fence test invocation in `source-gate`, after dbcheck/coupling and before exact final-authority verification/outputting the reviewed commit. Full fetch, pinned toolchain, secret/dependency checks, explicit registry release-base validation, release-system/automatic-policy tests, and release job checkout of `needs.source-gate.outputs.commit` are preserved. No release-mode, verifier manifest, approval/target, SQL apply or credential configuration changes are made by this diff.
- `supabase-migrate.yml` adds that same exact invocation and finite push/PR paths for `scripts/bpay-next-*.mjs`, managed-release scripts, `tests/bpay-next-*.test.js`, final-definer registry, actual Worker sources and `wrangler.toml`. Gate-only/registry-only changes therefore trigger the existing source workflow. Existing branch boundary, full fetch, Candidate workflow dependency, security/dbcheck/coupling, retained-artifact checks and complete normal `npm test` remain. No new fixture/runtime job, deployment step or hosted authority is added by this diff.
- Both commands reference the actual saved gate/fence files rather than a new harness. Source-gate content still requires its separately completed source review; wiring alone is not a content PASS. Workflow execution and green CI on the eventual pushed build commit remain unobserved by this reviewer.

## Fresh SHA-256 receipts

| Source | SHA-256 |
| --- | --- |
| `tests/banking-pay-cross-route-refresh-coalescing.test.js` | `55d8d8ffa1526b4f7732f9c756390097b7cb0832fb8537d3fb89104e488176a5` |
| `tests/banking-pay-workbench-stage2-worker.test.js` | `c5c1b3bb010b8ad1a04c9d778d75a986874b95a086f5613353b7cde33e06414c` |
| `tests/timesheet-lifecycle-workbench-wake.test.js` | `3b5c3fd4d02af6b69dc794a54f034728d9de97da419ead4bc6d9c249a44a742a` |
| `supabase/verification/22082026_1302_general_browser_isolation_verification.sql` | `a8f25fde93465f3cc35be3c86f02689309529c1977ecb20c3570fe47bc0a3544` |
| `.github/workflows/database-release.yml` | `602168afac04f4ed08a826d1714d9060780742d95559408ac037eb0866b851a1` |
| `.github/workflows/supabase-migrate.yml` | `7f578a7e053243a1da495868287db8a290ce39a30cb5a1e64eddcbab01760a70` |

At the workflow read, the referenced gate module/test hashes were respectively `fda6210ec452d97592ce5c8e5342dd2c5794b661f95ee491e1e28efae51c489f` and `0bce01aeec7d953d2cd680d55f7fae9272de4bff0d350f2e221b2e527802a691`. These identify the wired saved objects; they do not extend this review's verdict to their separately reviewed content.

No checklist, source product, verification program, release or application acceptance was changed by this reviewer.
