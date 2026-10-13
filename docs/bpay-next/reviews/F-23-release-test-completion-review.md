# F-23 release-source test completion review

10 October 2026. Source-only final follow-up at saved HEAD `3c91e5e6ce35b5833a5c218b77b741261eeebafa`. Reviewed the exact `tests/database-release-system.test.mjs` diff and its complete protected-release assertion case, and fresh-read/diff/hash comparison of the three foundation tests, both CI workflows and browser-isolation verifier against the prior closed review. No tests, modules, gates, SQL/catalogue checks, runtime, fixtures, CI, hosted calls or product writes by reviewer.

**PASS.** The existing manual-release source case changes only its `fetch-depth:2` expectation to `fetch-depth:0`, with one explanatory F04/F23 ancestor-history comment. This matches the already-retained complete-history checkout in the protected `source-gate` job. It strengthens the source contract needed for the fixed reviewed ancestor, rather than removing history/ancestor checks. All other assertions in that case and all other release-system cases remain unchanged in the diff.

Preserved protected assertions include dispatch-only/two-phase/environment protection, exact TEST and LIVE target/physical-database bindings, NEW/UPGRADE blank/managed checks, exact approval and canonical TEST repository/branch boundaries, logical postgres→CURRENT_USER mapping, LEGACY_UPGRADE, pinned dependency scanner and lockfile, secret/dependency checks, contract coupling, and the source-job restriction against the unrelated `security:verify` entry. This source review does not exercise a LIVE route or confer release authority.

The six previously reviewed foundation/CI/browser files still have **exactly the same raw hashes** as `F-23-foundation-closure-review.md`. Their closed PASS remains valid: actual NEXT wake/queue/recovery assertions and positive uppercase legacy-ID normalization with refusal/depth negatives; unchanged unrelated economic, lifecycle and native identity checks; exact source gates wired before protected source-commit output; finite source/registry/Worker/config trigger paths; and complete browser inventory with only the two actual service-only reader additions. The verifier still adds eight provenance comment lines and changes only count824→826 / exact hash, retaining service_missing76, browser_executable0 and all query predicates/assertion paths. No browser omission or grant relaxation is added.

## Fresh source receipts

| Source | SHA-256 |
| --- | --- |
| `tests/database-release-system.test.mjs` | `40d973ee8be48d5915774c8aa3a69b73ee70b57d81337ed184cda5dae541c828` |
| `tests/banking-pay-cross-route-refresh-coalescing.test.js` | `55d8d8ffa1526b4f7732f9c756390097b7cb0832fb8537d3fb89104e488176a5` |
| `tests/banking-pay-workbench-stage2-worker.test.js` | `c5c1b3bb010b8ad1a04c9d778d75a986874b95a086f5613353b7cde33e06414c` |
| `tests/timesheet-lifecycle-workbench-wake.test.js` | `3b5c3fd4d02af6b69dc794a54f034728d9de97da419ead4bc6d9c249a44a742a` |
| `supabase/verification/22082026_1302_general_browser_isolation_verification.sql` | `a8f25fde93465f3cc35be3c86f02689309529c1977ecb20c3570fe47bc0a3544` |
| `.github/workflows/database-release.yml` | `602168afac04f4ed08a826d1714d9060780742d95559408ac037eb0866b851a1` |
| `.github/workflows/supabase-migrate.yml` | `7f578a7e053243a1da495868287db8a290ce39a30cb5a1e64eddcbab01760a70` |

The wired F-23 gate module/test at this read have hashes `7cbdba5eccca9c83191f872f4fc60f9199e42287d8fbbb2524772c34fa10c55c` / `270978104f70e65bfa10750fa4362a152a08f3bc0519428688e7af80dfbb1098`. Their content acceptance belongs to the corresponding independent source-gate review, not this tiny assertion follow-up. The four0316 projection-only routines have their separate full-CREATE equality PASS receipt in `F-23-csv-projection-review.md`.

## Execution evidence boundary

Parent reports 41 release-system units PASS, complete normal `npm test` 1,853 cases PASS, security:verify PASS, and local browser isolation PASS. These are recorded as parent-run results; this reviewer did not execute or independently observe their command output. No new round, scenario or fixture is requested. Exact pushed-commit/CI, protected hosted PLAN/installation/VERIFIED release and application acceptance remain separate owner-controlled evidence; this review does not claim them or alter the checklist.

Final assigned-slice verdict: no remaining concrete finding in the existing test assertions, foundation CI wiring or two-reader browser-verifier change. No product behavior was altered for this assertion correction, no normal protected assertion was dropped, and no F-25/producer work was reopened.
