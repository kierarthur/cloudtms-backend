# First-install candidate after SEND9 — HELD, NOT INSTALLED

13 October2026. Owner-approved fixture-only item6 correction.

| Repository | Exact candidate code SHA | Published branch |
| --- | --- | --- |
| Backend | `2c61f97b38c31b4751177f1d1f62b815679b1193` | `codex/bpay-next-item6-final-20261013` |
| Office | `413cea9b35975c6ccac0b084327ef109b05c1c48` | `codex/bpay-next-build-20261009` |

Authoritative candidate worktree: C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/bpay-next-item6-final-20261013. This entry supersedes backend **e67d948f0c93cf15789445e243d3e2202efb316b — superseded, not installed**. Both frozen e67d948f and Office413cea9b remain unchanged. Later DR22/DR24 build work is excluded; ongoing build uses a separate branch/worktree. This recording commit is documentation only, separate from the exact code SHA above.

Code diff against e67d948f (only the named verifier):

```text
git diff --stat e67d948f..2c61f97b38c31b4751177f1d1f62b815679b1193
.../verification/17092026_0100_weekly_source_entitlement_schema_v1.sql   | 1 +
 1 file changed, 1 insertion(+)
```

Inside verify_entitlement_heads, the two named inventory constraints are explicitly DEFERRED before the positive HEAD_INSERT. Declared2, genuine SHIFT/EXPENSE, certified_zero=false, native receipt/activation/three-guard IMMEDIATE flush/restoration, every negative and FK remain byte-identical. No production guard, trigger deferrability, table, function or Worker/Office source changed.

Contract **1f5b6396215c1ccaec01e69c64beb5663ac905a37329f0d7278adca55dc62a26** remains unchanged; verifier-only code does not enter the contract. db:check389migrations/1018repeatables, coupling and source/legacy/final-definer32/32PASS. Independent re-rehearsal2 of e67 proved all94migrations/219closures,153/154eligibleverifiers, complete contract match and18fingerprints; the six root/cascade statements are now proved below. This targeted result is not a claim of a second complete154-verifier run.

Targeted SEND9 proof: same approved SEND6 agency TEST route/profile, shared separate session lock(20261013,1), exact candidate pending94migrations/219closures then the entire17092026_0100 verifier in one BEGIN/ROLLBACK. The two named inventory guards were forced IMMEDIATE before the verifier. S19–S24 all pass with their internal assertions; S19 diagnostic did not fire. All4401mappedstatements returned once, zeroerrors, explicitrollback, freshconnection18fingerprint equality and markerabsence. Explicit shared unlock returnedtrue.

```json
{
  "database": "cloudtms_test_clone",
  "entryNamedImmediate": {
    "id": "S004366",
    "sqlState": "00000",
    "error": false
  },
  "S19toS24": [
    {
      "ordinal": 19,
      "id": "S004391",
      "sourceSha256": "b684c80112d05f16a07e247472588ab54ff6a6cfd9e0c42b1a624abb8bb04015",
      "sqlState": "00000",
      "error": false
    },
    {
      "ordinal": 20,
      "id": "S004392",
      "sourceSha256": "69350cd5213ce6a3594e623221119d71d2aa580001672ccfc46774f85983ae0a",
      "sqlState": "00000",
      "error": false
    },
    {
      "ordinal": 21,
      "id": "S004393",
      "sourceSha256": "2d5e337a870ed9d0b8dcf2876bdd88796607cc87abdda1d3ff457e7bdb71203f",
      "sqlState": "00000",
      "error": false
    },
    {
      "ordinal": 22,
      "id": "S004394",
      "sourceSha256": "5d47c47f903086bf364e3820f772bbcb7eeea0274894cca9f0331c6a19568128",
      "sqlState": "00000",
      "error": false
    },
    {
      "ordinal": 23,
      "id": "S004395",
      "sourceSha256": "7535aeb80ff408eb1dec0d9cb22249450fb730658536b2131ee65345f945bde2",
      "sqlState": "00000",
      "error": false
    },
    {
      "ordinal": 24,
      "id": "S004396",
      "sourceSha256": "d84cc6df51455509ae117702b209273751c2e91bc7059926ab14fe0fded535f4",
      "sqlState": "00000",
      "error": false
    }
  ],
  "mapped": 4401,
  "returned": 4401,
  "uniqueExact": true,
  "primaryErrors": 0,
  "diagnosticFired": false,
  "explicitRollback": true,
  "connectedFresh": true,
  "markerAbsent": true,
  "fingerprintEquality": {
    "environment": true,
    "marker_absent": true,
    "physical_owner": true,
    "migration_count": true,
    "repeatable_count": true,
    "customer_key_null": true,
    "index_fingerprint": true,
    "physical_database": true,
    "column_fingerprint": true,
    "policy_fingerprint": true,
    "routine_fingerprint": true,
    "trigger_fingerprint": true,
    "module_control_state": true,
    "relation_fingerprint": true,
    "migration_fingerprint": true,
    "constraint_fingerprint": true,
    "repeatable_fingerprint": true,
    "default_acl_fingerprint": true
  },
  "turn": {
    "lock": [
      20261013,
      1
    ],
    "database": "cloudtms_test_clone",
    "acquired": true,
    "released": true,
    "atUtc": "2026-10-13T21:22:05.417Z"
  },
  "started": "2026-10-13T21:15:44.294Z",
  "finished": "2026-10-13T21:22:05.367Z",
  "packetSha256": "a09ce416deff375e2328d4fa15d008a9ae4668ce3d3a9d2ffb965598f20d6458",
  "fullReceiptSha256": "3e19cc229a0b7538b5a0002e69445200073d0e182ff11a9b390fa2f7c358383a"
}
```

Full local proof/statement map: the ongoing build's docs/bpay-next/design/SEND9-targeted-rollback-receipt.json and isolated candidate .codex-local/SEND9-targeted-map.json. Review: ongoing build docs/bpay-next/design/SEND9-item6-review.md. The report/index at cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/BANKING_FULL_REHEARSAL_2_13102026 are preserved. No durable TEST/ledger/release write, NEXTswitch, provider/bank/payment/email/drain or LIVE action. Sequence gaps remain possible under rollback.

**Do not promote or install until owner go.** After that: protected disabled-first release:test with LEGACY active, database before ordered Workers and Office, safe smoke/TEST_INSTALLS; then repair all33 stale Office fixtures from the rehearsal report without weakening assertions, require the suite green before F17 NEXTswitch. Continue full build independently now.

---

# Historical candidate entries — superseded, not installed

# Corrected first-install candidate — awaiting independent full rehearsal

Recorded 13 October 2026 after publication and fresh GitHub verification. Only the existing build branches were pushed. **Promotion, installation and activation remain held.**

| Repository | Exact candidate code commit | Published branch |
| --- | --- | --- |
| Backend | `e67d948f0c93cf15789445e243d3e2202efb316b` | `codex/bpay-next-build-20261009` |
| Office | `413cea9b35975c6ccac0b084327ef109b05c1c48` | `codex/bpay-next-build-20261009` |

The Office SHA includes every retained change since `980db9d1f0bb8cf119758ca3ef9030b0efbe1d27`, including stored-credit adoption and DR-23's PAYE recovery question. Backend starts from `3abaed3e4f74644bf229c2b58d7e0805ea7284fb`. Remote backend `test` at `e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92` and Office `main` at `887e7af7cd15d9f4034ad73806b44344f3eb0791` were rechecked and already integrated. No work was reset, forced or discarded.

Contract: `1f5b6396215c1ccaec01e69c64beb5663ac905a37329f0d7278adca55dc62a26`. The fresh, small PostgreSQL 17.11 schema-only NEW seal compiled 207 migrations and 791 closures; complete managed inventory is 389 migrations / 1018 repeatables. Regeneration produced exactly the existing committed contract bytes. The candidate tree contains that checked contract unchanged. See [clean seal](design/SEND6-clean-PG17-seal.json) and [canonical LF pin read-back](design/SEND6-PG17-pin-readback.json). The dedicated clean-seal container and volume were removed immediately; the unfinished build's PG17 infrastructure remains preserved.

| SEND6 item | Status | Evidence |
| --- | --- | --- |
| 1: two manual-upsert pins | FIXED — EXECUTED | Canonical committed-LF PG17 function-definition SHA `658e71e3…d0d7`. Both verifier sections pass in round 1, including the original ACL/family/runtime assertions. Every other pin is retained. |
| 2: Authorise pin | FIXED — EXECUTED | Canonical final 2223 body MD5 `52e97f5b…42e0`, definition SHA `08eacfea…ae6`. Withdrawal route/protection assertions pass in round 2. Unauthorise/invalidate pins remain unchanged. |
| 3: six legitimate actors | FIXED — EXECUTED | Six main fixture INSERTs explicitly grant payment-authoriser and exclude golden-key authority. Wrong/inactive reviewers remain unprivileged. Absence/audit/ordinary-guard assertions pass in round 1; publication/withdrawal pass in round 2; first-authorisation passes in round 4. No real agency actor was changed. |
| 4: D10 tamper case | FIXED — EXECUTED | A fresh real-I5 pending decision and successful exact receipt/committed-state/head control are rolled back before corruption. Real integrity refusal, permanent manual review, no-write and committed-revision protections pass in round 2. |
| 5: F5 singleton negative | FIXED — EXECUTED | Only the named receipt guard is deferred inside the original sentinel block. Both invoice-admission forms pass in rounds 1/3. Both invoice-issue forms pass in round 1, IDs S005574/S005886, statement SHA `b99825c2…6f6a`. Singleton, restored indexes and real receipt guards remain. |
| 6: original S19 inventory mismatch | **OPEN — instrumented, not repaired** | S19 succeeds in targeted rounds 1/2/4. Its original full-release-context cause remains unknown. On recurrence, instrumentation reports the finite firing phase, head, native declared/actual counts and certified-zero value, preserving SQLSTATE 55000. No zero was fabricated and no FK/guard was relaxed. |

The newly reachable assertions exposed further fixture issues, which were fixed forward and independently reviewed. D8 now has a real matching immutable receipt. The withdrawal fixture completes its actual inventory before flushing its two named native guards. Schema S21 retains the real receipt-table refusal and proves the exact real BEFORE TRUNCATE guard on an empty temporary copy, without CASCADE or removing an FK. Ordering now compares the withdrawal marks and audit to an independent caller-frame probe, correctly handling psql's required statement savepoint while retaining top-level transaction equality, all xmin bounds and managed-root observers. Executable and quoted-SQL checks retain forbidden-token detection and now use correct closing \M boundaries. **No production Source owner was changed.**

These are targeted reruns, **not** a clean full 154-verifier rehearsal or a complete installed-contract comparison. All selected 94 pending migrations / 219 canonical closures replay successfully. Historical failures are preserved: round 1 found 12; round 2 isolated two fixture issues; round 3, invoice-admission alone, passed 4,947/4,947 statements; final round 4 passed 4,455/4,455. Every completed rehearsal explicitly rolled back, matched all 18 fresh before/after fingerprints and left the marker absent. Sequence gaps remain possible. Statement maps and [independent final evidence review](reviews/SEND6-final-evidence-review.md) bind every receipt to its source. Three APPLYING-only verifiers remain for the genuine protected release.

Required db:check, contract coupling, 32 source/legacy-fence/final-definer checks, three existing withdrawal-pin checks and Office node syntax checks pass. An extra pre-existing Office unit-suite attempt remains explicitly **FAILED: 37 passed / 33 failed**. Independent investigation traces those failures to stale rule-B, journal, response, polling, cache and light-theme fixture assumptions already present in the old candidate; see [the investigation](reviews/SEND6-office-gate-investigation.md). No introduced material product defect was identified in those causes. That failed extra run is not presented as a gate pass or financial acceptance proof. Joined business/app/queue/timing acceptance remains **AUTHORED / NOT_RUN**.

| Card | Candidate state | Remaining work and disabled-install safety |
| --- | --- | --- |
| XD-01 | PARTIAL — INCLUDED | D02–D04 and the native list/sorting are present. XD-07's oversized typed-refusal row / whole D01 remain unfinished. Oversized responses fail closed, never truncate. NEXT admission is inactive under LEGACY; implemented source/catalogue/A4 gates pass. |
| XD-02 | COMPLETE | Native bind command, journal and Office Adopt pass their own source/catalogue/46-PNG A4 gates. Future joined in-app checks are authored, not run. |
| XD-03 | PARTIAL — INCLUDED | Existing native allocation/capture D01 is verified and retained. CM-02 comparison-owner D02 and XD-06 screen D03 remain unfinished. No alternate allocator or early screen was invented. Preparation is inactive under LEGACY; scoped gates pass. |
| DR-23 | PARTIAL — INCLUDED | Seven versioned-preview/audit/recovery leaves and the Office helper are present. DR-22's full host / D10 / whole ITEM and later EX-02/EX-13 joins remain. Private additive attachments are inert with unused NEXT tables under LEGACY; implemented source/catalogue/A4 gates pass. |
| DR-22 | EXCLUDED — no committed SQL/product | Three root-owned generated SQL files are preserved outside managed inventory with exact paths/hashes. Draft design notes remain local. Resume the earliest unfinished DR-22 item after this requested stop. |

The early HealthRoster group remains BLOCKED-ON-HEALTHROSTER pending the sole WP-PP25 published SQL/Worker actor-helper handoff. No producer stub was built. That deferral does not hold this disabled candidate. F-25 rule-B objects and parked files remain as directed.

The updated SEND6B future rehearsal/self-fix plan is recorded in BUILD_LOG. Round 1 preceded SEND7; rounds 2–4 held its session-level shared lock (20261013,1), with acquisition/release times logged. The invoice deadlock did not recur when rerun alone under that lock. No invoice production change was made.

**STOPPED after recording, as the user's explicit SEND6 request requires.** The separate review chat must fully re-rehearse these exact code SHAs and compare the complete contract. Owner go is required before promotion or installation. Then: disabled-first protected release:test, database first; normal backend → real private → synthetic private → public broker; Office last; safe smoke checks and TEST_INSTALLS; then F-17 together with F-12. No promotion, deployment-branch push, release, agency switch, LIVE, actual payment/provider/bank/email/drain action was performed. Timing remains moved to TEST after installation; no local 300-Timesheet data was created.

---

# Superseded first-install candidate — NOT INSTALLED

Historical entry: superseded by corrected backend candidate e67d948f0c93cf15789445e243d3e2202efb316b. Not installed. Recorded 13 October 2026 after non-forced publication and fresh GitHub branch verification.

| Repository | Exact candidate code commit | Published branch |
| --- | --- | --- |
| Backend (`kierarthur/cloudtms-backend`) | `a4e87c601bd567b14240af483283d602d21cc715` | `codex/bpay-next-build-20261009` |
| Office (`kierarthur/TEST-Frontend`) | `980db9d1f0bb8cf119758ca3ef9030b0efbe1d27` | `codex/bpay-next-build-20261009` |

Contains completed Banking NEXT through WB-34, the approved HealthRoster checklist overlay, current TEST/Office integration, the three reported gate fixes, the single combined Candidate finalizer, native runtime reload closure, exact release prerequisites/census classifications, and the newly generated clean PG17 contract.

Contract: `168bd9fccdb69e4dcc3bdea19d6ef852a8c771d7801891989169d55655f29959`. Integrity: 386 migrations / 1015 repeatables; all original migration hashes retained. Local schema/catalogue/owner/ACL/seal/coupling checks pass. All 32 source/legacy/final-definer checks also pass against the exact committed LF archive; 67 release-source checks and the repeated 26 managed-source checks pass. Office native combined-review source checks pass 31/31. Independent source-review receipts are in `docs/bpay-next/design/` and `reviews/`. Business, joined, timing and protected-release acceptance are **AUTHORED / NOT_RUN**.

These are the frozen candidate **code** SHAs for independent recheck. This subsequent recording commit is documentation only; later XD commits do not silently replace this candidate. Any reviewer correction will receive a new candidate code commit and an explicit replacement record here.

**Not promoted or installed.** Continue XD-01–XD-03 now. Only explicit owner recheck PASS permits promotion, protected disabled-first TEST installation and safe smoke checks, then F-17 and only necessary F-21 transition work. No direct database owner switch, LIVE, real payment/provider/bank/email/drain action. The sole-producer HealthRoster group remains BLOCKED-ON-HEALTHROSTER and does not hold this candidate.
