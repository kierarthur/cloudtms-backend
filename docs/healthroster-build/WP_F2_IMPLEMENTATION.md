# WP-F2 source implementation and handoff

13 October 2026. Own worktree `.codex-worktrees/healthroster-v2-f2-build-13102026`, branch `codex/healthroster-v2-f2-build-13102026`, predecessor `e286533e90c0658c252ba1bcc909b9af909be785`. The integrator owns installation, contract sealing, releases, central progress and all hosted proofs. This lane made no database call, Docker operation, app build, commit, push or publication.

## Complete source

- Additive `13102026_1210_weekly_source_final_value_snapshot_authority.sql` adds nullable `authority_subset_json`, `authority_subset_sha256` and `invoice_economics_sha256`. Existing rows are unchanged. The exact four-field JSON/paired hash shape is constrained. Existing immutable record/truncate guards protect the added facts. The existing generator appended its hash `0d0564eea33cff211efe8452d3c7d4cf0817aeb21854a967a4f7ad402966a7d6` to this lane's migration lock; merge the new entry into the integrator's current lock, never replace other lanes' entries.
- Complete engine successor `13102026_1210_weekly_source_finalisation_unchanged_values_v1.sql` includes the complete helpers page. The deterministic source builder pins the reviewed predecessor's full LF SHA-256 `19018ea7be5e06a510fdd748409bcd84a18d0e31724daa19afb0a4471349a95b` and verifies every replacement anchor. `--check` proves the saved complete successor. A changed predecessor requires fresh review before generation.
- `weekly_source_final_state_fingerprint_v1(jsonb)` projects actual stored identity/date/time/break/net-minute, pay/charge buckets, VAT rate/chargeability and reduced authority. It excludes top-level metadata hashes and the calculation hash nested inside both vectors. VAT is read from the immutable economic row referenced by the snapshot's exact unique `row_resolution_id`, never current Contract/settings. No rule-set/client predicate limits the shared value comparison. Existing NHSP physical report branch, price/charge acceptance, all original reversal/replacement writes, full snapshot provenance hash, native NEXT locks/publication, CURRENT CAS, manual-review and receipt tail stay byte-identical to the current engine.
- Every new snapshot freezes the four authority fields and their independent domain SHA at INSERT. New-to-new policy staleness compares only that subset. Old CANCEL with an unknown subset retains the existing current-whole-policy versus stored-whole-policy comparison. An old/current comparison without derivable authority additionally requires exact whole-hash equality before it may return NO_CHANGE; timestamp-changed unknown history is held, not claimed to pass or turned into a false AMEND.
- `weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric)` hashes only explicit stored net/gross/VAT pence, line VAT method and VAT rate. Missing money refuses. Engine storage remains NULL until the exact F5 released-money producer and invoice-only correction/unwind owner are frozen and installed. No money is fabricated, no pay comparison reads source invoice money, and no correction movement is enabled by this preparatory helper.

All five new private helpers are owner `postgres`, direct `PUBLIC`/`anon`/`authenticated`/`service_role` denied, with controlled search paths. The existing fingerprint and engine signatures/security/owners remain intact. New signatures for the integrator's final private-helper inventory are:

```text
private.weekly_source_authority_subset_v1(jsonb)
private.weekly_source_final_value_vector_v1(jsonb)
private.weekly_source_invoice_economics_fingerprint_v1(jsonb,text,numeric)
private.weekly_source_snapshot_authority_subset_v1(uuid)
private.weekly_source_snapshot_value_fingerprint_v1(uuid,jsonb)
```

No new relation or public RPC was added. Root/F1 must union the helper ACL inventory and release verifier manifest after all complete function bodies, then seal the exact combined PostgreSQL contract. This source-only lane did not generate or guess a PostgreSQL canonical definition hash.

## Approved controlled historical origin

Independent `F2_HISTORICAL_SOURCE_REVIEW.json` records all 109 curated release attempts, including FAILED attempts, and five reviewed complete engines. The exact original writer and three authority guards are common to all five. Each newly inserted snapshot reaches the unconditional full-join transition loop before the prepare-only return; guard failure raises and rolls back the function; an earlier replay return inserts nothing. The source census found one product snapshot writer and one engine declaration for every recorded commit; the only other product reference is the Correct Final caller. The four version diffs preserve that construction and add only later admission/scope/follow-up/native-hook behavior.

The integrator separately supplied fresh `F2_INSTALLED_FINALISER_BINDING.json` and `F2_SNAPSHOT_IMMUTABILITY_RECEIPT.json`: actual TEST engine body MD5 `7821cf16ae3c98aaa212325228e147a2`, original FIN recursive closure `240e2460baf9bff3164cc4ed389b6acb5774f8f88ab0deefc70d6f085fae00f1`, original schema migration hash `1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d`, denied browser/service direct table writes and enabled immutable guards. Some source-era first/last ledger records belong to FAILED partial attempts; the later exact e0 VERIFIED contract covers those same definitions. Their state is retained, never repaired or relabelled.

The integrator approved a bounded origin fence, which is now in the selector: exact original schema first-release/hash and original FIN closure; exact new F2 migration path/hash with its real recorded installation time; an earlier canonical CYCLE/profile/row/resolution/date/work-event lineage; the latest eligible original POSITIVE/REPLACEMENT pay movement at or before the origin revision; exactly one legal non-null presentation. Mode/true/group derive from the proved construction; presentation derives from that immutable pay movement. Unknown, conflicting, newer or changed proof returns NULL. The selector never reads today's policy/settings as history. This is proof of controlled recorded-source construction, not theoretical absence of an unrecorded administrator write.

If Banking integration changes the original FIN closure or introduces a new complete engine, the legacy gate fails closed. The integrator must review that complete native-hook-compatible successor and fresh binding before changing either allowlist or generator pin. No guessed hash is allowed. Actual installed HC-2 proof waits for the protected F2 release; no positive fixture may forge a VERIFIED release or ledger.

## Verification and remaining gates

| Check | Result / boundary |
|---|---|
| Initial `db:check` | 295 migrations / 802 repeatables PASS |
| Final `db:check`, migration lock and whitespace | 296 / 803 PASS; one locked additive migration; no old migration change |
| Deterministic complete successor and eight F2 source guards | PASS |
| Weekly Source product source/unit suites | 415 passed / 14 skipped / zero failed; skipped cases not acceptance |
| Parser suites | 70 passed / 3 skipped / zero failed |
| Pure harness suites / `test:weekly-source:unit` | 81 passed / 3 skipped / zero failed; no real database, Docker or external request |
| Source-era history census | 109 attempts / five complete engines; independent full source/control-flow review and attributed parent binding |
| Data-free SQL vectors `13102026_1228_weekly_source_final_value_fingerprint_vectors.sql` | Written for integrator first use; not executed by this lane; no business/ledger writes |
| Combined PG17 compilation, canonical contract/security inventory, exact installed API | Pending integrator |
| U-1…U-5/U-5b/N-7 actual Finalise and HC-1…HC-3 | Pending real TEST fixtures and installed engine; source guards are not business proof |
| U-12 native NEXT projection receipts | Pending joined actual published NEXT proof |
| SM-1…SM-7 / I-3 money, window, releases, pair replay and unwind | Pending F3/F5 source-money producer + F5/F6 invoice owners and joined acceptance |

Two setup failures were resolved without source changes: fresh worktree lacked its locked `xlsx` package (installed existing dependencies with scripts disabled), and the cross-repository delivery test assumed a sibling `mytms` path (rerun with its supported task-scoped `CLOUDTMS_MYTMS_SOURCE_ROOT` pointing to the complete current MyTMS worktree). No dependency or environment-file change was saved.

All edits remain uncommitted for integrator review/complete-source backup. No hosted state/TEST fixtures, payment/provider/settlement/remittance, real email, secret/auth state, Google inputs, app runtime, LIVE or canonical checkout was changed. Policy X is preserved: no post-draft live fallback, economic-key ladder or freshness bypass; all existing pay/reversal/NEXT owners are called unchanged.

Integrator checkpoint: hosted F1 configuration proof72statements and F2 helper/vector proof29statements PASS, then combined final-security sweep198statements PASS; all temporary objects/fixtures and original routines/ACLs restored on fresh connections. Combined source297migrations/808repeatables and45focusedchecks PASS. All installed financial/API/document/app/NEXT/phone gates and generated contract remain pending; no release or durable activation occurred. Exact receipts/progress live in root build output and PROGRESS.md.
