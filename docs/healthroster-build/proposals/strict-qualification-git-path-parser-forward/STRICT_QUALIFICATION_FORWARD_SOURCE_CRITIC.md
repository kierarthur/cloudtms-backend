# Strict qualification forward correction — independent focused source re-review

Reviewed 13 October 2026. **The two previously accepted source corrections are present and correctly bounded. Source verdict: REJECT the frozen12's complete all-tracked equivalence gate pending one remaining P2 path-parser correction.** The corrected setup expression is useful forward source, and both previously omitted actual configuration inputs are included. Unsupported path rows must now fail closed instead of disappearing, and raw path bytes must not be decoded lossily. This is a focused source re-review, not final Part B, a native acceptance result, release approval or activation authority. The old failed frozen45 remains rejected and unchanged.

## Exact scope and preservation

- **F12:** `.codex-worktrees/healthroster-v2-money-build-13102026/docs/healthroster-build/proposals/strict-qualification-first-use-inventory-forward`.
- Money branch `codex/healthroster-v2-money-build-13102026`: saved HEAD and fresh GitHub HEAD `1c7d4d53ccc17cb269715e2e6ff902942d73e452`.
- F12 `REVIEW_MANIFEST.json` SHA-256 `e05b9ec2574dcf83c7773171adb50e116f3181d28812ac6e8571dd79fab5c427`. All 11 declared files' complete saved-byte hashes and lengths match; the manifest is the 12th file.
- Root branch `codex/healthroster-v2-backend-13102026`: saved/current pin and fresh GitHub HEAD `cea15289e0283e3383e71f0acb8b18ce184647c2`. All 16 current root source pins in `SOURCE_DELTAS_AND_AUDIT.json` match, including exact canonical 2206 verifier SHA-256 `28c053d075970324387d33d86402156fe7bcc55225c03c769abe15c14d18e273`.
- Independently rechecked all nine frozen predecessor manifests and **210 declared source inputs**: no hash/length mismatch. This includes frozen45 manifest `66b5b97d5628be25a0555c20c3a5755bf9561bd760afcaaaaed1b9b09f71b700` and both six-file pin-forward packages, with manifest hashes `10db4d8d3728129f429a0cfaf8d8670dbbf1125bcc9be53b60ff7d06b1664e8f` and `4784752bda1a9c5df7edad45d99715fa114efd25b85c7810e9cdc7c01dd1e272`.
- The original OUT `STRICT_QUALIFICATION_SOURCE_CRITIC.md` remains SHA-256 `5fe44bacfbbafc6ac5c793fd1932bc5021b745d6ddada405b2b8f864c748d9d5`; F12's retained copy has those same bytes. This re-review does not replace it.
- The root's pre-existing untracked `FULL_VERIFIER_SOURCE_PROTOCOL_CENSUS.json` and the creator's separate unfinished `strict-qualification-native-interfaces/` directory were preserved. The unfinished proposal was not read or reviewed.

The source/owner instructions read in the prior focused review remain controlling: workspace/backend AGENTS, SEND_5, updated SEND_5B, SEND_7 and the Database Release Bible. No database route, secret or Banking worktree/chat was selected. F12's full source, source tests, frozen audit/metadata, predecessor reverse boundaries and effective SQL DTO were reviewed. Generators were read only.

## Previously accepted P1 and P2 corrections

**P1 expression correction is exact.** F12 `TEST_JOURNEY_SCOPE.sqlinc:121` uses:

```sql
hashtextextended('weekly-source-test-journey:'||(auth->>'scope_id'),0)
```

Independent whole-file reverse comparison confirms this is the sole difference from the frozen45 TEST setup owner. OPEN, CLOSE and REVOKE retain the same scope key, actor/admin/deployment, current VERIFIED/catalogue, reserved fixture, expiry/day, original census/Source CAS/root, terminal and assignments/readiness-OFF checks. No scope/audit/assignment behavior, signature or grant was added by this correction. The supplied source guard detects the rejected expression and accepts the parenthesized form. Real PostgreSQL first use of OPEN/CLOSE/REVOKE remains unexecuted; a source correction is not a runtime pass.

**Both previously omitted inputs are included.** F12's proposal/native execution-tree modules explicitly require `.gitattributes` and `osv-scanner.toml` and replace the old directory/root whitelist with selection of the current complete tracked tree. The SQL execution-tree DTO's required-path assertion at `OPERATIONAL_CERTIFICATE.sqlinc:165` now includes both inputs. The actual source-gate scanner configuration and SQL checkout attributes are therefore present in the current positive proof rather than asserted by an empty/hash-only claim.

Independently reversed all **four complete artifacts** against frozen45: TEST setup owner, proposal execution-tree module, native execution-tree sidecar and complete operational certificate/DTO owner. All four match their entire predecessor bytes after reversing only the declared hunks. The native sidecar differs from the proposal only in its declared qualification-helper import path. Thus every other operational certificate, source proof, setup owner and native guard is retained; no new routine signature, table, column, grant or exposure is supplied by F12.

## Remaining finding — P2: unsupported and invalid UTF-8 Git paths do not fail closed

F12 `source-equivalence.mjs:32-33` defines a supported-path predicate. `parseGitTree` at lines 37-41 decodes with `bytes.toString('utf8')` and then applies `.filter(row=>selected(row.path))` before its inventory/type checks. The identical native sidecar has the same behavior.

Two independently reproduced, data-free counterexamples use the actual pinned root Git tree and existing committed blobs:

1. The actual tree has **3,694 raw rows**, all selected. Add one in-memory row named `unsupported\source-input.sql`: there are 3,695 raw rows but still 3,694 parsed rows. The new row silently disappears and `assertTreeEquivalence` accepts equality with the original tree. A row with that same unsupported name and `160000 commit` also disappears before the unsupported-object assertion. Unsupported names should refuse; they must not become an exclusion.
2. Append an otherwise equal pathname represented by invalid UTF-8 byte `FF` in one input and `FE` in the other. Both decode as `input-�.txt`. The different raw pathname bytes yield identical parsed inventories, and the actual comparator accepts them with the real committed blob reader. It does not prove raw Git path equality.

These were in-memory protocol variations, not newly created Git trees, changed refs/files or an observation that the present corpus contains such paths. Git's `ls-tree -z` outputs filenames verbatim, terminated by NUL; converting that byte stream lossily loses part of the input authority. See [Git's ls-tree output format](https://git-scm.com/docs/git-ls-tree#_output_format).

**Consequence:** the current 3,694-file positive is valid for its supported paths, but the claim `ALL_TRACKED_GIT_OBJECTS` is not enforced for every input. A new unsupported tracked entry can be invisible, and distinct raw names can collapse. This is the same fail-closed completeness boundary that the broader inventory was intended to repair; no native/database security failure is claimed.

**Required adjacent forward correction:** parse every raw NUL record without silently dropping any record. Require valid, round-trippable UTF-8 if the protocol uses UTF-8 paths, or retain raw path bytes as the exact compared authority. Unsupported paths must raise a closed error. Preserve accepted regular-file mode/type rules, exact path/blob/byte comparison, required actual input assertions and the three fixed committed receipt-bundle exceptions. Add rejection vectors for unsupported path rows, unsupported objects under those names and invalid UTF-8; add an ordinary valid non-ASCII path positive and a differing valid Unicode path negative. Do not normalize, rename, omit or rewrite tracked inputs to make equality pass. Keep F12 and all predecessors frozen.

## Effective whole-vector proof and exception boundary

Ran the actual `proveCommittedSources` and `assertExecutionTreeProof` functions against pinned root `cea15289e0283e3383e71f0acb8b18ce184647c2` compared with itself, in memory only. The proof covers **3,694 files**, all eight explicit required paths, regular Git modes/types, committed blob IDs and raw blob hashes/bytes. It has zero receipt bindings in this present tree, JSON size **2,031,987 bytes**, and maximum pathname UTF-8 length 206 bytes. Domain hash: `8a5d6071b8dae6e9eea50d0bffc6a7c5459139440b4738ab0da8ef20a6096279`. This is a data-free source protocol result, not an R1/R2 execution or qualification artifact; no certificate or PASS receipt was saved.

The effective SQL DTO still accepts a nonempty literal array with unique paths, eight required actual inputs and closed mode/type/OID/hash/comparison fields. It does not need an invented thin/empty frame to represent the actual vector. PostgreSQL cannot read Git: this DTO/domain check remains subordinate to mandatory protected writer remeasurement. It must never be treated as proof that a caller-provided JSON happened to match itself. The actual complete SQL vector has not been executed, and actual route/envelope byte bounds must be preserved when root later qualifies it.

The only per-file exclusions remain the three literal committed paths `supabase/release/weekly-source-protected-prerequisites.json`, `weekly-source-protected-operational.json` and `weekly-source-protected-test-setup.json`. They must have fixed path/hash descriptors and actual committed bundle-byte bindings in each role. The release-manifest comparison removes only `releaseId` and those three closed descriptors, while comparing the remaining installation, verification, security, contract and exception authority. Other docs, AGENTS, proposals, native-result files, infrastructure, root configuration and scripts/tests are selected. Ordinary additions/deletions/path/mode/type/blob changes require fresh execution evidence.

Accordingly the 93 recorded d67→cea metadata/source changes do not become execution equivalence merely because runtime SQL stayed unchanged. Real R1 execution identities remain literal. Every permanent input must be committed before actual R1; later ordinary documentation/results/source changes invalidate its proof. Root may record real collector outputs only through the established three bound bundle paths. The unfinished separate SECURITY/OFF-automatic work cannot borrow an earlier execution identity after changing source.

## Checks and boundaries retained

- Re-ran all supplied **8 source tests**: passed, no failure/skip. Re-ran all **5 JavaScript syntax checks**: passed. The supplied vectors prove both formerly omitted files' real-blob, executable-mode and unsupported-type refusals; actual current whole-tree selection; AGENTS/native-result/new-root-file refusal; corrected setup expression; required SQL inputs and sidecar parity.
- The additional path counterexamples above are outside those supplied eight checks. Their passing result does not cover unsupported-path filtering or lossy decoding.
- Independently verified frozen file/pin preservation and all four full reverse proofs rather than accepting manifest booleans alone.
- The audit's one rejected packet prefix expression, zero corrected packet candidates and eleven retained native repeatable candidates are source-census observations. Historical/current controlling-owner resolution and actual first use remain separate; this re-review gives no authority to edit unrelated Candidate/native sources or declare their runtime failures.
- F12 changes none of the strict pending-V2/native-only-V1 activation refusal chain, owner-XID/SEND_7 scope, bounded cross-session TEST actor/event/fixture authority, original Client census/39 Source facts, complete native bulk/own-stage/financial locks, trusted Worker actor composition or immutable closure/remote-origin requirements reviewed previously.
- Source counts remain preserved root232 plus 26 absent dependencies = planned258, with 105 tables and 142 service signatures. Those counts remain source inventories. No measured complete installed catalogue is supplied by this packet.

## Gates held and handback

Native SQL/provider first-use OPEN/CLOSE/REVOKE and caller/role/expiry/terminal/CAS negatives; full generated contract/catalogue/ACL union; Banking first installation and current compatible NEXT; genuine LEGACY/NEXT financial/component/lock/hook, original-Client/39-fact prepare/stage/complete equality; fixture vacancy/descendants/account and real manager/Candidate signatures; actual source/document/renderer/frozen issue bytes; protected R1→R2 recording; deployed authenticated HTTP, Office UI and physical MyTMS TestFlight phone; actual native SECURITY activation positive, readiness and post-activation replay; lawful OFF automatic/effective-rule all-consumer qualification; actual F1 dates/rules/component/native callers; separate Report A/B financial/admission/render/pay-isolation dispatch; full operational sign-off: **all held/unexecuted by this re-review**.

The original `active_admin_activation` assertion cannot be renamed from lease OPEN. The separate proposed SECURITY/OFF-automatic interfaces are unfinished and outside this review. Sunday 11 October remains NULL-rule legacy control; enabled week Sunday 18 with Monday 12 coverage floor and default Thursday 22 upload gate remain actual native date constraints. No clock substitution, old-receipt retagging, manufactured certificate/rule/module/READY or Report variant selection is accepted. Full rehearsal remains subject to SEND_7 and the separate Banking first-install record.

Created only this new requested report. No frozen/source/generated file, old report, dirty file or unfinished proposal was edited. No generator, database/SQL/rollback/API/Worker/browser/Docker or Banking operation ran. Read-only GitHub branch checks and Git documentation were the only network work. No secret/private credential file was read or printed. No Git object/ref/commit/push/PR, TEST/LIVE install/deploy/promotion/activation, payment/provider/settlement/email/drain, phone build/upload or destructive action occurred. Policy X/OR-44 and economic owners remain unchanged.

Retain the source hold. A small adjacent path-parser correction can close the remaining source finding; every native and operational gate above still needs its own actual evidence afterwards.
