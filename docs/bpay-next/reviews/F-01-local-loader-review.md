# F-01 local schema loader independent review

Reviewed 9 October 2026 against the current saved BUILD-BE source, base `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. Read workspace/repository AGENTS.md, Database Release Bible, owner start sheet, B2.23, F-01/F-23 and their checklist rows, the loader, current release adapters, BUILD_LOG and the installation investigation. This is a source-only review. No SQL, test, fixture, Docker, runtime, hosted database operation or credential read was performed. PostgreSQL public documentation was consulted for connection-option precedence.

Verdict: **changes required** before the loader can provide the promised local-only, resumable working-tree compilation evidence. Protected verification and app acceptance remain pending.

## Actionable findings

### LL-01 — P1: effective libpq destination can bypass the local URL guard

`scripts/bpay-next-local-schema.mjs:23-30` checks URL authority/path fields but passes the complete unfiltered URL to `openManagedPsqlSession`. The latter also inherits all process environment variables (`scripts/cloudtms-db-release-lib.mjs:594-598`). A `hostaddr` connection parameter or inherited `PGHOSTADDR` can choose a remote address while the checked URL hostname remains `127.0.0.1`; query parameters can also override other connection identity fields. PostgreSQL documents that, when both host and hostaddr are present, hostaddr determines the network address. The loader opens that connection and requests the writer advisory lock before its database/role identity check. Consequently the advertised local-only refusal does not hold for the effective connection, even if the later identity check refuses DDL. [PostgreSQL 17 libpq connection documentation](https://www.postgresql.org/docs/17/libpq-connect.html#LIBPQ-PARAMKEYWORDS).

Fix: require a PostgreSQL URI scheme, reject target-affecting query parameters and unexpected query keys, and neutralise inherited libpq routing/service options before opening the child connection. Prefer explicitly pinning the effective host address to `127.0.0.1` as well as the existing role, port and database. Do not print the URL or environment values. This is a fail-closed source correction; no remote negative probe is needed.

### LL-02 — P2: interrupted loads do not retain a complete source checkpoint

The initial `LOCAL_SCHEMA_LOADING` checkpoint at `scripts/bpay-next-local-schema.mjs:75-76` contains no inventory or selected roots, and is written only after bootstrap commits. The deletion guard at lines 90-94 only inspects `previous.inventory`. If NEW is interrupted after a new repeatable is receipted, its root can disappear before CHANGED and pass both this guard and `pendingInventory`, which checks absent/modified installed migrations but does not reject removed repeatables. The same gap exists for a newly introduced root receipted during an interrupted later CHANGED invocation: the previous successful inventory predates it. The loader can then write `LOCAL_SCHEMA_COMPILED` while the database still contains the removed authority. A process/file-write failure between bootstrap commit and the first checkpoint also leaves neither NEW nor CHANGED able to resume through the stated procedure.

Fix: atomically persist the exact attempted canonical inventory/selection before executing bootstrap or units, with an explicit prepared/loading state and safe receipt-based recovery. Independently reject installed repeatable-ledger paths absent from current inventory. Retain previous successful inventory separately if needed. Do not manually repair database receipts.

### LL-03 — P2: reader activation rereads bytes without validating the receipted hash

Lines 125 and 137-139 compile each reader for source validation but discard the compiled closure, then call `readManagedClosure(...).source` again after all ordinary installation/preparation work. A concurrent editor can change reader bytes between those calls. Activation installs the reread bytes while `repeatableLedgerSql` receipts the older inventory hash. The final inventory comparison detects the change only after that activation transaction has committed; it cannot provide the promised fail-before-execution source/hash guarantee, and a further edit restoring the original bytes can hide the mismatch entirely.

Fix: retain the validated compiled reader closures and activate their captured canonical mapped source, or perform the same item-hash comparison immediately when reading activation source. Preserve the exact pair and single activation transaction.

### LL-04 — P2: checkpoint release ID is interpolated as raw SQL and receipt match is incomplete

CHANGED accepts any checkpoint release ID beginning `bpay-next-local-schema-` (lines 78-79) and places it directly inside a SQL string literal (lines 84-85). `identitySql`'s shared context validation permits arbitrary nonempty release IDs and does not escape this later handwritten query. An edited or malformed local checkpoint containing a quote can therefore change the query rather than be refused. Separately the receipt query checks only ID/status, while the engine's resume check also matches commit, expected contract, mode and managed evidence; CHANGED can accept contradictory checkpoint metadata that would fail the existing protected resume contract.

Fix: validate the exact generated ID form and checkpoint format/state, use the shared `literal` helper for SQL data, and require the complete existing resume receipt match while keeping this local compiler restricted to its APPLYING receipt. Keep the original bootstrap context stable across legitimate working-tree batches.

## Defect corrected during review

The initial loader unconditionally selected and reran `22082026_1402_candidate_named_security_definer_browser_isolation.sql`. Its baseline lock already receipts the unchanged historical closure (`a403e351a7adc5756547c74406de5a0bf71250e4574d11770f34c5a332db0a6d`), and its historical exact 85/7 inventory cannot run against later Candidate definitions. Root reported the real initial NEW failure and removed it from both unconditional selection/tail lists; current saved source confirms that correction. Preserve the old source and receipt. The current dynamic `26082026_0230` closure supplies the final public SECURITY DEFINER browser boundary, with the current exact Candidate v3 catalogue verifier used only within permitted final catalogue checks. Do not widen the historical fingerprint or claim full protected release verification.

## Satisfactory boundaries and remaining scope

- Current source reuses canonical release inventory/order, LF owner mapping, hash-bound exception handling, immutable bootstrap and owner RLS envelope. It uses one persistent writer-lock session with resets and atomic adapter-produced unit receipts.
- NEW performs the existing blank-schema assertion and uses the baseline/control/bootstrap path; CHANGED verifies database/role identity and installed migration hashes. It neither invokes catalogue rehearsals nor runs fixture/verifier manifests, and it never writes VERIFIED.
- The exact Weekly Source reader pair/preparation is retained; final invoice/general browser and Weekly Source ACL closures run after ordinary definitions/readers. These provide installation state, not app acceptance.
- The optional F-04 final-definer map is absent at this foundation stage. F-04/F-23 must complete and validate it before dependent changed-predecessor batches; its selection must include every required successor regardless of map row order. No completed F-04 claim was made here.
- BUILD_LOG correctly records F-01 as part done and local compilation/seal/coupling as pending. After root's successful compilation, update its local profile/check results and the investigation's obsolete recommendation to rerun historical 1402.

No product source was edited by this reviewer. Only this review note was created. No deploy, payment/provider/bank/email/drain action or secret disclosure occurred. Policy X and Candidate/MyTMS business semantics were not changed.

## Follow-up source verdict — LL-01 through LL-04 closed

Re-read the saved loader on 9 October 2026 after root's corrections. **PASS for the F-01 local compilation scope; no remaining actionable loader defect found in this review.** This supersedes the initial changes-required verdict above.

- LL-01: the loader requires the PostgreSQL scheme, rejects URI query/fragment values and pins effective libpq address/port/database/user to the task loopback profile. Service overrides are removed in this process only.
- LL-02: the loader atomically saves PREPARED source inventory before bootstrap and LOADING inventory/selection before each batch. CHANGED also rejects absent installed repeatable roots. The saved pre-bootstrap checkpoint permits NEW retry while the schema is still blank, and the existing exact bootstrap receipt supports CHANGED after commit.
- LL-03: exact reader units are compiled once and activation consumes their captured canonical mapped closure source.
- LL-04: checkpoint format/state, exact commit-derived ID and NEW/TEST identity are validated; the complete shared resume match is checked; SQL uses `literal`, and APPLYING remains mandatory.

Root reports a passing zero-unit CHANGED resume, db:check, node --check, local seal/coupling and a contract byte-identical to the base. These results were not independently rerun by this source reviewer. Full protected verification and app acceptance remain pending; F-04/F-23 still own the final-definer map and later final checks. BUILD_LOG should record the completed F-01 local results when root updates the completion record.
