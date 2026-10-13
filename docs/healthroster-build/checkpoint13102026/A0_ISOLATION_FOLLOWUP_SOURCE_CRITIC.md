# Independent A0 isolation follow-up source critic — 14 October 2026

Verdict: **ACCEPT THE FOCUSED SCANNER CORRECTION AND THE PINNED SOURCE EVIDENCE CHECKPOINT; KEEP EXECUTABLE COMPOSITION, NATIVE ACCEPTANCE, EXPOSURE AND RELEASE HELD.** The quoted internal `"char"` defect is corrected meaningfully, and the complete current effective census is preserved. No new P1/P2 source defect was established in this follow-up. The ranked items below are retained engineering blockers, not newly observed database failures. This is a focused source critique, not final Part B or operational/native acceptance.

## Scope, authority and exact pins

Lane: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-protection-build-13102026`, branch `codex/healthroster-v2-protection-build-13102026`, saved HEAD **`9201cb609a9f952811c99460f55717ec6d3d9175`**. A fresh read-only remote branch lookup returned that same exact commit. The lane was clean at both source checks.

Root evidence input: `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026`, initially observed HEAD **`ab26a5ff22200be037d215b1100c969d6c0bae73`**. During review the root integrator advanced it to **`18ba9d5061085d359838a361b6ccae07bb1fd24a`**. A read-only diff from the pinned input to that successor is empty for `supabase`, `broker`, Candidate Workers and `infra/miget`; the source checks here therefore retain the pinned actual SQL/Worker/contract authority. This is metadata-forward preservation, not a claim that contract equality proves an execution tree equivalent. No root source was edited by this reviewer. The pre-existing root `FULL_VERIFIER_SOURCE_PROTOCOL_CENSUS_V2.json` was left untouched; its subsequent integration was the root agent's work.

Reviewed new manifest: `docs/healthroster-build/proposals/cn-a0-isolation-source-followup/ISOLATION_SOURCE_MANIFEST.json`, **354,048 bytes**, SHA-256 **`f031b83e3b17c609260ae34508e4f859f8d49f91277ca49c5f43c84851fc7e50`**. All **11 listed members**, **5,896,201 bytes**, match their whole-byte identities. The manifest is separate from those 11 members. The large JSON files were read completely for parsing, input/source-definition verification and hashing; the review was not based on excerpts alone.

Applicable workspace, backend and lane AGENTS were read, together with the Database Release Bible and the complete owner SEND_5/SEND_5B/SEND_7 instructions. The source-only task boundary controls this critic. The PostgreSQL review skill was applied to the lock and isolation reasoning. Banking's supplied state remains IN PROGRESS, run `38476430690`, shared turn key `(20261013,1)` held. That supplied state was not refreshed by this reviewer; no Banking file/chat was read or messaged and no database turn was attempted.

All three complete reports were read and hashed:

| Preserved report | Bytes | SHA-256 |
| --- | ---: | --- |
| `A0_CREATOR_SOURCE_CRITIC.md` | 17,122 | `f2887142f109c35c0861eaed4e43a3b46429dcae5ec3eebba6a87e0812e81b9f` |
| `A0_CREATOR_FORWARD_SOURCE_CRITIC.md` | 19,062 | `865f801039ff14deb3f00821a3ec8ae7e4cec3ba8b79a9672d2510eefe53498f` |
| `A0_ISOLATION_SOURCE_FOLLOWUP.md` | 21,480 | `3683e5f3f20f27f95a8faa5c07fcd7e0c97add44d77a05a0f7db9b2001038795` |

## Ranked retained blockers

### 1. P0 exposure gate — hidden-P0 RR admission remains unqualified

Locations: frozen `cn-a0-phantom-writers/13102026_2153_native_creator_intents.sqlinc`, especially the active-request lookup before the isolation refusal; current packet `SOURCE_REVIEW_AND_NATIVE_RECIPES.md` and `APPLICATION_ISOLATION_AND_LOCK_REVIEW.json`.

The active writer arm rejects non-RC only after finding a visible WAITING request. SEND and the proposed atomic fence also reject non-RC within their own scoped missing-A0 purpose. These refusals do not establish what happens when transaction A acquired its RR snapshot before transaction B committed the new WAITING request/P0. A later child reference to B's P0 UUID can encounter neither a visible request nor a visible P0 parent; a Candidate identity/rank mutation can likewise consult stale request/family truth. There is no saved Timesheet FK that closes the validation/invoice reference case, and ordinary NULL/dangling history cannot be rejected indiscriminately.

Locking an unchanged Original or Candidate does not refresh A's RR snapshot. A concurrent-update error requires the relevant row to have actually changed after that snapshot, rather than merely to have been locked. The packet's hidden schedule is a retained source-level engineering counterexample condition, not a claimed executed failing test. PostgreSQL 17 describes both the fixed first-statement RR snapshot and the requirement for a committed update/delete before this serialization failure. [PostgreSQL 17 transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html#XACT-REPEATABLE-READ)

The 119 saved application files contain no explicit isolation configuration, and the saved catalogue contains no routine isolation override. That absence is insufficient to prove deployed admission. PostgREST v14 documents one transaction per API request and role/function overrides. Its v14.17 source is more precise: `MainTx.planIsoLvl` selects the top-level RPC's `pdIsoLvl` when present; otherwise it uses the impersonated role's cached isolation, falling back explicitly to RC. Table DML uses that role selection. `SchemaCache` derives the RPC setting from `pg_proc.proconfig`, and `Config.Database.queryRoleSettings` obtains role isolation separately. Thus RPC and table-DML paths require separate qualification; setting only SEND/atomic cannot qualify every writer. These primary sources support the packet's hold, not an assertion about the version/settings actually deployed. [PostgREST v14 transactions](https://docs.postgrest.org/en/v14/references/transactions.html#isolation-level), [v14.17 MainTx](https://github.com/PostgREST/postgrest/blob/v14.17/src/PostgREST/MainTx.hs#L107), [v14.17 SchemaCache](https://github.com/PostgREST/postgrest/blob/v14.17/src/PostgREST/SchemaCache.hs#L421), [v14.17 role settings](https://github.com/PostgREST/postgrest/blob/v14.17/src/PostgREST/Config/Database.hs#L128)

Required closure remains actual supported entry-point/isolation qualification and the exact hidden schedule against the complete guarded composition, or independently reviewed native serialization that covers every supported older-RR writer. No global ordinary-RR rejection, timestamp/version bump, invented FK, new mutex, namespace marker or write to signed/issued authority is approved here. This is engineering work, not an unanswered business ruling.

### 2. P1 native gate — static reachability and lock statements cannot close the five schedules

The graph independently matches **122 roots, 605 nodes, 2,250 syntactic call candidates and 4,149 automatic-trigger candidates**. All reached complete source-definition identities match the saved final-owner map. The review preserves **169 lock-bearing owners and 492 complete row-lock statement records**, with whole statement hashes/source inclusion checked independently. Actual advisory-lock/settings expressions are preserved with their surrounding statements. The original relation INSERT-column-list false callee issue remains excluded.

Every edge is still static. **Observed native invoked edges: 0.** Overload binding, unqualified search path, branch execution, dynamic EXECUTE, trigger WHEN/UPDATE OF conditions, statement/transition-table hooks and physical enabled state are not resolved into invocation by this packet. The graph is complete only for its stated finite saved traversal. Full lock statements improve reviewability but do not certify actual order or freedom from contention. Outer table DML can already own its target row before a BEFORE trigger, so sorted locks inside the helper alone are insufficient.

All five refined recipes are concrete and remain **UNEXECUTED / HELD**:

| Native gate | Remaining concrete qualification |
| --- | --- |
| `VALIDATION_FIRST_INSERT` | Reach genuine Weekly/Daily import-review first INSERT; prove actual precursor intent, active guard/I-1 admission or bounded refusal, conditional FIN wakeup and full history/timeout preservation. |
| `INVOICE_FIRST_INSERT` | Exercise genuine native line generation/edit with exact-ID and booking-only branches; preserve the archived guard and trigger order; qualify actual revision/document/statement branches without invoice issue or provider effects. |
| `CANDIDATE_ALIAS_RANK` | Use INSERT, owner-mediated DELETE, key/alias UPDATE and rank-only unrelated UPDATE; preserve native delete locks/eligibility and BEFORE setters; qualify canonical winner, reversed contention and same-unit reentry. |
| `RAW_BOOKING_SIBLING` | Exercise an admitted native same-raw-booking sibling with NULL/foreign Contract, raw/trimmed collisions and changed identity; prove before/after SEND behavior, same-unit P0 creation and request/family CAS. |
| `NATIVE_FIN_LATE_HOOKS` | Execute the genuine normal context/calculator/writers and exact guarded missing-A0 composition; record actual late FIN/expense/retention/document/invoice/Banking branches and native locks, then compare Original/R0/P0 and full frozen financial authority. |

The existing acquisition proposal remains sorted persisted Candidate gates, one whole native I-1 existing-family acquisition with bounded timeout, exact request SHARE NOWAIT and sorted Timesheet KEY SHARE NOWAIT, followed by complete rereads and timeout restoration. Neither this order nor unchanged precursor locks closes the hidden snapshot case. The five recipes correctly forbid fabricated financial/history/READY evidence, disabled native triggers and effects reaching payments/providers/settlements/email/drains. Branches must be screened before a future permitted trial; suppressing normal triggers is not a valid substitute.

### 3. P1 verification gate — A0 positives cannot be credited inside mandatory outer RR

Independent complete recursive reads covered **170 mandatory UPGRADE closures / 180 distinct verifier files**. Exactly **nine roots** establish RR and include the snapshot guard. The guard raises `SOURCE_VERIFIER_TRANSACTION_SNAPSHOT_REQUIRED` unless actual isolation is `repeatable read`. There are **zero source call references** to correction SEND, atomic or the new A0 helpers in those current closures. There is no evidenced existing mandatory positive A0 fixture to rewrite.

A future positive missing-A0 SEND/atomic/visible-active creator attempted inside the outer RR envelope will deliberately refuse with `CORRECTION_A0_TRANSACTION_SNAPSHOT_UNPROVED`/55000. That may prove refusal, but cannot count as successful native materialisation. Savepoints, removing inner BEGIN/ROLLBACK or setting `default_transaction_isolation` later do not replace the outer established snapshot. Running all verifiers in RC or stripping the guard would remove required ordinary coverage. Preserve the full RR rehearsal and run separately reviewed short RC native schedules with their own restoration evidence. The packet correctly leaves both requirements open.

## Scanner correction and census preservation accepted

The changed `typeText` retains quoted type tokens before alias lookup. `(p "char")` now remains `"char"`; ordinary `(p char)` becomes `character`; `pg_catalog."char"` and arrays retain their quoted/qualified distinction. The meaningful two-overload CREATE/DROP negatives demonstrate that dropping ordinary `character` no longer deletes the internal-char owner, while the exact quoted DROP leaves the ordinary alias owner. The predecessor fails the former negative. PostgreSQL documents internal `"char"` as distinct from ordinary `char(1)`. [PostgreSQL 17 character types](https://www.postgresql.org/docs/17/datatype-character.html)

The entire scanner successor reverses byte-exactly to its preserved predecessor by the recorded unique substitution. This is a finite conservative source resolver: retaining explicit quoted qualification is intentional, not a claim to resolve every quoted catalogue/user alias, OID equivalence or arbitrary search-path binding. No new generic PostgreSQL resolver claim is accepted.

An independent in-memory replay used the current repository release library's enumeration and recursive include order. Both old and corrected scanners reproduced **304 migration roots, 835 repeatable roots, 4,829 CREATE events and 2,270 final identities**. Every identity, file, line and whole-definition hash agrees with the frozen corrected census. Lifecycle replay also retains **226 DROP events / 67 actual removals**. The current corpus contains no observed char-driven census change. The earlier declaration-comment/DROP corrections and trigger-name/event ordering proof remain retained.

The **70 controlling direct-token writer candidates, 30 first creators and 233 historical writer occurrences** are preserved. These are saved-source candidate/effective-owner counts, not a newly observed installed census or native invocation count.

## Complete source/preservation checks

All **1,625 current root input pins**, **106,957,732 bytes**, matched their recorded whole-file hashes and byte counts. All **six predecessor manifests** and **205 listed frozen artifacts**, **11,529,818 artifact bytes**, matched. Manifest files are counted separately from their listed artifacts; the 205 must not be described as including their six manifests. No predecessor generator was executed and no frozen packet was rewritten.

The complete application inventory reproduced **119 JS/MJS/TS files**, zero explicit isolation/transaction-end source references and zero saved routine isolation configs. The declared `candidate-private-api/src` directory is absent; its actual configured main is `../broker/src/candidate-private-worker.js`, which is inside the covered broker scope. An initial independent inventory check stopped on that absent directory; the rerun explicitly accounted for it and matched the packet. This is not an omitted private Worker entry point or application defect.

Direct inspection confirms that `proxyPostgrest` forwards one HTTP request's method/body/query/headers and does not offer transaction reuse. The correction factory in `broker/src/timesheet-correction-orchestrator.mjs:447` is unmounted in the reviewed `index.js`. Its SEND/atomic calls at lines 507/499 are callback source references, not native application invocations; its current missing-Original-finance branch still refuses before mutation. Ordinary Candidate/Timesheet table POSTs and the separate invoice enqueue/generate RPC requests remain distinct authorities. Window-based `directMutationCandidates` remain candidates, not a complete AST mutation census. No caller/API observation was made.

Executable composition still needs at least the **four preceding normalization/fence helpers plus the six creator helpers**. The four are `private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid)`, `private.timesheet_correction_a0_clock_scope_v1(uuid)`, `private.timesheet_correction_a0_native_input_witness_v1(uuid)` and `private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid)`. Naming only creator6 would omit required dependencies. This follow-up adds **zero SQL owners/interfaces**. Root must perform the literal union with its complete existing private/security inventories and every other retained successor against current Banking source; a finite proposed count is not installed/runtime census proof.

## Checks actually run and limits

- Seven supplied Node suites: **123 passed, 0 failed, 0 skipped, 0 cancelled**.
- Strengthened isolation follow-up suite rerun: **11 passed, 0 failed, 0 skipped**.
- Four packet JavaScript `node --check` checks passed. No generator was executed.
- `npm run db:check` passed on the reviewed root: **304 migrations / 835 repeatables**.
- Independent scanner reversal, complete old/new/frozen census equality, all 605 whole-definition pins, all 169 lock-owner statement source/hash checks, all 1,625 input pins, all six prior manifests / 205 artifact hashes, full application inventory and mandatory RR closure scans passed.
- Read-only Git evidence confirmed lane local/remote equality and the reviewed root's runtime/SQL/contract preservation at its metadata successor.

PostgreSQL compilation/first use, actual service-role ACL, installed trigger state, deployed PostgREST/table-DML/RPC isolation, two-session contention, native Original materialisation/reread, exact R0 sign-copy/P0 approval, full component/rate/config/Source equality, immutable documents/NEXT, Worker/API/Office/MyTMS/TestFlight/phone acceptance and feature activation remain unproved by this critic. Mock callback tests and static receipt/census checks do not supply those results. Full rehearsal remains subject to Banking's first installed record, no active protected release, exact integrated commit, SEND7 turn ownership and fresh post-rollback restoration. No gate is waived.

## Safety and files changed

Only this new `OUT/A0_ISOLATION_FOLLOWUP_SOURCE_CRITIC.md` was written. All root/lane source, frozen output and pre-existing changes were preserved. No Git mutation, commit, push, PR, reset, restore, generator write, source edit or cleanup occurred. Read-only public primary documentation/source and one Git branch lookup were the only network reads. No database, lab, probe, application/API/business, credentials, browser, Docker, TEST mutation, release, activation, deployment, Banking action/file/chat, payment/provider/settlement/bank/email/drain or LIVE access occurred. No secret value/file was read or printed. Policy X, OR-44 and owner rulings remain unchanged.
