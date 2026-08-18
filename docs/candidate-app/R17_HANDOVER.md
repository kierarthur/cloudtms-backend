# CloudTMS Candidate App — Candidate Daily Phase 3 R17 handover

Date: 18 August 2026
Disposition requested: independent review of an unpublished TEST-only source candidate
Publication/install authority: none until independent GO and separate explicit publication/install permission

## 1. Executive outcome

R17 is the bounded integration correction required by the independent R16 NO-GO. It does not replace the accepted R16 identity architecture. It changes the one existing database writer that had not adopted the R16 source-lock and exception contract:

```text
public.candidate_daily_authority_transition_atomic_v1(
  jsonb, uuid, text, jsonb, uuid, text, text, text
)
```

The effective R17 definition now:

1. derives every syntactically safe source identity from the complete transition batch before any Candidate-scope lock;
2. deduplicates and sorts those lock identities independently of caller item order;
3. acquires the exact R16 transaction advisory lock namespace first;
4. only then acquires Candidate scope rows in Candidate UUID order;
5. lets the R16 trigger harmlessly reacquire the already-owned transaction lock;
6. classifies `IDENTITY_LINK_CONFLICT` as an expected per-item rejection;
7. preserves valid siblings, the completed batch receipt and exact replay;
8. preserves malformed source-link input as indexed `VALIDATION_FAILED` without a pre-lock cast;
9. changes no Worker, HTTP route, Google source, frontend, schema, finance or Banking Pay owner.

## 2. Why R17 was required

R16 made source history correct at the table boundary but introduced a new expected exception and a new source lock into an older supported writer. The old writer still used:

```text
scope rows → source insert → trigger SOURCE lock
```

while generation used:

```text
SOURCE lock → scope row
```

It also rethrew the trigger's `IDENTITY_LINK_CONFLICT` because that error was absent from its expected item catalogue. R17 updates that one owner so every cross-writer path shares:

```text
all SOURCE locks, sorted
→ all Candidate scope rows, sorted
→ item-level work
```

## 3. Exact runtime implementation

### 3.1 Later effective repeatable

File:

```text
supabase/repeatable/18082026_1051_candidate_daily_authority_transition_source_identity_v1.sql
```

It is one complete transactional `CREATE OR REPLACE FUNCTION` definition. It begins with `BEGIN`, restores the exact intended ACLs and ends with `COMMIT`. It deliberately appears after the Phase 2 and R16 first-generation repeatables in every ordered chain, so it is the effective installed authority.

It preserves:

- all eight arguments and argument order;
- JSONB return type;
- `SECURITY DEFINER` and empty fixed search path;
- signed internal-context validation;
- exact batch request, idempotency and correlation identities;
- one durable batch receipt and terminal response hash;
- exact no-op, cutover, rollback and disposition rules;
- global feature, entitlement, independent approver, generation, cursor, reconciliation, projection, command, batch and effect barriers;
- per-item subtransactions and caller item indexes;
- service-role execute and public/anon/authenticated denial.

### 3.2 Safe source pre-lock scan

The pre-lock scan accepts only an item whose `source_link`:

- exists and is a JSON object;
- contains `identifier_hmac` and `hmac_key_version`;
- has a 64-character lowercase hexadecimal HMAC;
- has a positive decimal key-version string;
- is no more than ten digits and no greater than `2147483647` by text length/value checks.

No cast is performed by the pre-lock scan. Invalid items are skipped by the scan and later rejected inside the established item subtransaction.

For every safe source it derives:

```text
<environment>:SOURCE:<hmac_key_version>:<identifier_hmac>
```

then executes:

```sql
pg_advisory_xact_lock(hashtextextended(lock_identity, 0))
```

in sorted distinct order. This exactly matches the R16 history trigger namespace and hash seed.

### 3.3 Item containment

`IDENTITY_LINK_CONFLICT` is added only to the established expected item-error catalogue. The R16 trigger remains authoritative. When it rejects one item, the item subtransaction rolls back only that item's attempted mutation and appends:

```json
{
  "index": 1,
  "status": "REJECTED",
  "error_code": "IDENTITY_LINK_CONFLICT"
}
```

Valid siblings remain committed. The batch reaches `COMPLETED`, stores one terminal body and hash, and exact replay returns the stored result with `_idempotent_replay=true`.

## 4. Verification implementation

### Static/source contract

```text
tests/candidate-daily-phase3-r17-authority-transition-source-identity.test.js
```

It enforces the exact signature/transaction boundary, SOURCE-before-scope order and lock namespace, safe no-cast validation, item conflict catalogue, final ACL and workflow engine gate.

### SQL runtime

```text
tests/18082026_1105_candidate_daily_r17_authority_transition_source_identity_runtime_verification.sql
```

Inside one rolled-back fixture it proves:

- single conflict becomes indexed `REJECTED`;
- exact replay returns the same receipt/result;
- valid then conflict leaves the valid sibling committed;
- conflict then valid leaves the valid sibling committed;
- malformed source becomes indexed `VALIDATION_FAILED`;
- a no-source same-mode transition retains `NO_CHANGE`;
- final grants remain correct.

### Real two-connection concurrency

```text
tests/candidate-daily-r17-authority-transition-source-identity-concurrency.integration.js
```

The first test runs the actual generation RPC against the actual authority-transition RPC. A test-only trigger widens the exact historical deadlock window before the R16 trigger. Under the old order, transition could hold scope while waiting for SOURCE; under R17 it already owns SOURCE before scope. Both operations finish without `40P01`, timeout control flow or partial ownership.

The second test submits the same two source identities to two real transition batches in opposite item order. Deterministic pre-locking prevents a transition-versus-transition cycle. One batch commits the two source changes and the other durably returns two protected-history conflicts.

The fixtures are isolated and cleaned by transaction/schema teardown. They do not mutate TEST Supabase.

## 5. Test results

```text
Focused Candidate Daily JavaScript: 72 passed, 0 failed
Complete backend JavaScript:        686 passed, 0 failed

PostgreSQL 18.1:
  ordered Candidate runtime:        46 suites passed
  public-auth chain:                3/3
  mixed-version auth:               7/7
  authority-transition concurrency: 2/2
  first-generation concurrency:     1/1
  R16 identity concurrency:         3/3
  R17 source/transition races:       2/2

PostgreSQL 17.6:
  ordered Candidate runtime:        46 suites passed
  public-auth chain:                3/3
  mixed-version auth:               7/7
  authority-transition concurrency: 2/2
  first-generation concurrency:     1/1
  R16 identity concurrency:         3/3
  R17 source/transition races:       2/2
```

Docker is present but inaccessible to this restricted desktop process, so it is not counted as evidence. PostgreSQL 18.1 ran through a real isolated local cluster. PostgreSQL 17.6 was obtained from the official PostgreSQL Windows binary archive and ran through its own isolated local cluster. Both engines executed the identical saved R17 runner; the evidence does not reuse R16's older result.

## 6. Workflow and migration gate

The reusable Candidate database workflow now installs, in order:

```text
Phase 2 schema/RPC authority
R16 identity-integrity migration
R16 first-generation repeatable
R17 authority-transition repeatable
all Candidate runtime suites
all concurrency suites
```

Its matrix remains exactly:

```text
postgres:17.6
postgres:18.1
```

The TEST safe migration remains dependent on the reusable workflow. No mutating migration job can start while either engine is pending or failed for the same commit.

## 7. Exact R17 change boundary

### Runtime source

```text
supabase/repeatable/18082026_1051_candidate_daily_authority_transition_source_identity_v1.sql
```

### Tests and runner integration

```text
tests/18082026_1105_candidate_daily_r17_authority_transition_source_identity_runtime_verification.sql
tests/candidate-daily-phase3-r17-authority-transition-source-identity.test.js
tests/candidate-daily-r17-authority-transition-source-identity-concurrency.integration.js
tests/candidate-daily-r16-identity-integrity-concurrency.integration.js
tests/candidate-public-auth-postgres-chain.integration.js
tests/run-candidate-daily-r16-local-matrix.ps1
tests/run-candidate-daily-r17-local-matrix.ps1
.github/workflows/candidate-db-runtime.yml
```

The R16 concurrency expectation changes only because the now-correct supported transition returns a durable indexed conflict rather than rethrowing it. The auth-chain port parameter enables the same exact chain against an isolated local PostgreSQL port and does not change production runtime code.

### Decisions and assurance material

The Phase 3 implementation authority, decision matrix, installation runbook, diagnostic/rollback runbook, programme status, full Decisions PDF and this handover are updated through R17.

## 8. Deliberate no-change boundary

R17 does not change:

- the R16 normalized CID1 index;
- the R16 source-history unique index or trigger;
- the first-generation binder or generation RPC;
- Candidate Daily Worker adapters, route shapes or OpenAPI;
- Availability API source;
- Master Rota legacy source or CloudTMS helper;
- Candidate/Office frontend;
- Google versions, triggers, properties or secrets;
- Candidate creation, authentication, sessions or ordinary workflows;
- Emergency/provider execution;
- finance, invoices, Banking Pay, payments or Policy X;
- production.

It introduces no manual bootstrap, Candidate allowlist, fuzzy matching, automatic Candidate creation/deduplication, automatic feature enablement or automatic authority cutover.

## 9. Operational state and safety

```text
R17 committed:                 no
R17 pushed:                    no
R17 installed in Supabase:    no
R17 Worker deployed:          no
R17 Google source installed:  no
Availability bridge:          false
Master bridge:                false
TEST business data changed:   no
Production accessed:          no
```

No rollback is required because R17 has not been installed. Keep the active Google versions and both false bridge flags unchanged.

No R17 commit or push has occurred, and both Google bridge flags remain false.

## 10. Remaining exact gates

The source correction now passes the focused/full JavaScript gates and exact PostgreSQL 17.6/18.1 local matrices. It remains unpublished and uninstalled.

Before publication/install:

1. obtain an Independent review and R17 GO against this exact pack;
2. rebase/collision-check current backend `test` before any authorised push;
3. rerun both engines if any source changes during rebase;
4. inspect the exact staged diff and exclude unrelated/secrets material;
5. obtain explicit publication/install permission;
6. let the exact-commit 17.6/18.1 workflow finish before TEST migration;
7. verify installed definitions/indexes/trigger/ACLs read-only;
8. keep every Candidate flag and both Google bridges false;
9. obtain separate authority for Google installation and enabled coexistence proving.

## 11. Requested independent disposition

Review R17 as one bounded correction. A GO is justified only when source inspection and exact 17.6/18.1 evidence show that no supported blocker remains. That GO would mean the complete R16+R17 candidate is fit for later separately authorised TEST publication/install and disabled Google qualification. It would not be final Phase 3 acceptance, Phase 4 authority, bridge-enable authority or production authority.
