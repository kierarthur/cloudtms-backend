# QT two top-level audit digests — source-only structural candidate

The current literal PL/pgSQL EXECUTE already requests a parallel-capable plan
with an unlimited executor tuple count. Primary PG17 code supplies no blanket
ban based on DO or earlier DDL. R4's nested plan and reason for missing the
30-second deadline remain unknown; this proposal does not diagnose serial SPI
execution or promise speed.

PG17's [CTAS executor](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/commands/createas.c#L280-L304)
plans its contained SELECT with parallel eligibility, runs it to completion,
uses a table destination, and emits the SELECT completion tag with processed
count. Its [parallel-query documentation](https://www.postgresql.org/docs/17/when-can-parallel-query-be-used.html)
explicitly permits a parallel SELECT for CREATE TABLE AS. Actual workers, chosen
plan and timing remain unproved. Prior DDL does not itself establish any cause.

Six complete postimages bind root parent
`267f4e2c5fe30b02cbe69ca143f17d50dd89e960`: QT assertion source, input response
builder, result validator, sealed programmes, sealed offline inputs, and adapter
manifest. All 46 other adapter manifest entries remain exact. There are eight
literal reversible code hunks and complete old raw files. No root file was
edited by the author.

The one server-owned outer B/rollback is unchanged. The new fixed sequence is:

1. Original result temporary table; separate metadata DO containing the exact
   existing event-sequence index/column guard and two fixed temp-name absence
   checks. It precedes either full digest and every user fixture.
2. TEMP CTAS `hr_contact_qt_audit_before`, ON COMMIT DROP, storing the unchanged
   all-row/all-column digest as one native text value.
3. Original fixture/control DO with only the moved guard and two aggregate sites
   removed. All remaining declarations, mutations, assertions, exception checks,
   real counts and result construction remain byte-preserved.
4. TEMP CTAS `hr_contact_qt_audit_after`, ON COMMIT DROP, using the identical full
   SELECT. A final native DO requires one non-null 32-hex digest row in each
   fresh table and compares them with IS DISTINCT FROM, raising the original
   `QT_UNRELATED_SETTINGS_OR_GLOBAL_AUDIT_CHANGED` on difference.
5. Original `programme_result` SELECT occurs only after that final equality.

The all-row/whole-row `to_jsonb(a)::text`, both MD5 operations, delimiter,
coalesce, exact bigint order and equality are unchanged from the accepted
sequence-order parent. There is no partial scan, sampling, partitioning,
JavaScript hash/value comparison, client interpolation or new financial oracle.
The two digest values stay inside B's temporary tables and are not returned.
Every temporary object is inside the same server-owned rollback.

Observation timing necessarily changes: the before audit statement now precedes
the original global/default reads, all still before fixtures. The after audit
statement follows the original tail. That tail contains already existing
read-only ACL/role/user-count checks **and the existing temporary result INSERT**;
it has no additional business write. The temporary result table is created plain
in this programme; the retained native preflight excludes event triggers and the
programme adds no rule or trigger to it. The audit fence therefore includes that
control write rather than silently claiming the entire tail is read-only.

QT programme statements change 51→55; main input changes 64→68. Only statements
60 and 62 are the two fixed CTAS commands. PG17 command completion plus the
previously reviewed local pg 8.23 parser support a **source expectation** of
command SELECT, rowCount 1, empty fields/rows, not truncated. The current deployed
CTAS ABI has not been observed. Only those exact two SQL hashes receive a
separate `FIXED_AUDIT_CTAS_ONE_STORED_ROW` shape. Any mismatch HOLDs; ordinary
one-row SELECT and empty-command checks remain strict. No broad CREATE/SELECT
waiver or fabricated row count is added. Local provider source is not labelled a
deployed-worker binding.

The full constructor case, native expected QT result, fixture IDs, original
verification SQL, fresh absence, all 12 canonical scopes, original WeakMap,
shared A, callback phases and 30/45/70/120/180 caps are unchanged. Performance
must fit those existing bounds; the source makes no new native qualification.

Non-writing review from this folder:

```powershell
node --test test.mjs
python pg17-proof.py
```

Tests cover six full inverses, exact two command shapes and refusals, response
indices/counts, original constructor frame compatibility, all unchanged fields
and scope/cap records. The PG17 parser proves both CTAS inner SELECT ASTs equal
the parent full digest, the main PL body transformation (including the removed
compiler-created INTO row datum and renumbered exception-only datums), guard
ordering, result ordering and complete programme/input grammar. This is grammar
and source proof, not native compilation or performance.

Source research also checks [PL/pgSQL's dynamic execution](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/pl/plpgsql/src/pl_exec.c#L4190-L4199),
[SPI's plan options](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/executor/spi.c#L598-L625),
[planner eligibility](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/optimizer/plan/planner.c#L348-L363)
and [executor tuple-count behavior](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/executor/execMain.c#L1581-L1594).
These are major-version PG17 source references, not a claim that an independently
observed top-level plan is the actual nested SPI plan.

Author execution: offline parsing/tests and public source reads only; zero SQL,
EXPLAIN, native, lock, auth, API, root edit or installed-function execution. Root
and its independent peer must review before adoption and any actual run. All
earlier failures and accepted source packets remain unchanged.
