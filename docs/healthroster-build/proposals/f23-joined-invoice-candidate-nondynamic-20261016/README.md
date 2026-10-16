# Joined Invoice/Candidate finite non-dynamic F23 review

This reviews exactly the 38 non-dynamic diagnostics in root commit `c1ebb691feb6a11324060e0c28210e2bd735624a`. It supplies 37 finite extension points and one complete Invoice grouping correction. It does not admit an entire function or file, modify a generic fence, or qualify native behavior.

The input census is `.codex-tmp/f23-current-source-review-20261016/CENSUS.json`: file SHA256 `509162ee4b70e223473b17e065ec6d3ac84c8c9cf8e7fef300ce5fcbe17346a6`; its diagnostic digest is `79bb8909d774f93551b3f53b6afa3d09176a9eb4d264cd9337e44e82649ca8d1`. The other 99 dynamic diagnostics belong to a separate review.

| Scope | Exact result |
| --- | --- |
| 26 declared-rowtype statements | One whole `*` target from one schema-qualified relation exactly matching the declared `%rowtype`; no top-level join, CTE or set operation. Native primary/unique keys bound point reads. The two financial reads retain their existing ordered `LIMIT 1`; that limit does not prove uniqueness of current financial authority. |
| 7 aggregate statements / 64 MIN/MAX inputs | Current CTE/table/function bindings prove scalar text, numeric, integer or date types. 59 expressions equal older reviewed expressions; every current binding is checked separately. Five fresh bindings are display week date, integer blocker order, earliest/latest summary week dates, and JSON text segment reference. |
| 4 UUID statements / 7 casts | Exact count/refusal guards establish the selected UUID cardinality. Invoice shift matching uses count=1 or NULL with MISSING/AMBIGUOUS; companion selection uses HAVING count=1; placement refuses pending/ambiguous/archived-unbound identities; Candidate anchors preserve their exact count guards and actual H103 alias resolution. |
| 1 JSON expression | Explicitly group the extraction operand in the complete Daily actual-revision assertion. No JSON exception is proposed. |

`REVIEW.json` contains every exact statement, current binding proof, schema key evidence, 64 scalar inputs, and bounded UUID evidence. `PROVENANCE.json` independently reads the original producer commits and proves all 16 complete current functions equal those producer functions. Five signatures have older reviewed counterparts, but none of those whole functions is equal to its current version. Fourteen diagnostic keys are exactly equal to old reviewed keys; this is comparison evidence only. The remaining points receive fresh finite review. Current reference/VAT/H103 declarations and whole-function hashes are separately pinned as binding dependencies.

The native contract JSON does not contain the additive self-bill-report table. Its rowtype/key proof therefore uses the exact `13102026_1344_weekly_source_self_bill_report_schema.sql` CREATE statement and PostgreSQL 17 AST, not a fabricated contract entry. Alias cardinality also pins the exact current physical-schema UUID primary key. Candidate display joins may repeat one UUID; no unsupported uniqueness assumption is made for `contract_weeks.timesheet_id`.

The only SQL correction is `forward/01_complete_invoice_daily_actual_revision_assert.sqlinc`. It changes `refs:=snap->'rate_source_refs_json'||` to `refs:=(snap->'rate_source_refs_json')||`. Reversing that single change reproduces the entire predecessor function. PostgreSQL 17 gives both expressions the same AST after source locations are removed: this is an explicit grouping correction required by the source fence, not a demonstrated runtime-value defect. Existing ACL/owner statements must remain current.

`CORRECTION_RECEIPT.json` pins the complete successor SHA256 `420085e76ac9611551d489e3eae468874e89f15d2b7d8056ce9a246a083887f2` and the exact joined Invoice file after that one replacement, `e03acb39db8cc2441cde46125ad9b38e8594b3790c772b247cea76a052cb2044`. Mounting a later function alone leaves the earlier lexical JSON diagnostic in the source census; root must apply the exact correction to the selected current leaf or otherwise reconcile that diagnostic explicitly. No Banking body or financial ABI is changed.

`EXTENSION_MAP.json` pins the original/corrected files, complete functions, bodies, exact keys/statements and occurrence counts for only the 37 eligible points. A changed source, declaration, query, guard, count or pin requires fresh review. Root can combine this input with its separately reviewed dynamic map while keeping every generic rejection fence intact.

Offline reproduction from this proposal directory uses the existing root checkout as a Git-object source. `discover.mjs` additionally requires the saved joined leaves to equal the frozen commit; do not rerun it against later edited saved leaves and treat their contents as this review. The other tools read exact committed objects:

```powershell
node discover.mjs <root-I2-path>
python review.py <root-I2-path>
node provenance.mjs <root-I2-path>
node extension-map.mjs <root-I2-path>
python verify.py <root-I2-path>
```

The grammar checks use the already available local pglast PostgreSQL 17 parser at `C:/tmp/healthroster-v6-invoice-parser-20261016`; no database is opened. `CHECKS.json` records source/provenance/type/cardinality/grammar checks. No root file, database, deployment, browser, API, phone journey or Banking producer was changed or exercised. Runtime/native acceptance remains `NOT_RUN`, including real currentness, permissions, money calculations, Invoice/Candidate journeys and prerequisite closure.
