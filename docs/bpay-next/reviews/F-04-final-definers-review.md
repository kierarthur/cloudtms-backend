# F-04 final-definer source review

Independent review, 9 October 2026, by the Sol 6.1 Extra High review subagent. Authority: the approved R3 start sheet, F-04 card/master chain table and BUILD-BE operating instructions. Reviewed saved BUILD-BE source only. No tests, SQL, database calls, Docker, fixtures, rehearsals or runtime checks were executed by this reviewer; no product source was edited.

## Verdict

**Changes required for the F-04 source gate.** The partial foundation is correctly reported as incomplete: `foundationComplete: false` preserves the outstanding F-06 1157/1301 imports and F-07 ordinary case successor. The registry and design notes do not claim those future bodies are installed. The local selector preserves canonical inventory ordering and expands registered final closures to a fixed point before its final source check. The present gate has the following actionable holes.

### FD-01 — P2: equivalent PostgreSQL type spellings bypass one signature identity

`scripts/bpay-next-final-definers.mjs:8–36` only normalizes selected unqualified aliases. A definition with argument `integer` produces a different key from a definition with argument `pg_catalog.int4`, although both replace the same PostgreSQL routine. The qualified spelling is accepted, rather than refused. Likewise `float4`/`real`, `char`/`character`, and `time`/`time without time zone` are not canonicalized. A newly added competing physical source using such a spelling can have a distinct, unregistered, single-root key, so lines 127–132 will allow it while the registered authority still appears last under the other key. The named-argument heuristic also strips an unnamed unqualified composite array's type token (`custom_type[]`) and can return an invalid `[]` identity instead of failing closed.

Use one canonical input-type identity for supported PostgreSQL aliases and their `pg_catalog` qualifications; refuse ambiguous or unsupported parameter syntax instead of guessing. Preserve INOUT/VARIADIC input identity and OUT exclusion. Review cases are source strings only, not database fixtures.

### FD-02 — P2: closure file order is mistaken for executable definition order

`definerInventory()` at `scripts/bpay-next-final-definers.mjs:75–81` scans every complete parent file before its included children. `readManagedClosure().ordered` is the canonical **file/hash preorder**, initialized with the entire parent at `scripts/cloudtms-managed-release-sql.mjs:118` and followed by children at line 125; it is not statement execution order. For a root containing `\\ir predecessor.sql` followed by its own final `CREATE OR REPLACE FUNCTION`, actual SQL installs the child first and the parent final last, but the census records the opposite. With definitions on both sides of an include, a wrong parent final can also appear valid while the included predecessor actually wins.

Traverse statements and include offsets in actual execution order, retaining each definition's physical source provenance. Keep the existing canonical closure hash unchanged. A source-only review case with an include before/after a same-signature parent definition is sufficient to distinguish these orders. This affects the promised recursive-root guarantee even where the present baseline happens not to collide within a parent/child pair.

### FD-03 — P2: protected source gate omits F-04, and NEW validation follows bootstrap DDL

`tests/bpay-next-final-definers.test.js` is reached through general `npm test`, but the protected `database-release.yml` source-gate job runs only the two named database-release/routing test files at lines 66–67. No other source-gate command imports the F-04 verifier. The new managed check at `scripts/cloudtms-managed-release.mjs:388` runs after the NEW branch has already executed and committed the bootstrap. Thus an invalid selected definer set can durably bootstrap a blank database before rejection, contrary to the design's source validation before the first DDL.

Add the exact F-04 static gate to the protected source job. For NEW, calculate its selected repeatable roots against the pinned baseline lock and validate those roots before executing bootstrap; retain the ledger-derived check before any pending units for resumed NEW and UPGRADE. Do not add runtime or financial fixtures.

### FD-04 — P2: merge HEAD and release-range changes can escape changed-path admission

`changedSourcePaths()` at `scripts/bpay-next-final-definers.mjs:95` uses `git diff-tree ... HEAD` without a merge-diff mode or an explicit first-parent comparison. At a merge HEAD this does not provide the normal predecessor changes that the gate needs. The optional exact ancestor comparison is sound when supplied, but neither source workflow currently supplies `CLOUDTMS_FINAL_DEFINER_BASE`. A clean committed merge that changes an earlier physical definer while leaving its final root unchanged can therefore pass the static changed-only check; the later installed-ledger check may reject only during release.

Compare merge HEAD explicitly with its first parent, and wire the reviewed release/CI comparison base into the source gate where applicable. Preserve the working-tree and untracked SQL inputs. Distinguish the current HEAD diff from the entire intended release range; a current-commit-only check is not release-range evidence.

### FD-05 — P2: selected migrations can redefine a protected repeatable without requiring its final closure

`definerInventory()` at `scripts/bpay-next-final-definers.mjs:72` scans only repeatable roots; both caller checks pass only selected repeatables. The managed and local loaders apply selected migrations before repeatables. Consequently a new migration containing `CREATE OR REPLACE FUNCTION` for a registered repeatable signature is absent from this census and does not require its unchanged final repeatable to be selected. On a changed-only load, that migration body can become the final installed definition while the F-04 check passes. Current migrations already contain routine definitions, so this source category cannot simply be assumed impossible.

Classify routine definitions in the actual selected migration envelope too, requiring the controlling repeatable when a migration replaces its signature, or fail closed for an unapproved migration definer. Preserve historical migration immutability and the baseline's exact receipts. This finding does not request rewriting historical migrations.

## Metadata and notes

The registry currently records 685 chains from the 2,009-signature census, covering the declared 468 signatures with multiple repeatable roots and all current NEXT single-root definitions. The 37 master metadata rows retain the 35 ordered editor chains plus the grouped single-editor and intake rows. `case-allocation-chain.md` retains the controlling F-25 through CM-13 succession, the WB-27 STABLE write-free allocation-plan requirement, shared instruction derivation, and accepted-draft freeze boundary. `intake-successors.md` preserves command/page kinds and each item's dependency order. Both start with an empty branches-added list, appropriate to this foundation batch.

Existing 0557 source owner completion remains designated final for first-authorisation and current-lock inventory. The ordinary case page, shared target valuer, and receipt-binding authorities remain pending their exact F-07/F-06 work; do not mark the corresponding F-04 acceptance rows DONE yet. Source review cannot establish installation, read-back, protected verification, or app acceptance.

## Follow-up review of FD-01 through FD-05 fixes

**PASS for the implemented source-gate foundation, 9 October 2026.** This verdict supersedes the initial changes-required verdict for the five findings above. Re-read the saved gate, static gate entry point, registry, managed/local callers and both database workflow definitions immediately before this follow-up. No tests, SQL or runtime checks were executed by this reviewer.

- **FD-01 closed:** Supported aliases, including qualified `pg_catalog` spellings, now share the canonical input identity. The internal `pg_catalog.char` type is explicitly refused rather than conflated with SQL `char`. Pinned baseline public enums/table composites normalize to their qualified public identity; unknown unqualified types fail closed. The final reread includes the additional rejection of `ARRAY` and bounded bracket syntax, and exact canonical-type admission rather than a first-token test. Thus the reviewed alternate array forms and interval field modifiers cannot silently create another signature key. Empty repeated brackets retain the PostgreSQL array identity; OUT-only parameters are excluded and input modes remain preserved.
- **FD-02 closed:** Definition traversal merges statement/include events by source offset and recursively inserts each included definition at the actual include position, retaining physical-file provenance. The independent existing file preorder and canonical closure-hash implementation remain intact.
- **FD-03 closed:** The protected release source job now has full Git history and runs the exact F-04 static gate before admitting a release commit. Both managed NEW and local NEW calculate the post-baseline envelope, close its final-definer selection, validate it and compile every selected unit before executing bootstrap. Resumed NEW/UPGRADE and local changed-only loads close the ledger-derived envelope before pending units.
- **FD-04 closed:** HEAD changes include first-parent merge diffs; tracked working-tree and untracked SQL inputs remain included. The registry's exact `releaseBaseCommit` supplies the intended build range by default, is checked as an ancestor, and is explicitly passed by the protected workflow. The ordinary source workflow's full-history `npm test` also reaches this default range-aware gate.
- **FD-05 closed:** Migration closures are classified with the same execution-order parser. Selected migration definitions that have a controlling repeatable add that final root to the selected set; the verifier independently requires it. Shared fixed-point closure selection then includes finals required by any newly selected predecessor and filters the result through canonical inventory order. Both production managed and isolated local callers consume this same selection helper.

The registry now has **686** exact entries and **37** master editor rows. The added `public.enqueue_ts_financials(uuid,public.ts_fin_reason_enum)` entry accounts for the newly unified audited composite/enum identity and preserves its existing final included closure. `releaseBaseCommit` is `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. `foundationComplete` remains false; F-04 stays PART_DONE until the designated F-06 imports and F-07 ordinary successor complete their acceptance rows. No new actionable issue was found in this fix batch.
