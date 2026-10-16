# Interim finite F23 type/cardinality peer review

No blocking source finding was identified in the three reviewed commits:

- `ef5468b5288e2d4ffef61edb20b703583fe2b66c`: finite tuple classification.
- `c29b4a6b7cfaf60deb888a740c3015a77aedc3b0`: two genuine invoice presentation singleton corrections.
- `24a2cabb9aaf31b91f8b9c671ba4a8dda3f9605f`: finite UUID cardinality classification.

This is an independent interim source peer review. It is **neither final PartB critic**, a full F23 pass, nor native/catalogue/security/effect/lifecycle/API acceptance. No SQL was executed. No product, root, Banking, Office or Candidate payload was edited. Candidate `f5668bf45854ca36b47e713c0e8f3dd3ae4b852b` remains the immutable joined checkpoint.

The source wrappers retain ordinary `assertSpecialConstructs` refusal. An approved tuple must match the exact path, complete LF source hash, signature, declaration-derived hazard, statement hash and occurrence count. Every actual tuple occurrence also passes the finite token proof. I independently parsed all 1,204 actual occurrences across 151 files with PostgreSQL 17 grammar: one `A_Star`, one direct schema-qualified `RangeVar` matching the declared row type, no CTE or set-operation source. There are 1,178 distinct file/key pairs and 91 native relations. None of the approved signatures is a `bpay_next` function.

Nineteen F23 statement keys include earlier PL/pgSQL control text and an earlier SELECT. The independent AST check locates the actual final SELECT before the first target INTO, removes only that INTO target/STRICT token span, then parses the complete remaining SELECT. Parsing the complete F23 diagnostic key as standalone SQL would incorrectly report those control prefixes as SQL grammar failures. No predicate or SQL tail is discarded.

The UUID classifier matches all 19 exact paths, 21 keys, whole function bodies, statement counts, 46 guard witnesses and the pinned review/correction/schema inputs. It classifies only `UUID_CARDINALITY_UNCLASSIFIED` in the exact reviewed signature. Foreign paths/signatures and other hazards receive no new allowance. The guard strings are witnesses inside a pinned complete body, not a general syntactic or control-flow theorem. The source review therefore remains necessary when a body or schema changes.

I checked the distinct guard families: winning-reference CASE; optional companion HAVING; source-ladder count refusals before use; zero/exact-one WAIT paths; staged WAIT refusal; complete resolved-client census and ambiguity refusal; correlated receipt Contract HAVING plus complete identity refusal. Nullable native columns are not treated as globally NOT NULL. The existing CASE/HAVING/absence/error behavior is preserved.

Both invoice-edit reader corrections contain exactly one identical insertion. Removing that insertion reproduces every predecessor LF byte. The guard follows genuine service/Office VIEW authorization and precedes movable-line construction. Its selected-invoice/CURRENT/presentation grouping covers the same bindings as the projected line UUID. Native `invoice_line_id` is NOT NULL. Counting **distinct line identities** accepts legitimate multiple movements for one line and refuses ambiguity with SQLSTATE `23514`; counting movements would be incorrect. Both readers are STABLE; the later adjustment-authority reader is also STABLE. No signature, owner, ACL, price, rounding, DTO or other effect was changed by these insertions.

The two focused suites passed 46/46 tests. I separately consumed every tuple file/key/count and reran all 1,204 AST checks, rather than relying on the focused suite's single tuple-file example. `verify-source.py` preserves those reproducible read-only checks and verifies the exact two insertion-only diffs. `REVIEW.json` pins the reviewed commit, critical source/evidence files and qualification limits. The reviewed root subsequently advanced to `8af28a98efe4594594997a068ea3d4c4f2de16fb` with only progress/plan/census documentation changes; none of this reviewed executable source changed.

Reproduce from the physical lane, supplying the root worktree and existing offline parser directory:

```powershell
python docs/healthroster-build/proposals/f23-finite-type-peer-review-20261016/verify-source.py --root "C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026" --parser-path C:/tmp/healthroster-v6-physical-parser
```

The checker refuses changed reviewed inputs before importing the reviewed gate helpers. It reads source/Git objects, parses SQL offline and runs only the two named source test files. It creates no database, schema, fixture, session, lock, network effect or deployment. A later changed owner/schema needs a new review; this receipt cannot be borrowed for it.

Actual full F23 remains held for the remaining dynamic and aggregate review. Native type binding, effect/ACL qualification, complete final contract, full rehearsal, lifecycle/concurrency/device acceptance and Install2 remain unproved by this report.
