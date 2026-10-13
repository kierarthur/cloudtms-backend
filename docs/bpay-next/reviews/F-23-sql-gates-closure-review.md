# F-23 SQL gate closure review

Read-only independent review, 10 October 2026. Full current gate and source-unit file read. No code/test/gate/SQL execution, runtime calls or product writes. **Three bounded D09 source findings remain; closing PASS is pending their correction.** No existing locked 2153 or Source/PIN/producer SQL change is requested.

| Reviewed source | SHA-256 |
| --- | --- |
| scripts/bpay-next-source-gates.mjs | `fda6210ec452d97592ce5c8e5342dd2c5794b661f95ee491e1e28efae51c489f` |
| tests/bpay-next-source-gates.test.js | `0bce01aeec7d953d2cd680d55f7fae9272de4bff0d350f2e221b2e527802a691` |

## Earlier findings closed in these bytes

- The genuine 0018 `approved_line` SELECT now pairs with the inner SELECT nearest its INTO and binds x to the actual declared relation. Its SQL already uses x.*; no business-source edit was warranted.
- Routine AS discovery now skips parameter/default strings and nested comments; the new normal unit explicitly covers the fake AS/dollar default before the actual forbidden body.
- DO LANGUAGE's dollar body is inspected. Unsupported single-quoted/unknown routine/DO bodies produce BODY_UNCLASSIFIED rather than silently pass.
- Nested dollar literals are preserved by comment stripping; their contents no longer change quote/comment state. Unclassified opaque body literals/dynamic SQL fail conservatively.
- Newly changed EXECUTE statements fail closed except finite fixed owner-policy/private-schema templates. The existing locked 2153 format loop remains a finite schema-only template; this is not authority to alter or activate its parked F25 business protocol.
- JSON chains recognize ->/->>/#>/#>>, grouped completed operands and function operands. Qualified special forms remain checked in executable tokens, and MIN/MAX UUID/unclassified types are refused.
- Same-signature hazard/declaration keys and consumed counts prevent cross-function borrowing and excess current occurrences. Baseline file maxima avoid summing retained copies.
- D10 covers changed public migration definitions, requires an actual final controlling repeatable, expands canonical closure order, and rejects substantive work after the final required NOTIFY. No new RPC owner was invented.

## 1. Inherited keys discard changed unsafe code before SELECT

The final key still uses `raw.search(/\bselect\b/i)` and slices from that first SELECT for **every hazard**, despite the token-selected nearest SELECT used by the rowtype check. This is an actual exception admission hole.

Consider same-signature baseline and changed routine bodies:

```sql
-- baseline statement
if 'prefix:' || j->>'old'='x' then select 1;
-- changed statement
if 'prefix:' || j->>'new'='x' then select 1;
```

Both have a JSON_CONCAT hazard, but both keys retain only `select 1;`. The changed unsafe extraction therefore consumes the old allowance although the executable statement changed. The same issue applies to qualified special forms, aggregate calls or dynamic code appearing in a condition before a SELECT. A literal's word SELECT can also supply the wrong raw offset.

**Small correction:** for non-ROWTYPE hazards bind the full executable statement/condition tokens with literal values. Only a ROWTYPE-specific key should use the correctly paired SELECT statement span, derived from executable token offsets/control scope rather than a raw regex. Preserve signature, declaration/type and occurrence count. The normal source unit should assert that the changed pre-SELECT condition cannot inherit its predecessor's allowance. No SQL owner rewrite is required.

## 2. Flattened quoted identifiers can bypass rowtype target discovery

The helper still removes double quotes into arbitrary text before D09 tokenization. A valid quoted variable name containing a space demonstrates the problem:

```sql
declare "v x" private.example%rowtype;
begin
  select * into "v x" from private.example;
```

After flattening, declaration matching records X; target tokenization sees V. The whole-row target is not found and the bad new SELECT can escape the ROWTYPE guard. An identifier containing a dot can similarly be confused with qualification rather than one identifier.

**Small correction:** preserve quoted identifier identity in the D09 lexer, or fail unsupported quoted identifier shapes before flattening. Ordinary quoted single identifiers may be normalized only as one token; do not silently split their contents into SQL identifiers/operators. Existing simple quoted C collation and literal strings are not a reason for unrelated product changes. Add a normal source refusal unit for the quoted whole-row target.

## 3. Typed-local inference scans DDL/expressions, not declarations

The `types` regex scans the joined tokens of the entire routine body. It can overwrite a declared local's real UUID type from a later same-spelled DDL column:

```sql
declare u uuid;
begin
  create temp table unrelated_type_example(u bigint);
  select min(u);
```

The map's later U/BIGINT match supersedes U/UUID, so min(u) can pass as bigint even though u is the UUID local (the aggregate SELECT has no FROM). This is source reasoning only; the reviewer executed no example or temporary DDL.

**Small correction:** derive locals from actual DECLARE sections and parameters, not arbitrary body token adjacency. Unsupported nested/shadowed declaration scopes or aggregate column types remain unclassified/refused. The current changed source has no executable MIN/MAX calls, so no larger type system or SQL-source edit is needed. Add one finite source unit for declaration type surviving an unrelated DDL column.

## Review limit and next verdict

The implementation is now materially closer to the exact D09/D10 requirements. Its safe fail-closed behavior for unclassified dynamic forms/types is appropriate for the current finite corpus; it is not a general PostgreSQL parser. The three findings above concern silent admission of unmatched unsafe new code, so they require gate corrections before a source PASS. They do not justify edits to frozen files, locked migrations, producer/enroller/PIN owners or the parked F25 protocol.

Source units were read but not run by this reviewer. Root owns fixes and authorized normal gate execution. Fresh source hashes are required when closing these findings.
