# PS-01 catalog assertion source-gate review

10 October 2026. **PASS — finite classification finding closed in fresh source.** Read the small `catalogAssertion` delta, its surrounding lexer/statement classification, the existing new source controls and actual 0533 predecessor assertion. No tests, source gates, SQL or product runtime were executed; only this review file was written.

The intended exception is appropriate: the actual migration compares `pg_catalog.pg_get_constraintdef(c.oid)` from `pg_catalog.pg_constraint`, scoped to one literal private relation and one literal constraint name, with a dollar-quoted metadata value. That nested metadata is data; it is not an executable SQL body or an EXECUTE producer. The F-23 lexer already represents it as `<BODY>` and protects its embedded semicolons/quotes. This adjustment fixes the observed false positive without needing a product migration rewrite.

## Resolved initial finding: multiple dollar literals could borrow the exception

The initial anchored raw regex contained a backtracking `\$tag\$[\s\S]*?\$tag\$` capture without a one-literal span requirement. This complete RHS could therefore match by treating the last closing tag as the metadata end:

```sql
is distinct from $expected$CHECK$expected$
  || private.some_function() || $expected$tail$expected$
then raise exception using errcode='23514',
  message='BPAY_NEXT_PREDECESSOR_MISMATCH';
```

The real lexer sees two `<BODY>` tokens and an outside function/concatenation expression, while `catalogAssertion` sees a seemingly closed literal. `!dynamic` does not close this case because there is no EXECUTE token. This violates the promised finite one-metadata-literal shape, even though the authentic PS-01 assertion is safe.

Smallest correction: require exactly one scanner `<BODY>` token in this assertion statement, retaining the current anchored raw form and `!dynamic`, or require the equivalent exact scanner-token skeleton. This is a boundary correction, not a PL/pgSQL parser. Add the same-tag concatenated-literal negative to the existing normal control. Do not broadly exempt dollar literals or concatenated assertion values.

Root was notified immediately with the exact source expression and minimal correction.

## Other boundaries and controls

The raw form is otherwise bounded to the fixed catalog function/table, simple private relation and constraint literals, `IS DISTINCT FROM`, a fixed `23514`/`BPAY_NEXT_*` raise and an anchored statement tail. Multiline whitespace is supported. Semicolons, different dollar tags, quoted comment markers or SQL-looking text **inside one genuine expected literal** remain data. An actual statement after that literal is independently scanned; appending an EXECUTE after the raise's semicolon remains unclassified. An EXECUTE inserted before the raise loses the assertion shape/`!dynamic`, and a dollar assignment plus EXECUTE remains refused.

The new source controls cover the actual metadata positive, subsequent separate EXECUTE, EXECUTE in the assertion branch, dollar assignment/execution and now the exact multiple same-tag literal escape above. Useful finite positive coverage is the real multiline expected constraint text containing quotes/parentheses/semicolons; this does not require executing migration SQL. Other D09 hazards and D10 controlling RPC handling remain untouched by this delta.

## Fresh closure

Freshly read the saved assertion and all its new normal source controls. `catalogAssertion` now requires `t.filter(token=>token==='<BODY>').length===1` as well as `!dynamic` and the existing anchored finite raw form. Two actual same-tag literals cannot borrow the metadata exemption even if regex backtracking finds a later delimiter. The new refusal control constructs the exact concatenated function RHS using a replacement callback, preserving literal dollar tags rather than JavaScript replacement expansions. The prior EXECUTE insertion also uses a callback and remains a genuine negative input. One real metadata literal remains data, while outside EXECUTE/assignment/unclassified body statements are still independently refused. No remaining actionable finding in this small delta.

Reviewed SHA-256:

| File | SHA-256 |
|---|---|
| `scripts/bpay-next-source-gates.mjs` | `65a6ff58a8919cc443518da1a4bb24c895b3a41a5245b988f581333696b4afb8` |
| `tests/bpay-next-source-gates.test.js` | `3a8608c6d714eb28489c5b165b36f3f559bc6da2cd4bb52b63948d0151ec069b` |

This review does not claim gate execution or general SQL parsing correctness.
