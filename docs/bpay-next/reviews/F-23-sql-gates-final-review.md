# F-23 updated SQL gate source review

Read-only review, 10 October 2026. Reviewed full saved gate, full source-unit file and the actual 07102026_0018 routines. No code/gate/test/SQL execution or product edits. **Findings remain; source PASS is pending the bounded corrections below.** This is a source review, not an installation/test receipt.

Saved bytes read: gate `12f3723dab15b3046b2c4e1bd16343928e12b0d5888e0650ccbd870abb6249b8`; source units `b844e0f30169eff8e3d97152256f5c47d2f3f1ecd4c5c16b405591f8cc2e1c82`; initial-TSFIN SQL `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2`. Root may be changing the gate concurrently; a later verdict must name fresh bytes.

## Actual failing ROWTYPE:L is a parser boundary error

`private.bpay_next_initial_tsfin_bind_v1(uuid)` in 07102026_0018 has this real sequence:

```sql
for component in select value from jsonb_array_elements(cert->'components')
  order by value->>'component_id'
loop
  select x.* into strict l from private.bpay_next_approved_line x
    where x.revision_id=revision.id
      and x.source_component_id=(component->>'component_id')::uuid;
```

The SQL already uses the card-required alias.*. `specialConstructViolations` obtains a semicolon chunk containing both SELECTs, chooses the **first** SELECT and the later INTO, and consequently treats the FOR query/LOOP as part of the projection. Correct the checker, not this SQL or its Source owner.

Small fix: iterate executable INTO tokens and pair each whole-row target with the nearest preceding SELECT at the same SQL parenthesis depth after the latest relevant PL/pgSQL control boundary. The FOR query has no INTO; the nested body SELECT does. A nearest preceding SELECT fixes this exact case, while depth/control tracking also handles IF EXISTS(subquery) THEN SELECT and WITH/subquery projections correctly. Do not blindly ignore everything inside loops.

The same failure mode can hide a bad nested rowtype SELECT if the first SELECT happened to have a three-token alias.* projection. A source unit should cover an outer FOR alias.* query followed by bad `SELECT * INTO r`, not only the currently valid inner query.

## Remaining finite lexer/boundary gaps

1. **AS-body discovery searches text inside quoted literals.** `executableParts` uses `code.match(/(?:\bas\s+|^do\s+)(dollarTag)/)` after masking comments, rather than an outer executable AS token. A default parameter literal containing `AS $fake$ select 1; $fake$` can be selected before the real body, hiding a forbidden construct in the actual AS body. Locate the routine/DO body by quote/comment-aware outer token offsets; routineDefinitions already gives its canonical signature. Do not alter the managed release parser's opaque-body semantics.

2. **Valid body forms can silently escape checking.** `DO LANGUAGE plpgsql $$...$$` is not matched by the current `^do\s+dollarTag`, and `AS '...'` SQL/PLpgSQL bodies are not matched. Returning the unextracted outer statement leaves its BODY/QUOTED token opaque to D09. Support the finite forms or explicitly reject an unclassified routine/DO body; never report it checked. Single/dollar literals *within* an identified body remain opaque data.

3. **Nested dollar literals are not protected while comments are stripped.** sqlWithoutComments understands ordinary quotes, but not dollar literals; `--`, `/*` or a lone quote inside a nested dollar string can change its scanner state or cause an erroneous unclosed quote. Preserve identified body bytes until the body lexer distinguishes executable comments, identifiers and single/dollar literals. Quoted identifiers must remain distinct tokens, not be flattened into arbitrary SQL text (e.g. one `"pg_catalog.coalesce"` identifier must not become schema + function).

4. **New dynamic SQL is unchecked.** `EXECUTE 'select pg_catalog.coalesce(...)'` becomes EXECUTE + opaque QUOTED token with no hazard, even though it executes forbidden SQL. Retain literal values as data in lexer output and inspect a static EXECUTE SQL literal through the same finite checks. Any newly changed variable/constructed dynamic SQL that cannot be classified must fail explicitly rather than pass. Same-signature exact baseline statements can remain inherited. This is a local source-check seam, not permission to rewrite existing history/producer workflows or introduce a dynamic SQL framework.

These defects are source-visible; no demonstration code was executed by the reviewer.

## Remaining rule details

- `%ROWTYPE` declaration discovery currently recognizes only PRIVATE/PUBLIC-qualified declarations. The card says `%ROWTYPE`, so other qualified/unqualified declarations must be recognized or fail unclassified, not silently bypassed. Quoted identifier preservation is required here too. Whole-variable targets remain separate from scalar `v_row.field`, RECORD and RETURNING targets. Current real 0018 target l is correctly discovered; its projection pairing is the failing part.
- JSON frames now distinguish safe parenthesized extraction/function operands and are materially better than a line regex. They currently test only ->>/#>>. Ungrouped concatenation with ->/#> is also JSON extraction used before text extraction and must not silently pass; recognize all four JSON operators and require an extracted text operand/grouped completed result. Preserve valid jsonb object concatenation without raw extraction in its operator chain.
- MIN/MAX now fails closed for explicit UUID and unresolved argument types. It has no declared-local/column/literal type resolution or recognized cardinality proof yet, so it will reject valid new `min(bigintLocal)` and `min(x::text)::uuid` even when a genuine proof exists. This is conservative rather than a UUID admission hole. Add only finite type evidence needed for actual source (primitive casts/typed locals/simple resolved columns, safe scalar literals); do not infer from `_id`. Current changed corpus contains no executable MIN/MAX calls, so broader analysis is unnecessary. Retained baseline unknown aggregates are not new violations.

## Inherited exceptions and controlling reload

The new map materially corrects the initial same-file whole-routine exemption: signatures and hazards/declaration types are part of the key, baseline occurrence counts are consumed, and copied physical successors can retain the same statement. Preserve that design.

However, the key is currently raw text from the **first regex SELECT** to the semicolon, not exact executable statement tokens. It inherits the nested SELECT mismatch above, can select the word SELECT from a literal, and is sensitive to harmless formatting. Derive the selected statement span from executable token offsets, include literal values, and bind its target declaration/type as now. A modified statement, different signature, or excess occurrence must not consume another statement's allowance. Max baseline count per physical source is preferable to summing repeated retained copies; no broad path waiver should replace the map.

D10's tail correction is sound in the inspected bytes: only COMMIT may follow the required top-level notification, and latest controlling roots are expanded through readManagedClosure. Existing new/changed public RPC sources with their notify tails remain the right inputs. One finite coverage gap remains: candidate RPC selection iterates only repeatable `routines`; an added callable public routine defined only by a migration can escape D10. Inspect changed migration definitions too and require their genuine final controlling repeatable (or fail missing). Exclude actual trigger/event-trigger routines from PostgREST-callable candidates by their declared result kind; do not invent another RPC owner.

## Bounded next read

Root should correct the actual rowtype pairing and lexer/body/dynamic coverage seams, extend the same normal source-unit file with those finite cases, and re-read these bytes before a PASS verdict. No frozen file, Source producer, PIN/enroller or parked F-25 file change is justified by the approved_line failure. All product changes remain root-owned.
