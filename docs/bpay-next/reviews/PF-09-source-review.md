# PF-09 D01 independent source review

10 October 2026, BUILD-BE. Current verdict: **PASS for the finite D01 source-check scope** after the follow-up below. Initial verdict was CHANGES_REQUIRED at the original hashes, retained here for provenance. The product financial owners are unchanged. Source review only: no tests/gates, SQL, runtime, business/data/hosted or timing operations were executed, and no product source was edited.

| Reviewed source | SHA-256 |
| --- | --- |
| `scripts/bpay-next-source-gates.mjs` | `42fc85618c3f41fc8df91bfbb710e06fd16ca25fce7b6a096992e508e9dd9d05` |
| `tests/bpay-next-source-gates.test.js` | `1d68469b7b13b139193740cfc855ad667cb0bfeb13ebd3a1a237a08c420c6ab9` |
| `docs/bpay-next/design/PF-09-projected-pages.json` | `e49a40894b0076784e9a23e33623a4958edc13688eee5f9f927378a2bd169a52` |
| `docs/bpay-next/design/PF-09-SCOPE.md` | `473fb49edfd83e8b3c00a071f76572ebbdafca588ae8dd15090da6dd3857c0ff` |

## Initial findings — closed in follow-up

**PT01 — Named constructors do not enforce reply keys or reject a whole-row operand.** At gate lines 320–334 the serializer scan covers `to_json[b]`, `row_to_json`, aggregates and arrays, but omits `json[b]_build_object` operand checks. It also never uses the finite `literalKeyConstructors`/explicit projection evidence saved in the census. A valid successor of the current public preparation COMPONENTS branch can append `,'extra_financial',l` to its existing `jsonb_build_object` while keeping every current money cast. PostgreSQL converts the complete approved-line composite `l` into JSON; neither the opaque-row scan nor the known-money-key scan recognizes it. Adding an ordinary unknown scalar key likewise passes this assertion. The current Worker correctly refuses the changed reply later, but that does not satisfy the requested reply-key source gate. Fix narrowly using each named constructor's finite key/operand projection, with multiplicity, and reject new keys or base-record operands. Keep the existing internal row lock positive control.

**PT02 — An incidental text cast does not prove the transported operand is text.** Gate lines 340–354 accept a money expression containing any `::text` or `->>`. For example `jsonb_build_object('cash_amount',(v_row.cash_amount::text)::numeric)` passes this source assertion while emitting a JSON number. A CASE with one text/NULL branch and a numeric result cast has the same issue. The check must protect the final money operand's retained text expression, not the presence of a substring. The finite existing projection/constructor operands provide that evidence without new SQL type inference. Add one negative text-then-numeric source-string case alongside the existing missing-cast case.

**PT03 — Static NEXT float DDL in a DO body is skipped.** `assertNextExactNumericTypes` at lines 230–254 scans outer managed statements only. The existing scanner deliberately treats dollar bodies as opaque, so `DO $$BEGIN ALTER TABLE private.bpay_next_amount ADD COLUMN amount float8; END$$;` is ignored: its outer token is DO, not ALTER. Existing special-construct controls reject dynamic SQL but do not reject this static ALTER. Reuse the gate's existing executable-body extraction for this check and recognize CREATE/ALTER inside BEGIN bodies, retaining the direct declaration tests and comment/row-lock positives. This is a finite traversal correction, not a new parser.

**PT04 — Projection comparison normalizes literal values.** `parts[0].text.toLowerCase()` at line 315 changes literal contents before comparison. Thus changing the actual current-work projection's `w.approval_state='APPROVED'` to `'approved'` preserves the approved gate prefix although it changes `position_ready` against the stored uppercase state. The census explicitly provides literal-preserving `approvedWhitespacePrefix` values for this reason. Preserve string/quoted-identifier contents when normalizing SQL keywords/whitespace, or compare the finite reviewed literal projection directly. Do not treat arbitrary literal edits as whitespace-only equivalence.

## Retained correct scope

The gate resolves actual current final definitions for all 19 listed signatures, not only changed files, and preserves legitimate private `%ROWTYPE`/point-key SELECT guards. The finite derived-page constants reject the currently tested star, extra derived field and removed decimal cast. The typed BREAK return/query checks preserve the actual helper used by frozen detail. Direct NEXT CREATE/ADD/ALTER declarations recognize real/float/float4/float8/double precision, including pg_catalog qualification, while prose and private row locks remain allowed. The existing Worker command/preparation/history contracts retain closed schemas, bounded text and decimal-string/BigInt transport; no monetary owner change is required by these findings.

The two added normal source-unit cases preserve prior cases. Root reports 7/7 passing; the reviewer did not run them. Their current negative set does not include PT01's named extra/composite operand, PT02's cast-back expression, PT03's static DO DDL or PT04's literal-case change. These can be added within the same existing cases. Future app/joined penny agreement and large/negative/zero transport remain NOT_RUN.

## Fresh-source follow-up — PASS

Re-read the corrected saved source and tests on 10 October 2026. Gate SHA-256: `553d3492c28174ceca2188562bc6a2af6db893679cdcd774323a97cd3b6fade1`. Source-unit SHA-256: `d36900886356b7c435fe046d77d28f427fa3205819c88cfc65b4461e68934c93`. The original hashes above identify the initial reviewed state.

| Finding | Corrected source boundary | Result |
| --- | --- | --- |
| PT01 | All 19 selected final signatures have an explicit expected constructor list. Actual `json[b]_build_object` clauses are compared with exact case-preserving clauses in source order, including their multiplicity. The 77 retained clauses preserve named operands, not merely the presence of a legitimate key. The demonstrated extra/composite operand changes a clause and is refused. | CLOSED |
| PT02 | The final money expression must end in retained text encoding, text extraction or literal NULL. An earlier text cast followed by `::numeric` no longer qualifies; actual finite named constructor/page operands remain independently protected. | CLOSED |
| PT03 | Numeric declaration checking traverses the existing executable-body extraction and finds CREATE/ALTER tokens inside static BEGIN/DO bodies. The previously skipped static ALTER is inspected, with no new SQL parser or runtime operation. | CLOSED |
| PT04 | Page-prefix matching uses the original extracted text. The retained current-work literal is uppercase `'APPROVED'`; changing it to lowercase no longer matches. Named constructor comparison likewise preserves literal case. | CLOSED |

The existing two PF-09 source cases now contain the four reported negatives: named extra/composite data, text cast back to numeric, static DO float DDL and changed projection-literal case. All earlier cases and the legitimate private row-lock positive remain. Current final-definition lookup and per-signature/per-clause checks have no unchanged-file or whole-routine hash waiver. The guard remains a finite source check, not a general semantic/data-flow proof of every possible SQL rewrite.

No open concrete finding remains in this D01 follow-up scope. Root reports targeted PF-09 2/2 passing and current-final/source-gate 7/7 passing before the final negative-case additions, with the complete rerun in progress when this review was saved. These are root execution reports; the reviewer independently read source and did not execute the checks. Application/joined acceptance remains NOT_RUN.
