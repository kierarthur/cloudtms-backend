# F-23 SQL source guards

Read-only design, 10 October 2026. Read full F-23 card (01:985–1008), checklist D01–D12, B2.13–15 and current F-23-SCOPE. No product edits or SQL, application, test, scanner/gate execution. PowerShell/Git/rg reads only. Root implements. Rule B remains binding; no producer/PIN/enroller rewiring or parked F-25 repeatable revival.

## Reuse and finite scope

Use one small source-gate module, called by the existing explicit source/push gates. Reuse `inventory()` and `sqlDateKey` from `scripts/cloudtms-db-release-lib.mjs`, `readManagedClosure`/`scanManagedSql` from `cloudtms-managed-release-sql.mjs`, and `routineDefinitions`/`definerInventory`/`changedSourcePaths` from `bpay-next-final-definers.mjs`. No new release engine, SQL rewriting, compiler or fixture programme is needed. Wire checks before local apply's first SQL and before protected release admission through existing source-check seams; do not put a DB connection inside the source gate.

The fixed reviewed F-01 base is `a6f7423eb71e6e7cd173e705c2290e5408a1d077`. `changedSourcePaths` already includes base→HEAD, saved HEAD diff and untracked supabase inputs, validates the base ancestor and uses registry releaseBaseCommit. A scan using only HEAD's last commit misses earlier authored SQL. Resolve admitted migration/repeatable roots and recursive physical files through canonical inventory/closure, retaining source path and offsets for diagnostics. Outside-live-SQL evidence packs and historical review artifacts do not become executable inputs. Deleted parked repeatables remain deleted.

Do not fail unchanged upstream source or silently exempt an entire touched file. A finite inherited-construct check can reuse exact Git-base source: key each occurrence by the **same canonical routine signature, target declaration/type, exact executable statement tokens including literal values, and occurrence multiplicity**. Permit a current occurrence only while the same statement exists in that routine at the reviewed base; consume baseline occurrence counts. New/mutated occurrences fail. This allows an unchanged inherited statement in a copied full successor, without allowing a new function to borrow another function's exception. For nonroutine migration/DO code require the same physical base path and exact enclosing statement. Report inherited occurrences separately; their presence is not a new-source PASS. No broad path wildcard or documentation-comment waiver.

## Body lexer: the current scanner alone cannot inspect these hazards

`scanManagedSql` (lines 13–71) intentionally treats dollar bodies as `<BODY>` and all strings/quoted identifiers as `<QUOTED>`. `routineDefinitions` (61 onward) correctly identifies signatures but does not inspect body SQL. Keep this release parser's opaque semantics unchanged.

For the source gate, add a narrow offset-preserving lexer using those same comment/quote/dollar-delimiter rules. Locate only the outer routine AS dollar body or DO body from statement source spans, and lex its body once. Ignore nested comments and literal content; retain single-quoted literal value as data when checking NOTIFY and preserve/unescape quoted identifiers rather than confusing them with strings. Keep operators `->`, `->>`, `#>`, `#>>`, `||`, `::` as whole tokens, track parenthesis depth, and reject malformed/unclosed tokens. A dollar string inside a body is a literal, not automatically executable code. Dynamic EXECUTE containing one of the forbidden constructs needs explicit classification rather than being silently marked checked; the current changed corpus has no identified such unsafe dynamic SQL. The lexer is for the finite rules below, not a general SQL parser or a release rewriter.

## D09 exact checks

| Rule | Practical finite algorithm | Failure / safe form |
| --- | --- | --- |
| Qualified SQL special forms | In executable tokens recognize `pg_catalog . NAME (` with NAME exactly COALESCE/NULLIF/LEAST/GREATEST, including equivalent quoted identifiers. Check routine/DO bodies and outer statements; ignore comments/string contents. | Fail new occurrence. These are SQL special forms, so safe source is unqualified `coalesce(...)`, etc.; do not mechanically qualify it again. |
| `%ROWTYPE` SELECT target | Build per-routine declared variable→qualified relation map from `% ROWTYPE` declarations. For SELECT's top-level INTO [STRICT] target, check only a whole declared variable. Ignore scalar `v_row.field` targets and RECORD variables. For a new full-row SELECT require one matching FROM alias expanded as `alias . *`; reject composite `SELECT alias INTO v_row`, bare `SELECT * INTO v_row` and unrelated/multiple projections into one whole-row target. Preserve INTO STRICT, source filters/order/locks. | Safe `select r.* into strict v_row from schema.table r where ... for update;`. `RETURNING * INTO`, assignments and scalar field selects are separate constructs and are not this guard. Existing identical base `SELECT * INTO` is reported as inherited, not a reason to change money owners. |
| JSON extraction with concatenation | Parse operator chains at each parenthesis depth rather than regexing an entire line. Fail an extraction (`->>`, `#>>`, and JSON extraction operands used as text) whose JSON left operand is part of an unparenthesized `||` chain at that same level, e.g. `'x:' || j ->> 'k'`. Require the text-extraction operand to be a completed grouped operand before concatenation. Recognize function argument boundaries so `prefix || format('%s',j->>'k')` and `v_core || jsonb_build_object('k',j->>'k')` pass. | Safe `prefix || (j->>'k')`; chained safe groups can concatenate freely. Comments and `'literal ->>'` do not trigger. Do not reject every line containing both operators. |
| UUID MIN/MAX | Find actual aggregate calls MIN/MAX, including pg_catalog qualification, rather than comments or function names containing those substrings. Resolve simple arguments from typed locals/parameters, explicit casts, FROM-alias columns and current source table/type declarations (saved contract columns may corroborate existing types). Fail UUID arguments. An unresolved new MIN/MAX expression fails with a bounded unclassified diagnostic rather than pretending to infer a type from `_id` suffix. Proven numeric/text/date/timestamp arguments pass. | Prefer native ordered UUID selection where appropriate. For `min(x::text)::uuid`, recognize the outer UUID cast and require the real same-scope cardinality proof in source; support a finite explicit `count(distinct x)`/same filtered input followed by a rejecting count branch before use. Do not count an unrelated COUNT or a comment as proof. An unclassified proof remains a review failure; there are no current new MIN/MAX calls requiring a broader analyser. |

The last rule needs type context: unchanged `21072026_1820_01_import_review_lifecycle_rpcs.sql:508` has `max(id)`, but its `public.import_review_events.id` is **bigint** (baseline 2495–2498), so a blanket `max(id)`/`*_id` prohibition is wrong. No mutable schema inference or SQL query is needed for this fact.

## Current saved-source census and inherited constructs

A read-only base→saved-source path census found 29 current changed/new SQL paths, including one changed verification file; this is a source snapshot count, not execution inventory validation. The admitted SQL guards should cover the canonical migration/repeatable closure set; unchanged verifiers remain separate release authority.

- No raw executable match of `pg_catalog.coalesce/nullif/least/greatest` was found among those 29 paths. Unchanged upstream examples remain in `25082026_1704_candidate_google_identity_link_v2.sql:50,54,65` and `25082026_1616_candidate_home_daily_read_v1.sql:11,82`; they must not trigger unrelated Candidate rewrites.
- No MIN/MAX call was found in the changed paths except comments describing overlapping counters (1958:350 and 0151:890). These comments are not SQL violations.
- No direct unparenthesized `prefix || j ->> key` match was found among the changed paths. `07102026_0018:96,98` uses grouped component extraction and is safe. `06102026_1206:358–361` puts JSON extraction inside format arguments, and 2018:3612 concatenates jsonb_build_object: also safe. Unchanged upstream counterexamples exist at `17082026_0015_candidate_daily_phase2_rpcs_v1.sql:382,390,785` and later retained Candidate successors; preserve them unless the exact new source owner changes that statement.
- Strict `%ROWTYPE` style matches are numerous: 2018 contains retained `SELECT * INTO` full-row reads; 1958:364/0151:904 retains v_period_rule's SELECT. **Proven inherited examples:** base 1590 seal_selection has the same 0151:20 statement; base 1600 claim_prepare/pin_selected has the same 0151:111,117,140,235,241,245,279,282,292,298,336 statements; base 0360 has the same v_period_rule statement now at 1958:364 and 0151:904. A raw changed-file ban would falsely force style-only changes across these money owners. Newly authored/nonmatching instances must use alias.*. A complete token/occurrence comparison is implementation work; this read-only census does not claim D09 passed.
- `06102026_1301:45,73` selects `*` into **RECORD** v_stage/v_binding, not `%ROWTYPE`; do not flag these with the rowtype-specific check.

## D10 exact controlling RPC NOTIFY

Use `definerInventory().routines` in canonical execution order, not lexicographic filenames or the physical predecessor's tail. Compare base/current canonical public callable signatures and definition tokens: new signatures and changed definitions are candidates (the card's new/changed RPC wording is stricter than merely new overloads). Include signature changes under the same public name; exclude trigger/event-trigger bodies and non-RPC procedures. A changed body in an included physical child selects that signature's **latest controlling repeatable root** from the current inventory/registry. Do not infer PostgREST exposure merely from a private function or a worker route string.

Read and expand that root with `readManagedClosure`, preserving include execution order. Scan its outer statements. Require exact top-level `NOTIFY pgrst, 'reload schema';` after the last included definition and ACL GRANT/REVOKE for the relevant RPC, and as the last substantive statement before final transaction COMMIT (comments and ON_ERROR_STOP metadata do not count). Check literal channel/payload from original source spans: `<QUOTED>` tokens alone cannot prove the payload. A NOTIFY in a function, comment/string, earlier predecessor, local loader's generic final notify, or notify to another channel does not satisfy B2.14. `SELECT pg_notify(...)` is not the card's exact required spelling. No controlling repeatable or unclassified signature/ordering fails closed.

Current public changed RPC-bearing sources inspected all contain the required outer NOTIFY after grants:

| Source | Actual RPC / outer NOTIFY |
| --- | --- |
| 06102026_1157 | public.pay_finance_component_resolutions_apply; NOTIFY 998 |
| 06102026_1206 | public.bpay_next_payment_history_page_v1(jsonb); GRANT 413, NOTIFY 414 |
| 09102026_2018 | original CSV advice, financial/preparation intake/page and weekly_source_first_authorise existing signatures; final NOTIFY 4345 |
| 09102026_2109 | public.bpay_next_cutover_readiness_page_v1(jsonb); GRANT 714, NOTIFY 715 |

0236 has only private channel readers and still has NOTIFY 105. 0151 has only private successors and NOTIFY 1207. 0718's frozen guard/trigger successor has no new public callable RPC and needs no new NOTIFY merely for its trigger body. Final closure/inventory selection must still be used by the implemented check; this table is physical-source evidence, not a substitute for that gate.

## B2.15 and exact implementation boundary

B2.15 is the separate same-migration public-RLS policy requirement, not permission to widen D09 into a schema policy framework. The current changed migrations introduce private tables; no new RLS-enabled public table was found. If later code creates one, source-read CREATE TABLE + ALTER ... ENABLE ROW LEVEL SECURITY and require the exact `cloudtms_miget_service_owner_all` policy in that migration; preserve its required real installation verifier. Dynamic/unclassified public-table creation must not silently pass. No existing private table should receive that public policy by analogy.

## Source receipts

| Source read | SHA-256 |
| --- | --- |
| cloudtms-managed-release-sql.mjs | `ff391fccf7f690d4d476e935870d740667e4dfd92b972bfd5d7d80b1b3bb912b` |
| bpay-next-final-definers.mjs | `651217a7554f77a79aa944e72034a22cbd4e7a23298e6f7a35e8cf0321c54670` |
| cloudtms-db-release-lib.mjs | `a5c79ae9d95b5e835e0c84fd876cad45bd8f5b31f8c1bafa95c76d6578afb4bf` |
| 10102026_0151 rule-B consumers at read | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |

No gate was implemented/executed by this reviewer. Re-read current saved source before root edits; root may be working concurrently.
