# WB-10 independent SQL read-limits review

10 October 2026. Read-only source review of the uninstalled1634 migration/repeatable against the full WB-10 card02:422–462, all five checklist deliverables, R2's exact filter/keyset/finite-seed/top-level-timeout correction, current1252 schema, current1520 public/kind wrappers and current1601 common reader. No SQL, tests, gates, database, application, fixtures, timing or hosted calls executed. This does not certify a two-second UI or deployed PostgREST configuration.

| Reviewed file | SHA256 |
| --- | --- |
| `supabase/migrations/10102026_1634_banking_pay_next_workbench_read_indexes_v1.sql` | `886cdd1d4333b05ec92ff6cf39b775bfe0a9f3225b02a8da2e237673b794b4b2` |
| `supabase/repeatable/10102026_1634_banking_pay_next_workbench_read_limits_v1.sql` | `391f32cbecaf4b32a311931d69ed7cbdb0dd85c0bae11a2cdf6a3a07a2e9c02d` |
| `docs/bpay-next/design/WB-10-index-to-kind.json` | `96fbb32d85035787bdad463ccccadf9d9f674fea0f5cceedeb0bba141e0ec457` |
| `docs/bpay-next/design/WB-10-read-performance-scope.md` | `1d451791988839ab5ff80bdbe5b9966cb5cc2588010fa03c599f731cc48d8998` |
| `docs/bpay-next/design/WB-10-timeout-and-group-scope.md` | `66d7ba13e69c2b8390e952612b8c4a582eb2cacc6cf276c2058a143ae1bdf2b1` |

**SQL/schema source verdict: PASS. No concrete correction found in these reviewed files.** Source findings and practical limits follow.

## Index/keyset correspondence

All23 CREATE INDEX definitions use actual columns on existing private maintained tables. A pure text comparison of their names, tables and full key expressions against the JSON inventory found23/23 exact matches. No duplicate index name, nullable final native tie, new table/trigger, public grant, producer or financial write is introduced.

| Current read scope/order | Reviewed index support |
| --- | --- |
| Candidate NAME | Section/channel/client_scope equalities followed by C-collated name, explicit reference-NULL flag, C-collated reference and Candidate UUID. |
| Candidate AMOUNT | Same equalities, actual unknown-count discriminator, known amount or internal0, native Candidate UUID. Unknown public amounts remain NULL. |
| Candidate DEDUCTIONS | Same equalities and the actual deduction-count boolean followed by the complete NAME tuple. |
| Candidate prefix search | Actual name/ref `text_pattern_ops` indexes under section/channel/client_scope. |
| Action/Blocked/Issue NAME, PAYMENT, AMOUNT | Section-prefixed exact reader tuples respectively use display name/Candidate UUID/item UUID; Timesheet ref/item UUID; nominal-NULL discriminator/coalesced sort operand/item UUID. CASE portions retain the same nonnull item UUID tie as WORK. |
| Issue optional Candidate/client/channel/reason filters and prefix search | Added finite item filter/page indexes, actual name/ref/Timesheet prefix indexes and retained1252 scoped index. |
| GROUP_LINES / EXPORT_ROWS | Actual group/section/item UUID and group/item UUID indexes, plus Candidate/section, section/reason, section/item and native item PK for the supported finite scope combinations. |
| CANDIDATE_GROUPS | Actual Candidate equality then week-NULL discriminator/native coalesced date/C-collated group kind/coalesced WORK-or-CASE UUID. Existing schema pins one real native parent per group. |
| PICK_CLIENTS / HEADER / ISSUE_DETAIL | Added section/channel/C-collated client_scope ordering; existing natural summary and item primary keys remain point-lookup support. |

The existing1252 `eligible_from_date,id` FUTURE_WEEK partial index and1100 active finite snooze-due index are real source objects. Existing1429 task/metadata/due/seed indexes are retained, rather than recast as new evidence. The repeatable header explicitly maps current kinds and sorts to these indexes. This is source correspondence, not an assertion that a planner will choose a particular index for every optional-filter combination.

## Exact successor preservation

Pure text comparison of every complete CREATE definition establishes that the ten private kind wrappers and public wrapper are identical to1520 after removing the single added `set statement_timeout='5s'` line from each. The common helper is identical to1601 apart from that setting and exactly three authorized group-order lines: native-date tuple, native-date cursor seek operand and matching scalar ORDER BY. No issue projection field, classifier, query authority or money expression changed.

The group outward cursor remains `[weekIsNull, YYYY-MM-DD-or-empty, groupKind, nativeParentUUID]`; the new seek converts its date operand to the same native-date/coalesced sentinel expression as the index/order. The explicit NULL flag distinguishes a missing week from a genuine date, and native WORK/CASE UUID closes the tuple. Ascending/descending traversal remains consistent with the preexisting fixed scalar ORDER BY implementation.

All eight1601 issue provenance fields remain intact in each of the three issue projections: direction, nullable payroll stage and six actual stored exact-decimal-text operands. The successor preserves unknown monetary NULLs, closed JSON constructors, current/adoption/dirty/pending barriers, exact context/membership checks and whole-response byte refusal. No opaque financial record serialization or history read was added.

The full ALTER OWNER/REVOKE/GRANT/NOTIFY/COMMIT tail is byte-identical to1520. Private helpers remain inaccessible to browser/service roles; the public STABLE definer RPC retains its exact service-role and active-admin checks, fixed search path and service-only EXECUTE. Required final `notify pgrst, 'reload schema';` is present. The12 current reader signatures are selected by1634 in the final-definer registry.

## Timeout and bounded enrichment scope

All12 current maintained readers—the common helper, ten kind wrappers and exposed public RPC—carry the5s function setting. The public metadata is the intended PostgREST14 hoist input; a private transitive setting alone is not claimed to establish a top-level bound. The saved timeout design correctly retains the release prerequisite to verify effective `db-hoisted-tx-settings` includes `statement_timeout` and that the actual schema cache has reloaded. The existing global financial120s route is not changed. This review verifies source placement and the explicit prerequisite; it does not verify a hosted setting/session or elapsed timeout.

CANDIDATE_GROUPS still limits the exact native parent seed to the requested maximum10 plus one lookahead before metadata/current-member enrichment. Each selected parent's aggregate covers all actual current members of the requested section, using the scoped group index. It preserves exact group money/unknown/selected/reason semantics and reads no historical approvals, effects or outcomes. It does not cap a parent's child count, truncate money to a page, or substitute Candidate-wide totals. An extremely large group remains a genuine unmeasured performance dependency; neither these indexes nor a local compile can prove its two-second usability. No new stored per-group money projection is demanded by this scoped review.

The retained0580 CURRENT_WORK reader is the existing bounded factual-detail owner and is not replaced by a payable summary. The new timeout configuration covers the actual maintained Workbench public route and its12-reader closure; this is not a new timing claim about unrelated preparation RPCs.

Worker4s timeout/step header and Office local browser measurement are separate caller deliverables and are outside this SQL review. All real browser/Worker/RPC/DOM/page-size latency remains NOT_RUN, including limits1/17/100 and heavy-Candidate behavior. The historical300 Timesheet/30-worker/two-Draft result remains owner-reported DB-only evidence with the original receipt unlocated, exactly as the current card requires. No whole WB-10 or joined runtime completion follows from this source PASS.
