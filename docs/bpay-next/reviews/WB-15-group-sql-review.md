# WB-15 group SQL source review

**VERDICT: PASS for fresh finite SQL and source-gate scope; F1 and F2 closed. No open source finding.** This is the uninstalled draft review, not whole WB-15, application, database or screen acceptance.

Read-only review of full WB-15 card (02_WORKBENCH.md:616–652), all eight checklist rows, BD071 design, both 1946 files, exact Work1438/Case1252/common-public1806 predecessors and native item schema. No product code, SQL, tests, runtime, fixtures, hosted reads or checklist were executed/changed. Only this findings document is written.

## F1 — CLOSED: use actual selected authority for split reserved portions

1946 repeatable34–40 (migration60–66) treats `accepted_run_id IS NOT NULL` as selected authority. Native Work1438:245–253 and Case1252:585–591 intentionally copy a base accepted member into a RESERVED portion and clear `is_selected`, `selected_amount_known` and `selected_source_ex_vat` when an AVAILABLE portion also exists. The copied accepted identity is retained. Those known native RESERVED amounts therefore become `amount_unavailable`, and their NULL selected value is chosen before actual reserved amount. The aggregate can consequently null a fully evidenced ALL/WAITING presentation. This changes display only, but it hides genuine amounts.

Root corrected both identical SELECTs: they now carry `i.is_selected`; selected-amount unknownness/value selection use that boolean, and `accepted_pair` requires it. An unselected RESERVED portion now uses its existing `active_reserved_source_ex_vat`. The native classifiers and retained identities are unchanged. A genuinely selected member whose captured amount is unavailable continues to produce NULL. Fresh source verification: repeatable29–46 and migration55–72. The initial finding applied to repeatable `179788ae3b2da4510d5092a7d17f931e8bb2be558f50122daa7eee8012098d47` / migration `49e3f66ac46a278f06e8051c93da321c782419f2feff1ff89d812bd43a17d70d`; those bytes are superseded by the exact reviewed hashes below.

## Reviewed source boundaries

- Exactly five routine definitions: new owner-only display refresh, existing Work core and Case refresh with one tail call each, existing common reader and service-only public wrapper. The complete Work and Case bodies equal their exact predecessors after removing that single `PERFORM`. No money classification, native locks, holds, Rule-B choice authority, Source producer or financial publication changes.
- Additive private presentation cache only. WORK retains native Work UUID; OVERPAYMENT retains Case UUID; ROW uses the actual standalone Case-component UUID and retains its existing Case group as parent. Parent/candidate composite FK and Case-component FK prevent missing parents; helper joins the actual component to the existing parent Case. It does not create a financial Case/group identity.
- Initialization and refresh use the same complete INSERT/SELECT with only the group filter changed. Current metadata remains a keyed read join; cached Candidate display text is not invented. Existing metadata control-change paths refresh native parents; name/reference-only paths do not change any stored display sort field.
- Every supported CANDIDATE_GROUPS whole-list sort has an exact tuple/order/cursor and matching additive index: payment/week, client, channel, deductions, amount, reason. Native kind plus UUID provides a unique tie. Ascending/descending use the same tuple with reversed seek/order. NULL values are carried distinctly; no browser sorting of one page.
- Read scope is candidate + section, with existing client/channel filter and READY parent complete/current/not-dirty plus selectable requirement. Limit+1 occurs before factual PK enrichment. Parent composite FK and single metadata key prevent post-limit joins dropping or multiplying ten presentation rows. No lateral full-member reconstruction remains in this reader.
- ROW GROUP_LINES is narrowed to the exact native Case-component UUID and exact parent/candidate display mapping. Its returned presentation key is preserved; native Case/member fields remain actual facts.
- Refresh anchor is a native presentation identity resolved under the new coherent statement snapshot to the current exact sort tuple. Inclusive anchor seek preserves the row, while ordinary cursor seek remains strict. Closed context/refusal and exact-member cursor verification remain intact.
- All ten original named kinds, HEADER counts, issue fields, full auth/module/read-fence/date/permission behavior, original response limits, 5s statement timeout and typed refusal paths remain. Public wrapper retains only service_role EXECUTE. All private routines and the FORCE-RLS cache deny public/anon/authenticated/service_role direct access.

## Finite integration qualifiers

The accepted link is admitted only by an actual PREPARE run-command/epoch binding with SEALED active run. One unique pair is exposed; multiple actual pairs are deliberately not guessed into one. Consumers must not interpret a NULL single-pair result as proof that no Draft exists; bounded member expansion remains the owner of distinct actual links. The future WB16/WB17/WB24/WB25/DR child joins stay explicitly open in BD071. Registry closure and strict Worker/FE field mirrors were still being authored at this draft snapshot and are not certified by this SQL-only receipt.

Whole WB-15 application/render/12→10+2 journey, expanded-money reconciliation, source execution/compilation, database installation and actual screen-policy checks remain **NOT_RUN** by this reviewer.

## Exact reviewed source hashes

- `supabase/migrations/10102026_1946_banking_pay_next_candidate_group_display.sql` — `2ed25c12800b25e90a4992ee8915452c0f2251393d691ed8d20906e1d34d43b2`
- `supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` — `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37`
- `docs/bpay-next/design/WB-15-candidate-window.md` — `84bf0ed37c95d12c32d26536738ad78ba531ca32b161f0ffca7377c7815641ba`
- `supabase/repeatable/10102026_1438_banking_pay_next_workbench_origin_projection_v1.sql` — `b8d445c4a4cfc02978e8ac77a159350b0e1978aaff9a1172da8e7e157a6414f5`
- `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` — `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b`
- `supabase/repeatable/10102026_1806_banking_pay_next_issue_reason_reader_v1.sql` — `bbde6d21a6aad4eeb5bfa0741976246f58fbdebd6410c163d759fcf3321c8329`

Static equality receipts: `private.bpay_next_workbench_refresh_work_core_v1` tail-only=true, `private.bpay_next_workbench_refresh_case_v1` tail-only=true; initial SELECT equals helper except group filter=true.


## Fresh scalar-cast/source-gate review

The identical seed/helper now explicitly cast the seven MIN operands to their existing finite scalar types: client/payment/reference/hold labels to text, snooze/eligible dates to date, reason priority to smallint. These are scalar display values, not rows/JSON or financial decisions. Unknownness, selected/reserved authority, all sums, accepted pair, native identity and filter behavior are unchanged. The full Work/Case owners remain exact predecessor bodies plus their one display tail. Both INSERT/SELECT bodies still match except the group filter. The native first-page anchor remains exact typed identity + current tuple with inclusive seek; later cursors remain strict and context-bound.

The gate adds only the exact helper signature/callees and two native owner callee edges. Its new dynamic EXECUTE shape is restricted to the common Workbench signature, exact `s.presentation_key=$7` statement, exact target `p_after`, and the original eleven USING arguments. Existing template scalar interpolation prohibition and special-construct checks remain; no whole-file or routine exemption was added. Other diff lines in preparation constructors are newline-only.

### F2 — CLOSED: verify the new anchor producer variable

The initial gate `4c00554a3d5c2898c4a08f75d91621faf58c32dd47562fd09490bbea6ef37a17` at `scripts/bpay-next-source-gates.mjs:937–958` added `V_ANCHOR_PAGE` to the allowed generated-SQL expression tokens, but not to the declaration-checked variables set. Its declaration/assignment were consequently outside the closed producer checks. The final prohibited direct-scalar concatenation check at974 also does not cover it. This permitted a source edit replacing the actual boolean with a caller-derived text producer and concatenating it into `v_where` while satisfying this specific template boundary. The actual reviewed SQL declaration `v_anchor_page boolean:=false` and sole literal `:=true` assignment are safe; the finding concerns this gate extension's claimed closed source boundary.

Root corrected the finite gate. Fresh lines943–956 require exactly one `BOOLEAN:=FALSE` declaration whenever the anchor exists and admit only literal TRUE assignments; alternate INTO producers are refused. Line979 refuses direct anchor-string concatenation. Older exact reader definitions without the anchor remain supported. The exact common signature, closed execution/binding shape, original variables checks and literal special-construct checks stay active. No parser framework or exemption was introduced. F2 is closed on the final gate hash below.

Fresh hashes for this review:
- `supabase/migrations/10102026_1946_banking_pay_next_candidate_group_display.sql` — `2ed25c12800b25e90a4992ee8915452c0f2251393d691ed8d20906e1d34d43b2`
- `supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` — `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37`
- `scripts/bpay-next-source-gates.mjs` — `5279198ab62a4b013b40efcf13a284227b6d8d8a7bab254c4ffee02a4758f42a`

No product execution, SQL, database, runtime or tests were run by this reviewer. SQL and gate PASS remain finite; there is no open finding in this supplement.

## Final scalar/gate closure receipt

Fresh saved source binds the same SQL. Removing only the seven scalar casts reconstructs the prior PASS migration `80c86d1e59c730b303e5053419d64a123e68f5e871eac3d27a0874093993e9b9` and repeatable `fff77e169d348ac61bcb1b493af930faa222d34e861007250c41ef8b2831b992`; no other SQL behavior changed. F1 selected/reserved fix and native anchor remain intact. F2 is closed by the current finite boolean producer checks, preserving old no-anchor definitions.

- `supabase/migrations/10102026_1946_banking_pay_next_candidate_group_display.sql` — `2ed25c12800b25e90a4992ee8915452c0f2251393d691ed8d20906e1d34d43b2`
- `supabase/repeatable/10102026_1946_banking_pay_next_candidate_group_reader_v1.sql` — `c050f51fb7f132626bd479c87de127f78a41029ee16044d048d39af2e94b4e37`
- `scripts/bpay-next-source-gates.mjs` — `d06fa34c7edb92677def812ce8cc0e563b9b2de2d37823fbfea275704610280c`

Verdict: **PASS for these source changes only**, with original whole-item/future caller joins/application checks still outside this receipt.
