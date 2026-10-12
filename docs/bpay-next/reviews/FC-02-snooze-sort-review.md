# FC-02 Snooze sort source review

Reviewed 12 October 2026 by the independent read-only reviewer. **SOURCE PASS for the 0108 SQL branch, membership and cursor design; no remaining source finding.** This is not a database-plan, timing, joined runtime or visual acceptance result. No SQL, tests, business calls or captures were executed by this reviewer.

## Reviewed bytes

| Source | SHA-256 |
| --- | --- |
| `supabase/repeatable/12102026_0108_banking_pay_next_snooze_screen_sort_keys_v1.sql` | `a01a392b6b5d07581431615ed7373866d79bd767673b61228b2115791f604685` |
| Original row owner, `supabase/repeatable/12102026_0041_banking_pay_next_snooze_management_seek_v1.sql` | `58d05480da8e138d89233044e985b34782035e86d0cc16e271d237afd0a8a599` |
| Native Case key refresh, `supabase/repeatable/12102026_0058_banking_pay_next_case_screen_readers_v1.sql` | `47aab29e2e908075c074531b5aae375be4520336c3c3535a50bcd787efa0d321` |
| Locked supporting indexes, `supabase/migrations/12102026_0058_banking_pay_next_case_screen_sort_indexes.sql` | `9a8ba79f02d421c0245efcf233c484d952ef8ad8225c093cdd500cd1043cffff` |
| `scripts/bpay-next-source-gates.mjs` | `33fca448664ef8ced970041c8324cce2bd26ccfd204ed9c9f52250a2bf05b9d8` |
| `docs/bpay-next/final-definer-closures.json` | `894cd370ebb577bba67cc785587c5c13c2c4a8788ff5342ab8b7585ffef5fa8d` |

## Source conclusions

All 43 literal branches were inspected: sixteen Created/Until/Note control branches (0108:67–209), twelve Work Candidate/week branches (211–293), two Case Candidate branches (295–311), two Case NULL-week tails (313–329), and eleven fixed What category branches (331–428). Every enabled arm applies its whole requested Candidate, Client, scope, date mode, view and complete cursor qualification before its own `LIMIT`. The outer merge (429) applies the same primary direction, NULL-last rank and Created/UUID tie order before its final `limit + 1`. The arms are disjoint by active state, pointer state, NULL date state or native scope/category; no duplicate legal member or omitted current/applied/captured fallback was found.

The Work pointer arms reproduce the original row owner's `coalesce(current_revision_id, applied_revision_id, expected_revision_id)` exactly (0041:27–35). Applied is admitted only when current is absent; captured is admitted only when both pointers are absent. The selected revision remains owned by the Work and sealed. Expense What classification uses the effective revision's exact component key first, then the original captured approved-line ID, preserving Mileage, Travel, Accommodation and Expense precedence (0041:33–43; 0108:358–392). SEGMENT remains the original primary label Shift; its native date and clocks remain row facts rather than a newly inferred primary sort key.

Case Candidate order is the native Case key's `candidate_sort_name`. The key refresh derives this as PostgreSQL `lower` of precisely the same expression used for `candidate_display_name` (0058 repeatable:15–17), so sorting agrees with the row owner's actual displayed name; it does not depend on browser case conversion. Case Timesheet keys are NULL, consistently placed after Work dates in both directions, and the NULL tail advances by the same Created-descending/UUID-ascending tie. Until uses two literal non-NULL/NULL arms per active state and direction; its indefinite tail is last in both directions and preserves ties within that tail. Note is the native NOT NULL text column, so no invented Note NULL cohort is required (10102026_1100 migration:21).

The native row helper remains the final bounded DTO and amendment authority (0108:437–439). Its existing Case component/key checks, Work/revision checks and current-basis amendment restriction are retained unchanged (0041:15–39). This review covers states admitted by those native guards, not repair or adoption of corrupt missing authority. Exact-ID lookup still reaches an inactive original CLEAR for retained readback while ordinary views retain their native active/history semantics (0108:53–64). Count-only reads use the same filters and context, remain restricted to Created ordering, count Work controls and the expense subset separately, and return no row inventory (15–21, 432–435). Case controls do not inflate either Work count.

Input closure, active admin/NEXT authority, immutable cursor context and current control cut remain explicit (0108:11–52). The cursor carries canonical Base64 only for the native scalar sort operand; decoding preserves full UTF-8 text, is bounded and round-trip checked. Emitted cursors are bounded to 16 KiB (445–446); row arrays are bounded to 110,000 bytes (440–443). Continuation records only the last returned native key, including when the byte ceiling ends a page. Original microsecond Created values are retained. No financial authority, money computation, new table, public trigger, command or fixed shadow label projection was added by 0108.

## Index mapping and limits

Created has exact active-state literal ASC/DESC paths through existing active indexes and the 0058 `history_created` / `history_created_asc` indexes. Note uses the exact C-collated ASC/DESC expression and Created/UUID ties of its two indexes. Until's non-NULL and NULL arms agree with the index's native date direction and fixed NULL state; splitting the tail avoids pretending that DESC's default NULL placement is the screen's whole-list order (0058 migration:21–25,30).

Candidate Work arms use the native revision-name prefix index and complete full-name residual ordering; week arms use the revision-week index with same-week residual ordering. Case Candidate arms use the existing Case name key/index. What uses exact native scope/case-kind probes and the scope-label Created/UUID order; the four expense classifications still require current-or-captured line probes. The Work/scope index supports the control lookup leg. These are useful native index paths, but joined full-name, same-week, sparse-filter and expense cohorts can require residual work. **No universal full-order index plan, constant cost, fixed rows scanned or measured deadline claim is made.** At most 51 keys are retained per enabled arm; the largest enabled What merge has eleven arms, hence at most 561 intermediate keys at the public maximum page limit. This bounds retained keys and response materialization, not every candidate examined by PostgreSQL.

## Packaging closure

The final-definer registry correctly leaves the row owner at 0041 and advances only `private.bpay_next_snooze_management_page_v1(uuid,jsonb)` to 0108 (registry:16236–16275). Owner-only ACLs and the original private SECINV signature remain unchanged (0108:453–454).

The expense arms use a direct approved-line LEFT JOIN with one exact scalar-ID subquery (0108:360,369,378,387), not a LATERAL join. The earlier proposed `lateral` gate follow-up was an incorrect carry-over from the preceding investigation and is withdrawn. No additional token allowance is needed. The existing exact page callee allowance retains the original row helper edge, native cursor conversion functions and constructor checks; no broad keyword exemption or suppression is justified. Gate execution and installed catalogue verification belong to the builder's separately recorded checks.

The FC-02 screen, Worker/FE contract, native scroll behavior and A4 evidence remain separate acceptance work. This review does not close FC-02 or FC-01 D08 by itself.
