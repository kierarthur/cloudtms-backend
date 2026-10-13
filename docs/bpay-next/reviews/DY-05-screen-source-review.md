# DY-05 frozen detail source review

11 October 2026. **Scoped SOURCE and A4 PASS after the corrections below.** Read-only product inspection. Only review notes are written. No product, checklist, SQL, database, business fixture, browser, screenshot, test, build or hosted action.

## Sources and scope

Full DY-05 card: ordinary BE build plan `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:982–1009`; current ordinary checklist D01–D04 at `IMPLEMENTATION_CHECKLIST.md:2562–2577`; dependencies DY04/F06/F08/F16; `docs/bpay-next/design/DY-05-build-scope.md` and the independently saved `DY-05-screen-scope-review.md`. Supplied SCR47 Lines/Shifts/Breaks/Used rates/Available rates and SCR156 references, their specs and all nine matching PNGs were previously inspected individually. Relevant policy: header Close/read-only control purpose, SDP29 return continuity, SDP30–31 continuous bounded chunks/loading more, SDP34 dialog/fixed order exemption, SDP78 labelled scroll region, SDP81 phone targets, and the 42-row actual screen review. The earlier scope note's ordinary checklist line anchor is stale; the current anchor above is verified.

Current product reads: FE `js/bpay-next-office.js:120–131,409–434,1011–1073,1149–1168`; `css/bpay-next-office.css:490–516`; unchanged native window adapter `js/bpay-next-ui.js:23–76`; BE frozen context successor `11102026_1005_banking_pay_next_frozen_client_origin_v1.sql:5–74`. SQL correctness is separately reviewed by the investigator; this receipt reviews UI/source integration and closed frozen transport.

## Essential findings

1. **P2 — selected later-page detail cannot recover.** Office1060 Refresh resets LINES to its first100 rows and then insists that the currently selected run_line_id appears there. If line101 or later was opened and SHIFTS/BREAKS/rates subsequently fails, Refresh cannot pass this check. Retain the exact selected immutable line tuple and retry the same failed native read/cursor, or restore its original native page; do not replace the selection with a first-page lookup or fetch new economic input.

2. **P2 — cache and internal Work discovery are unbounded.** Office1023–1031 concatenates every page into `lines`, `comparisons` and each detail list. `findWork` immediately drains REVISION_COMPARISON pages until a match/terminal page and retains the entire prefix. This does not implement the bounded three-page native presentation already used by the Workbench factual child. Keep at most three displayed chunks and bounded cursor history; Work discovery can inspect each finite native page while retaining only its cursor and the matching immutable Work identity. Scrolling backwards must still recover earlier native chunks. The complete-page scroll handler1049 also calls `act(more)` after exhaustion, creating a needless busy/rerender cycle; admit continuation only when the selected native page has a next cursor.

3. **P2 — money columns lack the required numeric treatment.** Office1046–1047 creates every heading and cell without a money/numeric class; CSS501 explicitly makes all headings left aligned. Lines Amount ex VAT and all three Available rates cells therefore do not have matching right alignment/tabular digits. Apply numeric classes only to the native monetary/quantity columns and retain factual string formatting. No calculator or aggregate is needed.

4. **P2 — read-only detail button disappears for a surviving READY member after PARTIAL discard.** Office1149 excludes all actions when `discard_phase` is non-NULL. Office1161 creates the READY Draft detail button, but1166 only mounts it when `hasActions` is true. Desktop double-click/Enter still opens this read-only child; phone has no visible opener. Compute read-only detail availability independently of the existing mutation exclusion. Do not restore Try again/Remove on the discarded scope.

5. **P2 — remount restarts the initial read and selection.** Office1073's onMount always resets LINES and selects its first row. The native adapter calls onMount whenever the frame returns to the top of the stack (`js/bpay-next-ui.js:59`). A return can therefore discard later-page selection, current detail data and scroll instead of restoring them. Use one initial-load fence; later mounts should render retained native state. The same bounded cache/cursor state can satisfy both this finding and finding2.

## Already corrected or sound source boundaries

Fresh source has removed the unnecessary primary footer Close: Office1054 empties and hides that footer, retaining the native header Close. The subtitle1056 now uses selected LINES `target_pay_channel`, not origin `pay_channel` (the latter is the captured source channel in SQL70). Numeric facts remain string formatting; genuine NULL rates render an em dash and no missing value is replaced with zero.

The actual READY row exposes the detail in the existing Draft review rather than a screenshot-only path. Five existing tabs use only LINES/REVISION_COMPARISON and the four native frozen reader kinds. Every frozen reply has an exact origin schema, expected native revision equality and identical captured context across tabs; no current Client/Contract/Timesheet lookup is introduced. “No Contract (Daily)” is gated by actual origin DAILY plus NULL Contract, rather than NULL alone. Recorded clocks/break duration, approved hours and native available cells remain reader facts. Only STANDARD permits genuine three-NULL rate cells; other shape/family/cursor checks remain. Client labels and role/band are captured display facts even when Client UUID classification is unknown; no UUID is inferred by the UI.

Actor/owner and closed-window fences are present. The parent review also carries an immutable preparation namespace fence; final closure should verify the child uses that same scope fence before accepting late reads. No unknown mutation/command journal is needed for these read-only pages. A failed read must retry its original tuple, not become a preparation retry.

No whole DR15 identity/header redesign, server sort protocol, new RPC, command, rate calculator, payment action, source producer or mixed-channel preparation system is required for these corrections. SCR47's missing used charge column cannot be fabricated from an available rate; later shared full-review scope and genuine joined rename/rotation/payment/2-channel2/2 acceptance remain separately owned/AUTHORED_NOT_RUN.

## Initial inspected bytes

| Source | Raw SHA-256 |
| --- | --- |
| FE Office | e8d8f30272b36e9baa5684dc2c7ea3c9d1427ca66396293776fa4c919f807bad |
| FE Office CSS | e0fbcfcac926ff1744416c4580e709ea26625080c41cf6f3d8c0f92c7aed5329 |
| Native adapter | a4ae919379621a21e68ef9955bc4a8cebb5e37f20ede56691eef7a16b60d0ee5 |

These are the inspected initial product bytes, not a final source or visual approval. Root is producing actual A4 captures separately; this source receipt does not mark any screenshot or screen criterion PASS.

## Fresh scoped source closure

All five original findings are closed in fresh Office1011–1077 and the READY action decision1154–1171. Page retention is at most three native chunks; history is capped at32KiB. Backward native cursor reads and stable row anchoring retain the detail view. Work discovery retains only one at-most100-row comparison page, rather than accumulated membership. Complete native pages do not start a further scroll action. Selected later-page line identity remains retained independently of the current display cache. Initial load runs once; later native mounts render retained state. The child now uses the same preparation namespace/current namespace fence as its parent, catches invalid context and rejects actor/owner/closed late reads.

The later precise retry finding is also closed: failedRead retains the actual kind, exact native arguments (including Shift ID and cursor), original page basis and direction. Refresh repeats that request rather than resetting the line or selecting the first Shift. Comparison discovery retries its exact failed cursor. These are ephemeral read arguments, not another command journal or money-input fetch.

Native numeric classes apply right/tabular treatment to values/headings; phone pseudo-labels remain left. Source money formatting groups whole-number digits and uses the UK minus-before-pound presentation while preserving genuine native fractional precision. Approved hours use the same factual decimal formatting in Shifts and Used rates. Both Used/Available rates use the five Standard business bucket order; all genuine NULL Standard cells survive. The scroll host is a labelled region, continuation says Loading more, and phone tabs retain native13px labels/44px targets with wrap inside the same tablist. No extra tab level or clipped label is required.

Native header Close remains the only dismissal action. Header subtitle includes captured Candidate/reference/Timesheet reference and selected target channel; native mount/late subtitle placement reuses the original title host. The READY read-only opener is mounted independently of PARTIAL mutation restrictions. No DR15 parent redesign, new reader, money formula, source producer, sort protocol or financial action was added to close these findings.

**Verdict: scoped source PASS; no remaining essential source defect found.** This does not approve the whole DR15/shared SCR47 screen, server business behavior or joined DY05 acceptance. Final actual A4 review is saved at `docs/bpay-next/screen-designs/SCR-47/review.md`; its31 individually inspected current PNGs support a scoped child-screen PASS.

| Fresh corrected source | Raw SHA-256 |
| --- | --- |
| FE Office | a62322f06a74857c141b9a1b5c663dc3aac404631f01f8d75c7d49d7236a3800 |
| FE Office CSS | f5f132a6b98159050227b06be1d1ac631bc3aa3778dea479fcba6c221bd17152 |
| Native adapter (unchanged) | a4ae919379621a21e68ef9955bc4a8cebb5e37f20ede56691eef7a16b60d0ee5 |

## Final source and A4 closure

The prior corrected-byte table is an intermediate source receipt. Latest Office1016/1030–1031/1063/1069 retains one exact selected native Shift fact separately from the at-most-three-page Shifts cache, so the Breaks selector remains visibly bound to the current Shift after a page leaves that cache. The exact failed kind/line/Shift/cursor still drives Refresh. The actual second-Shift request log repeats the same ID ending00ef; latest phone failed/recovered images retain its16:30 option and19:00–19:30 break facts.

Office1051 marks already-formatted native money using the existing data-next-formatted-money hook. Factual decimal-string formatting and genuine fractional precision remain unchanged through shared presentation. Two current actual captures visibly preserve £20.123456 and £1,234.123456. CSS530–531 adds exact-child phone minimum44px dimensions for native header Close and Shift select; all five final phone tabs remain complete at native13px.

All31 current actual PNGs, including the final second-Shift chronology-only sample recapture, were individually inspected against the previously individually inspected nine supplied references. No essential source/pixel defect remains. The full42-row scoped A4 review records owner-directed25 and readerAdditions26 N/A, all visible differences/control purposes, current PNG hashes and explicit joined/runtime/full-parent limits. No broader DR15 or business acceptance is claimed.

| Final current source | Raw SHA-256 |
| --- | --- |
| FE Office | 9a27f277929aec2fb5f7a20d8b05f4bcf2d3c37d046154d6f687168bf060b501 |
| FE Office CSS | 99c4461101c08c5295ead71d2926ebc57f154d0844795c09685f1713f31a9171 |
| Native adapter, unchanged | a4ae919379621a21e68ef9955bc4a8cebb5e37f20ede56691eef7a16b60d0ee5 |

**Final verdict: scoped SOURCE/A4 PASS. Genuine joined payment/rename/rotation/two-channel2/2 checks remain AUTHORED_NOT_RUN.**
