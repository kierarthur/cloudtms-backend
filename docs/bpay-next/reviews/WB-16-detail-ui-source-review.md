# WB16 detail UI source review

**SOURCE PASS — no open findings in the reviewed integrated WB16 detail/caller subset. Whole-WB16 journeys and screen acceptance remain separately qualified below.** Read current product FE mountPaymentDetail/openCandidateWindow and real Office preparationTransport join, plus current Worker/FE closed row projections. No product edits or SQL/runtime/app/test execution. CSS/screens are a separate pending root review.

## Closed earlier findings

- Office passes its existing actual preparation transport to the Candidate window (office759); actual Expand/Collapse mounts in-place under native group (workbench1012,1042–1049). Native store is retained; opening detail performs only existing GET readers. Accepted-run branch remains the existing Draft owner. Reset/refresh/dismiss disposes detail controllers and nested child frames; pruning removes groups absent from the current bounded Candidate page.
- Case caches/pruning now use CASE:component identity consistently (938,970–978). Original displays actual case_principal_approved once; each component Outstanding/due uses exact factual receipt or NULL. No RESERVED/zeroed-portion fallback. Native ordinal continuation proceeds to actual component rather than first100 find failure. Tax labels retain genuine component treatment.
- Actual same-segment portions share one physical shift and status facts; same-date siblings keep native approvedsegment/shift IDs. Aggregate per-shift payable amount is NULL (957), not an approved-money substitute. Correction original reference and current role/band remain actual server facts.
- Break/Pay-line/shift/main panes now have both-direction cursor branches and three-page/history limits, rather than forward-only replacement. Compact histories use UTF8≤32768; this closes missing direction wiring, but scroll-anchor details below remain.
- Used/available rates remain separate; formatter retains supplied fractional precision instead of rounding unit rates to pennies. Five-bucket RATES request is inherently bounded by the native five-value domain; no claimed additional bucket exists. Suggested margin/rate remains unavailable pending its actual named owner.
- Worker and FE GROUP_LINES closed field mirrors match: component_amount_known boolean?, three component amounts decimal?, native line/shift/parent references nullable. Their identical coherence checks reject Case Work-fact contamination, mismatched shift identity pairs, reference-without-parent, and known/null total inconsistencies. Decimal matches existing numeric text convention.

## Remaining finite findings

**F1 — full GROUP_LINES validator absent at detail owner.** mountPaymentDetail906 and980 use sameWorkbenchRevision only, despite supporting an injected client just as the Candidate owner does. That comparison only covers six revision identity fields (590), not the request scope/kind/actor, row shape/order, cursor or page receipt. Apply existing validateWorkbenchResponse(page,'GROUP_LINES',normalizeWorkbenchInput(...),actor) on initial/continued pages and exact member point revalidation before smaller original-context/member checks. Recheck current/generation after the second awaited point read before returning facts. Preserve exact Case/Work/line/segment/member identity under the original address.

**F2 — continuous scroll loses the visible anchor on trim/back.** Main load981 trims/prepends pages then render restores only old absolute offset; moreShifts966 similarly changes detail pages without compensating removed/prepended page height. showFactualWindow933 uses *net total scrollHeight difference*. Three similarly sized pages exchanged produce0 difference even though the first page disappeared. Reuse the existing native page-height or visible-row-key+offset strategy: subtract actual evicted front page height, add actual prepended front page height, preserve the exact currently visible native row. Do not retag cursor. Cursor history that gets trimmed needs a truthful restart/earlier affordance; current detail panes stop silently at the earliest retained history.

**F3 — child failure/finally/cache races.** showFactualWindow934 catch tests live but not its read ticket; finally writes loading=false without ticket/live. moreShifts966 catch tests only current, so an old failed read after a same-key retry can overwrite the replacement detail entry and render it; finally writes old detail loading unconditionally. Use captured ticket, live frame and evidence.get(key)===detail consistently through success/catch/finally, including after break batches. Restore parent UI focus only while the parent is interactive: render964 currently calls ui.restore while a deeper native child may own focus. Pure stale internal counters may be harmless, but late visible publication must be suppressed.

**F4 — exact negative fractional precision loses sign.** precise897 converts whole '-0' through BigInt and emits '0', so native signed quantity/rate '-0.500000' becomes '0.500000'. Keep the original negative sign independently of BigInt formatting (except actual zero if desired) and retain fractional digits. Current preparation money/quantity numeric transport allows signed decimal text; a correction quantity must not flip direction visually.

**F5 — nested evidence cache is finite but materially larger than a small page cache.** Each detail retains4 section×3 page×25 member rows; each aggregate key can retain3×25 shifts and10 break rows per shift. Up to30 visible Candidate groups may remain expanded. Current pruning bounds keys but does not bound retained nested evidence bytes or aggregate total cache; four evidence tasks each fan outfour Break readers. Keep the existing page cache sizes, but load child evidence on demand/visible native members and bound shared evidence/in-flight fan-out. No new protocol/framework or durable evidence store is needed. Do not claim a three-page total-memory bound merely because each nested lane has three pages.

**F6 — typed new scalar amounts allow negative impossible display totals.** Both new Group validators permit any signed decimal when component_amount_known=true (Worker499/FE501), although ready/waiting/reserved aggregates are sums of nonnegative classified positive/reservation facts. Add the same finite nonnegative guard to these three new scalar display fields, preserving nullable known semantics. Do not apply a blanket sign ban to signed residual, recovery origin or unrelated existing amounts.

## Boundaries and qualification

No parked F25 protocol, new financial command/RPC, browser money aggregation, Work target quote calculation or opening mutation is introduced by the observed integrated join. Native old thin/Draft/history owners stay separate. WB27 estimate, bucket resolution controls and later snooze/tick ownership remain explicitly pending. Source confirms the implemented subset and listed defects only. Actual continuous-scroll behavior, keyboard/inert focus, mixed same-day/correction/Case journeys, phone layouts/screens and timing remain NOT_RUN by this reviewer.

## Exact source snapshot hashes
- `js/bpay-next-workbench.js` — `098a8727a8477b3fc25c5927f4d5810b0151e9d880f797fb5eac758ecedb0944`
- `js/bpay-next-office.js` — `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0`
- `broker/src/bpay-next-workbench.js` — `78917cd474bbd585de010d1b515c8c71a56b87857aafc8f4d7820746636368c1`
- `broker/src/bpay-next-preparation.js` — `da8b67652178bbbaa0e0b418d6ef97800e916416c1521e989c631effd02d8fb9`

## Fresh correction review — current product after root fixes

This supplement supersedes the initial six-finding status above. Current full GROUP_LINES normalized request/actor/receipt validation is present at both main pages and factual point (check904,load982). Native DOM row-key+offset anchors replace total-height-delta compensation in main/shifts/factual panes; actual day/shift/break/line IDs anchor the current view. Child catch/finally uses ticket/live, shifts catch/finally uses ticket/current/cache-entry, and precise preserves -0.xxx. One evidence task at a time means at mostfour inner Break tasks from that lane. These close F1's full-validator portion, F2's anchor compensation, F3's visible catch/finally race, and F4.

**Remaining R1 (small tail of F1):** factual906 still checks current after preparation transport but not after the awaited GROUP_LINES point. Add current after that second await/check before returning. openPayLines988 performs chunked RATES requests without checking current immediately before/after each chunk; after closure during a point, it can issue another batch even though final child publication is suppressed. Use actual current predicate at both seams; no new protocol/abort framework.

**Remaining R2 (small tail of F2):** Main, shift and factual-child histories discard earlier seeks at32KiB. With history empty and first retained page.after non-null, there is no first/restart-earlier affordance/intent branch. Older real rows become inaccessible. Reuse native Candidate restart-on-upward intent or an honest First action only when earlier history was discarded, reset that lane through its unchanged original request. Never retag an old context/cursor; retain the current native anchor for ordinary forward/back reads.

**F5 narrowed/qualified, no new cache framework demanded.** Actual retained key counts are finite: each Payment detail has4 section lanes×3 pages×25 members; each current key has at mostthree25-shift pages, each emitted shift at mostten cached Break rows, and one actual Case receipt per present component. Candidate keeps at mostthree10-group pages and prunes controllers outside them; evidence keys are pruned to present native members. This is bounded source behavior, although nested upper bound is larger than three total pages. Root's one evidence task limits that lane's break fan-out. No observed unbounded retention or whole-history cache exists; memory/latency performance remains NOT_RUN. On-demand refinement may improve cost but is not a separately required framework/source blocker.

**F6 recommendation withdrawn.** Keep native signed decimal transport and its existing known/null coherence. Core1252 explicitly constrains included/reserved operands nonnegative, but current2026 ready aggregate can select native selected_source_ex_vat; that field's schema/type does not impose an unsigned transport contract. The card does not authorize a blanket Source/correction sign refusal. A native signed fact must not be rejected merely because it is called an amount. No extra nonnegative guard is required by this source review.

**Focus qualification.** Existing ui.capture returns NULL when activeElement belongs to the deeper child, and actual native frame stack makes parent inert; mount render only restores a non-null token captured inside its own host. No concrete deeper-child focus-steal remains evidenced. Native focus/screens are still NOT_RUN, not a visual PASS claim.

Fresh snapshot:
- `js/bpay-next-workbench.js` — `098a8727a8477b3fc25c5927f4d5810b0151e9d880f797fb5eac758ecedb0944`
- `js/bpay-next-office.js` — `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0`
- `js/bpay-next-ui.js` — `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5`
- `broker/src/bpay-next-workbench.js` — `78917cd474bbd585de010d1b515c8c71a56b87857aafc8f4d7820746636368c1`
- `supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql` — `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a`

## Final-tail reread

R1 is closed in actual factual906 (current after awaited point) and Pay-lines992 (before/after every awaited chunk). Main and shifts now restart from unchanged native original arguments on deliberate upward wheel/touch/key intent when compact history is exhausted but retained first-page after is non-NULL. No extra Load-more/First buttons or retagged cursors are introduced.

**Only remaining R2 child guard:** earlierScroll924 hardcodes the parent `interactive()` predicate. Its Breaks/Pay-lines use938 is inside an active native child; parent Candidate.frame.active() is then false, making that restart permanently unavailable. Accept a narrow per-pane interaction predicate (default parent interactive); pass actual `live() && childWindow.active()` for factual children. Main/shift panes keep their parent predicate. This is an existing native frame seam, not a new paging protocol.

Fresh FE snapshot:
- `js/bpay-next-workbench.js` — `89984d720f2e4b6cbafd1abe6e60e97e4eadf6115a72469adc99ea1122e21192`
- `js/bpay-next-office.js` — `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0`

## Final source closure

Fresh current product closes the last child restart guard: earlierScroll924 accepts canInteract with parent-interactive default; factual child938 passes actual live&&childWindow.active. Main/shift lanes retain the parent predicate; child lanes now work while parent is inactive. Restart preserves original arguments, compact cursor eviction and native scope, with no artificial pager buttons or cursor retagging.

The actual managed-frame lifetime fix is correct: mountPaymentDetail.current901 no longer mistakes native stashed DOM (host.isConnected=false while a deeper child is open) for dismissal. It relies on the authoritative supplied current predicate, whose actual caller binds Candidate frame alive/owner/actor, same original Workbench revision and exact details entry. Controller close remains authoritative and invalidates generation/disposes nested children. This lets genuine child reads continue while preserving stale owner/actor suppression.

RateValue897 marks its exact typed six-precision currency display with existing data-next-formatted-money so global monetary formatting cannot round it again. Negative fractional precision is preserved. Pay-line available schedules are de-duplicated only by their actual schedule UUID identity within the currently drawn bounded pages; this is display identity de-duplication, not money aggregation or latest-rate substitution. Used rates remain actual per-line evidence and separate from available schedules.

All earlier findings are closed, explicitly withdrawn (blanket nonnegative transport ban) or statically qualified (finite nested page/cache upper bounds). No new producer/money/accepted-input framework is needed by this review. The Worker/FE exact field/coherence mirrors, existing environment/actor transports and actual Office preparationTransport join remain intact. Choices are preserved by the native original store; opening/collapsing/detail reads do not mutate financial selection or journal bytes.

**Verdict: SOURCE PASS for this reviewed detail UI/transport/caller subset; no open source finding.** Compilation/source-unit checks and12 screenshot capture are parent-reported separate evidence, not executed or visually certified by this reviewer. Screen/policy/mobile/keyboard actual journeys are owned by the independent capture review. Future named rate-resolution/estimate/snooze/selection joins remain as recorded in card/checklist and are not certified complete by this source receipt.

Final exact snapshot:
- `js/bpay-next-workbench.js` — `4a3e805c42d83398565146fd1c9a200bf10b4b1a77682d7d380e29a00499d6dc`
- `js/bpay-next-office.js` — `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0`
- `js/bpay-next-ui.js` — `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5`
- `broker/src/bpay-next-workbench.js` — `78917cd474bbd585de010d1b515c8c71a56b87857aafc8f4d7820746636368c1`
- `broker/src/bpay-next-preparation.js` — `da8b67652178bbbaa0e0b418d6ef97800e916416c1521e989c631effd02d8fb9`

The following assets are current companion snapshots only; CSS/render policy is reviewed separately:
- `css/bpay-next-office.css` — `6402b5a6a2cd881a770ad0d1e5e2a097f158f8977637c5c453ca365c63f88224`
- `index.html` — `ffaf5d4797b1a0bd0a28c3e2458d09c9e73b0e1280c9b0764a7d750a51a33b22`

## Final presentation/RATES boundary supplement

**SOURCE PASS retained; no new finding.** Fresh narrow source reread confirms the real day header now contains two actual labelled cells spanning6+2 columns, rather than six empty phone-card fields (workbench952). It retains its exact native row-key/amount authority and changes no money. Case copy says Recoverable this Draft; the actual estimate remains its recorded later authority. New rate/unit alignment selectors are scoped to the native read detail table and revert to left alignment inside the same720px phone-card media scope (CSS245–246). This is source/presentation scope confirmation, not independent screenshot certification.

usedRates992 uses the existing factual RATES/point-revalidation transport, original Work/revision/line, fixed five-bucket domain and limit5. If the conservative first reply is nonterminal, it requests at mostone second native after_bucket page. It rejects missing cursor, nonterminal tail, combined count>5 and duplicate bucket. Original transport validates per-page bucket order/shape/identity; helper merges only actual row evidence into the local Pay-line view, not money or a new server receipt. Current after-point actor/owner guards and Pay-line chunk guards apply unchanged, so no additional tail after dismissal/actor loss. No indefinite paging, automatic retry, guessed used row, new RPC/SQL or financial producer is introduced.

Current exact snapshot after these changes:
- `js/bpay-next-workbench.js` — `d05871d6f5b934d7266fa53ff06eb49f9732da6716e3c9baff1bb75deebe7877`
- `js/bpay-next-office.js` — `9466cff351950a54590ffd5419469110e2a07de39a8361cfcb87aa5594519db0`
- `js/bpay-next-ui.js` — `e72d3c6280cd44dfb34346613724f05e0e44f2c4bcf33cdc174862834295ecc5`
- `css/bpay-next-office.css` — `6db1ba1e37e703405d8f98cab474c2e0c6b44156511b4ee0aedb03d5d778c618`
- `index.html` — `ffaf5d4797b1a0bd0a28c3e2458d09c9e73b0e1280c9b0764a7d750a51a33b22`
- `broker/src/bpay-next-workbench.js` — `78917cd474bbd585de010d1b515c8c71a56b87857aafc8f4d7820746636368c1`
- `broker/src/bpay-next-preparation.js` — `da8b67652178bbbaa0e0b418d6ef97800e916416c1521e989c631effd02d8fb9`
