# DY-05 screen scope review

11 October 2026. **Scope findings only; product and A4 capture approval pending.** Read-only inspection of the complete DY-05 card, every checklist row, current frozen-view source, indexed supplied specs and nine supplied PNGs. No product/spec/pack edit, generator, renderer, screenshot tool, test, SQL, database, fixture, browser or hosted action. Only this note is written.

## Authority and references

Full card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:982–1006`; D01–D04 `IMPLEMENTATION_CHECKLIST.md:2722–2741`; dependencies DY04/F06/F08/F16; sequence92; current native owners and owner A2/A3/A4 plus the 11 October screen corrections. The specific Screen bullet997 is **“No Contract (Daily)” where Contract is NULL in NEXT frozen detail**. The complete card also requires preserved captured Client/role/band/window/breaks/used and available rates, HOURS-only Daily, and original physical identity. A sample design does not authorise unrelated reader fields or later-card behavior.

Pack root (read-only): `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`. Inventory98 maps SCR47 jointly to DR17/WB20/SP22/DY05/CM19; inventory237 maps SCR156 jointly to DY05/DY10/WB04. Individually viewed:
- B4 `SCR-47_draft_detail_lines_dark.png`, phone version, `SCR-47b_draft_detail_shifts_dark.png`, `SCR-47c_draft_detail_breaks_dark.png`, `SCR-47d_draft_detail_used_rates_dark.png`, `SCR-47e_draft_detail_available_rates_dark.png`, and all five matching JSON specs.
- B10 `SCR-156_daily_labels_dark.png`, phone version and `SCR-156b_daily_blocker_dark.png`, with both matching specs.

Current audit explicitly removes “Rate set by” from47d and Additional/mileage from47e (`AUDIT_ROOT_B4.md:176–180`). Do not restore removed fields. The pack is a reference, not source to regenerate/copy under DY05.

## Current source and renderability

`FE js/bpay-next-office.js:120–131,404–423` already validates four exact frozen reader kinds: SHIFTS, BREAKS, RATE_DETAILS and RATE_SCHEDULE. They bind run_line/run_work IDs. Current SHIFTS carries date/clocks/quantities/bucket hours; BREAKS carries genuine clocks or duration; used rates carry bucket/hours/source pay; available rates carry all stored channel/charge cells. They carry **no Client/Contract/origin/role/band/Timesheet display context**. LINES88 and REVISION_COMPARISON89 likewise carry IDs/amounts, not that display context.

The old inline view1357–1362,1436–1442 uses those readers, but displays raw revision/component identifiers, multiple “Next …” buttons and explanation text inside the thin action sheet. The real completed-preparation path1170 goes to `openWorkerReview`1002, which does not expose that inline frozen viewer. Merely adding the text to the thin sheet or using a fixed screenshot bypass is not proof the required label is reachable.

DY05 can render a **bounded read-only frozen Work/line child** without waiting for DR15's whole-worker name fields: the captured revision already stores Candidate name/reference, Client name, job/band and Timesheet reference (`BE core1324:158–163; publisher0939:388–402`). Use the actual captured revision and native Client-origin identity promised by D02; expose only the exact closed fields needed through the existing frozen read boundary. A NULL alone is not proof of DAILY origin; the label requires a genuine captured DAILY/NULL-Contract identity. Do not fetch current Client/Contract/Timesheet names or infer identity from IDs/date/display text.

Full SCR47 whole-Candidate aggregate lists, its complete review header/subtitle and integration across every Work remain DR15/DR17/shared owners. Those omissions must remain named/deferred; they are not blanket approval of a generic Candidate header. A reachable DY05 child can show the captured Work's own human context from its revision while preserving that later boundary.

## Narrow complete UI recommendation

Reuse `createStandardWindowAdapter` in `js/bpay-next-ui.js` and the existing frozen transport, not another overlay/controller or a new public command. The existing `js/bpay-next-workbench.js:1250–1260` factual child supplies reusable geometry, labelled scroll region, bounded three-page retention, late-read fences and in-place retry behavior. Reuse presentation/scrolling only: its current-memory/current-revision factual qualification must not replace the frozen run_line/run_work binding.

Open from the actual Draft/Candidate context, preserve parent tab/scroll/ticks, and return through header Close. Bind every read/result to actor, owner, child lifecycle/generation and the original captured identity. Use genuine captured **Daily · booked date**, Client, **No Contract (Daily)**, role/band and Timesheet reference where supplied; retain contracted labels. Exact clocks, overnight facts, recorded breaks, approved hours/buckets and native money stay reader facts. **Approved hours, not paid hours** avoids equating approved quantity with the paid residual. Hide raw UUIDs, component keys and revision IDs from payroll copy.

Native Standard order is Day/Night/Saturday/Sunday/Bank holiday. All fifteen genuine available cells remain, including “—” for NULL; never substitute zero, convert channels or calculate totals. A used charge value shown by47d must come from actual captured used-rate authority, not assumed from an available rate. Do not invent a missing column merely to match a reference. Certified zero is factual £0.00; failed/unavailable read is a plain state or “—”.

SCR156's current Candidate structure is already owned by WB15/WB27/WB38. Retain its native server sorts, ticks, caret rules, inline Open Timesheet/More, existing estimate state and one primary Show estimate/secondary CSV. Its decorative “Deductions due” total is superseded by the already-recorded owner correction; no reader supplies that total. Do not recreate it. Exact captured Daily/date/No Contract label can be consumed where an existing native row supplies its identity; if absent, identify that exact transport gap before editing. SCR156b's TSFIN failure/Retry belongs DY10, not a new DY05 recovery command or fictitious blocked receipt.

## Lists, controls and A4 boundary

Policy164 overrides UI paging: any newly touched frozen list uses automatic continuous scroll with the same finite server keyset chunks (at most100), a loading-more status, bounded cache, retry and preserved scroll. No Previous/Next/page-number/Load more controls. Do not fetch all Candidate membership or sort loaded rows.

Policy172 exempts dialogs, expanded child rows, results and fixed business bucket order from sorting; inventory explicitly describes SCR47 as dialogs. A narrowly scoped read-only child can keep original native deterministic detail/bucket order without a new sort protocol/index. If root instead implements a top-level whole-Draft/whole-Candidate list or promises the supplied Lines/Shifts/Breaks sortable table, each displayed non-action data column needs an exact server sort operand, stable keyset tie-break and matching index; present inputs only have natural detail/line/rate cursor keys. This larger list is not required just to deliver DY05's label.

Every visible control needs a payroll purpose: header Close returns to parent; existing detail navigation selects genuine frozen facts; Break detail opens exact recorded breaks; Used rates and Available rates answer distinct questions; Try again repeats the failed read. No ticks, rate edit, calculate, pay-date picker, print/export/new execution action or unnecessary footer primary belongs inside the read-only child. Parent controls retain their own ownership.

Actual A4 review must compare saved product PNGs beside these references and account for every difference: current sample identity/rows, narrower per-Work scope, actual available fields, omitted later whole-review header/list, original native order rather than unsupported sort arrows, retained current Candidate controls, approved wording and dark-only native frame. Dark desktop,390px phone and device-light (still dark) must be inspected, with keyboard/focus,44px phone targets, sticky headings/right-tabular money, no clipping/sideways scroll, missing/loading/error/empty states and late-read recovery. Apply all42 rows (`SCREEN_DESIGN_POLICY.md:396–459`); state N/A/deferred reasons individually. Source investigation alone cannot mark these screen criteria PASS.

## Saved source/reference hashes

| Input | Raw SHA-256 |
| --- | --- |
| FE Office | 8b9e437170c1cb015d13391447d3fb20aa39b37718de9040dc0e5051097abdf3 |
| FE Workbench | f01b5540b281e9ee30b45098811f6ec7c8067ddad93f9247faa4bea760990e3e |
| FE window adapter | a4ae919379621a21e68ef9955bc4a8cebb5e37f20ede56691eef7a16b60d0ee5 |
| Screen policy | 8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5 |
| Screen inventory | 3b9b035de99847483c0111fd689dd2fc6d9f9589758f02744ec7b028c6727b2c |
| Design index | cba0ef6e87c8937d558b20ef9c9ee010fcc9f9a8b479b626351206d07076cffe |
| Agreed beyond plan | bf53ff40ca527535b54e152c945541797ccb3c11e8c2edfb23dca5a6f484c38c |
| SCR47b spec / PNG | f63926425a079ab66a63db8a3219eda1e623547597236dfd863d9c45431c1ee8 / 588929ee2eca5695d4f497b1db78986f2238db012b755d735b6f0f9c51d83456 |
| SCR156 spec / PNG | 11dc3c13a2f7846e5140d8fc835362e92e49b389a0e1f71a4d6822bdd7e94b24 / 3825989b38740730b127c610e9b0d2498d98575037f765977298610203a2b48f |

Joined genuine payment/rename/rotation, immutable history, two-channel2/2 enrollment and retry remain AUTHORED_NOT_RUN. No screen delivery is marked built or approved here.

