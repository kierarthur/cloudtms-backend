# PF-08 screen investigation — sequence 112

**DESIGN INVESTIGATION ONLY. No A4 approval or joined acceptance claimed.** PF-08-D03 adds Candidate-specific recoverable preparation failures to the existing DR-06 progress window. PF-08-D01/D02 own bounded retry/backoff and the durable needs-attention handoff; D04 remains a future joined Worker/queue/minute-tick journey. Dependencies are PF-07 and F-16.

## Supplied design and owning scope

The read-only pack `SCREEN_DESIGN_POLICY_20261010/SCREEN_INVENTORY.md` row SCR-40 and `DESIGN_INDEX.md` map PF-08 to **B4 / T07b**:

- `batches/B4/specs/SCR-40_draft_progress.json`
- `batches/B4/png/SCR-40_draft_progress_dark.png`
- `batches/B4/png/SCR-40_draft_progress_phone_dark.png`

Both supplied PNGs were individually viewed. The spec's overlay contains one overall progress bar, stored Done / To review / Still to do counts, and a Candidate / Pay channel / What is happening / Actions table. The failure row has a reason and **Try again**; the waiting row has no retry action. The supplied examples use Grace Hall, Harry Evans and Ethan Ward, not required production values. SCR-40b ready and SCR-40e refresh-failed are adjacent existing-host states, not new PF-08 pages.

The full owning card is `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1830–1851`; the current checklist's ITEM and D01–D04 target bounded automatic retries, isolation and visible recovery. The precise D03 target is `js/bpay-next-office.js :: DR-06 progress Needs attention/Retry`. A row action must retry the **same captured Candidate job** after automatic tries stop. It must not recapture current Work or use DR-05 `WORKER_REPREPARE`, which changes preparation revision/allocation through its separate Review flow.

## Current source and minimum remaining screen change

`js/bpay-next-office.js` currently opens `openBpayNextPreparationProgress` in the native medium window and reads `PROGRESS` while visible. It already guards the current actor, NEXT namespace, parent/window lifetime and exact run; preserves journal recovery; and renders truthful read failures separately from successful progress facts.

The current closed `PROGRESS` row contains aggregate status/counts, starter/time/deadline, enrolment and discard facts. Current `WORKERS` contains worker/Candidate UUIDs and outcome money/issue fields. **Neither supplies a human Candidate name/reference, PF-08 job identity/retry eligibility, or exhausted-retry reason.** Do not substitute UUIDs or repeated “Candidate” labels for the supplied “who” column. The implementation must identify the smallest native PF-08-owned typed read support for these actual facts; the supplied design does not itself authorize extra server fields. If human identity remains assigned to later DR-15, explicitly defer full “who” approval instead of fabricating a pass.

The narrow UI delta is an inline **Candidates that need attention** list within the existing progress body, using native universal-table/card rows and native badge tones. Each row needs only its actual identity/channel, a finite safe reason/state, and **Try again** when the existing job owner says it is eligible. Use the existing command route and retained request/unknown-outcome semantics; no new page, financial command, money owner or browser retry scheduler. Keep the progress aggregate sourced from stored reader counts.

Existing **More → Continue processing** wakes the Draft's background work; it is not a replacement for the Candidate-specific job retry. Existing error-line **Try again** retries readback/journal lookup; distinguish it from a row's preparation retry. Existing **Review payment issue** and Continue to review retain their DR-05/DR-06 ownership. Do not add the supplied waiting-on-another-Draft row, exclusions Timesheet detail/count, full Review table, or invented Draft/person/date values before their owning cards/readers provide them.

## Required evidence before A4

Capture the actual native progress host in desktop dark, device-light and 390px phone modes using the existing local fixed-response approach. Minimum states: no attention rows; bounded automatic retry still running; exhausted eligible row with reason; retry sending with a visible disabled reason; accepted same-identity retry; failed retry; unknown outcome with retained request recovery; recovered current read; read failure preserving stale facts; a second Candidate continuing while the affected row remains isolated. Inspect phone top and scrolled row controls, native footer, More placement, 44px touch targets and no horizontal overflow. These rendered local samples do not prove the future joined 300-Candidate acceptance journey.

Every actual screenshot must be viewed next to its supplied PNG. Record every visible difference and every control's payroll purpose; fix unexplained differences. Owner A3g overrides supplied multi-sentence example errors with one plain sentence. Judge all 42 policy rows after actual captures; wording row 16 is manual against `tools/policy.json`, rows 23/24 use actual PNG evidence, and owner-excluded spec-generation row 25 / readerAdditions row 26 are N/A. Preserve prior SCR-40 DR-06/DR-08/DR-09 review history when adding the PF-08 receipt.

## Exact seams, controls and required differences

Current FE seams are `ROWS.PROGRESS` at line 178 and its exact-envelope/count validator at 635–653; preparation request/receipt maps around 130–168; `openBpayNextPreparationProgress` at 1619–1708; and scoped progress styles at `css/bpay-next-office.css:472–531`. BE `broker/src/bpay-next-preparation.js` mirrors the closed page/request contracts. Extend only the named PF-08 native support once its exact job-retry semantics are defined; do not bypass strict keys or call generic financial/job APIs from the browser. A retry must preserve the accepted run/Candidate/job identity and retained same-request recovery, ignore late results from an old actor/window, and refresh actual current facts before enabling another action. Success means processing was requested, not that the Candidate was paid or prepared.

| Control | Payroll purpose and minimum rule |
|---|---|
| Row Try again | Restart only the eligible Candidate's stopped automatic preparation attempts with the same accepted payments; quiet row button, no global wake or current Work recapture. Keep the row reason and visible busy/unknown state next to it. |
| Refresh | Re-read current progress and attention facts; never silently dispatch processing. |
| More → Continue processing | Existing rare Draft-wide wake; keep separate from row retry and preserve current unknown-result recovery. |
| Continue to review | Existing primary next step; disabled with its truthful reason until the stored reader says building finished. |
| Review payment issue | Existing DR-05 handoff only when its actual reader condition applies; do not duplicate it as a new PF-08 page. |
| Discard this Draft | Existing destructive left-side action, with its own confirmation and exact native result; unchanged PF-08 financial authority. |
| Header Close / Escape | Close safely while background work continues; no duplicate footer Close. |

Required differences to record beside the supplied PNGs: real stored sample counts/actor/start/deadline instead of illustrated values; no fabricated Candidate name/TMS reference; only PF-08-owned failure rows instead of future DR-14 waiting/DR-15 exclusions; row retry busy/unknown/recovery states that the static example does not show; no raw retry count, due timestamp, error code, job/lease vocabulary or elapsed counter. The attention rows are a progress-dialog/result list, **sorting-exempt under SDP-34**; do not invent server sorting keys or a cosmetic loaded-row sort to reproduce the illustrated What is happening arrow/phone Sort by. Document omission of those pictured controls using that explicit policy exemption.

The existing actual local helper is **BE `.codex-local/DR06-screenshots.cjs`**, with `DR06-phone-menu.cjs` and `DR06-stale-layout.cjs` for phone menu/layout coverage. It loads saved FE Office/native window code, blocks all nonlocal requests, supplies finite responses through `authFetch`, and uses two native layout frames before screenshot. Root can reuse this capture seam for the PF-08 final states; this reviewer only read it. No new design spec or pack renderer is needed. Capture state combinations may share a screenshot when all named visual obligations are visible; they are not a new business test programme.

Policy version 1.2 and all 42 checklist rows were read, together with `tools/policy.json` never-show/chatter/format/vocabulary/rewrite lists. Final A4 must verify native frame and five tones, sticky headings/phone cards, 44px targets, visible keyboard focus, live status/alert roles, retained unknown/read errors and truthful row feedback. Source review cannot substitute for the actual PNG comparisons.

## Investigation source identity (SHA-256)

| File | Hash |
|---|---|
| FE `js/bpay-next-office.js` | `2a53ec2adb75c94e9aea95321b8f372882142db0b948bee2f0f535a48f650314` |
| FE `css/bpay-next-office.css` | `ce94fea2bed38497172819488f0346c59adeba2e03ddc303b6122bbfab858d1d` |
| BE `broker/src/bpay-next-preparation.js` | `3773d8bd0bf0c659ba4aced7f0a66440d228ec4fbcde7cdf6b07b51e67892c26` |
| BE `.codex-local/DR06-screenshots.cjs` | `d266345b7f6ff9e21125d179917bf15b7218d7a811e7ba1d8881797d58b379ee` |
| Supplied SCR-40 spec | `f25cbb54c53551ce5a56e75c1bb48bcf07f6180e8d9cc16443dbfd38f927730d` |
| Supplied SCR-40 dark PNG | `01b663672caece7f9411b40329630988976dbfdb820ff3f7bd726fdfcf75ecff` |
| Supplied SCR-40 phone PNG | `31c394b7f980d886a1737d8e2191109e0f721760ca946215d12d32228a9f1f6b` |
| `SCREEN_DESIGN_POLICY.md` | `8bc212b174469236f2b46dbf70eadc3664a946636458321cd3c77b7146323de5` |
| `tools/policy.json` | `45036d841d4568aa2d4d7adf4f063d6d17b70288e289ee0b285068797a87369d` |

No product, pack, SQL, business data, tests, render/capture tools, hosted services or commits were changed or run by this reviewer.
