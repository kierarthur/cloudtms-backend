# DR-15 screen review seams — sequence 119

12 October 2026. Read-only design investigation of card 03:403–423, all DR-15 checklist rows, BD132 (`DR-15-build.md`), `DR-15-PREPARE_ISSUES.json`, the current native Office seams and the supplied B4 references. This is an implementation boundary and screenshot acceptance plan, not A4 pixel approval. Root alone authors product. No capture, renderer, pack edit, business request, test or database action was performed.

## Supplied appearance and ownership

The read-only pack is `cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010`. Exact references:

| Reference | DR-15 use |
| --- | --- |
| `batches/B4/specs/SCR-46_skipped_timesheets.json`; `batches/B4/png/SCR-46_skipped_timesheets_dark.png` and `_phone_dark.png` | Owning Not in this Draft child: Candidate, Payment, Reason, Actions; Candidate A–Z by default; no selection ticks or monetary columns. |
| `batches/B4/specs/SCR-45b_reason_wording.json`; matching `_dark.png` | Plain reason vocabulary; actual SP15 review remedies and owner E/A rulings take precedence where the old illustration differs. |
| `batches/B4/specs/SCR-40_draft_progress.json`; matching `_dark.png` and `_phone_dark.png` | Existing progress host: one information line and Not in this Draft opening control, separate from genuine preparation attention. |
| `batches/B4/specs/SCR-44_draft_review.json`; matching `_dark.png`, `_devicelight.png`, `_phone_dark.png` | Parent review placement and omission information. Its future monetary composition remains DR-16 ownership. |
| `batches/B4/specs/SCR-43c_review_row_results.json`; matching `_dark.png` | Existing review/action host and retained action outcomes; no replacement journal or confirmation protocol. |

Supplied SCR-46 dark and phone PNGs, SCR-40 dark and SCR-44 dark were opened as images. SCR-46 uses a medium native frame, title plus factual actor/start subtitle, header Close, one rounded native table with quiet uppercase headers and useful wrapped reasons. Its body ends with whole-list count and sort status; it has no footer action rail. Phone uses labelled rows, a real sort control, one Open action per navigable Timesheet and viewport scrolling. The illustrated people, references, amounts, dates and counts are samples, not permission to invent data.

## Smallest complete layout and state mapping

| Native fact/state | Required presentation and control |
| --- | --- |
| Actual REVIEW worker | To review tone, factual Candidate/Payment and one plain reason; original qualified Try again / Remove from Draft controls only where supported. Preserve their original unknown-outcome recovery. |
| PENDING / missing worker | Truthful waiting state and available selection identity; retain the row. Do not synthesize PREPARING success, a Worker ID or an actionable retry. |
| READY | Ready state and actual accepted facts; no omitted-row remedy or new financial summary. |
| Native cleanup still CANCELLING | Removing/cleanup truth. Do not say the payments are already back in Banking Pay. |
| Completed automatic removal, E1 selection skip or other retained omission | Not in this Draft information. It must not count as unresolved To review or block Confirm. Only claim returned payments when native completion proves it. |
| Unknown well-shaped preparation code | Keep the code in the typed reply/journal, retain the affected row, show the one-sentence Contact support reason; no technical code on screen under owner A3g. |
| Missing historical label/physical identity | Preserve the row and plain unavailable identity. Use captured Timesheet/reference/week only when supplied; do not substitute newer CURRENT_WORK labels or a UUID as a person's name. Omit unsupported Open. |
| Failed read / retained read | One visible error before retained facts and the owned Retry/Refresh path; errors use alert, loading/recovery use status. Never show an empty success or zero-count result from a failed read. |
| Empty successful list | Plain truthful empty state without money or a fabricated row; no blank footer rail. |

Use the single closed PREPARE_ISSUES display manifest in the existing Office host. Native exact captured labels and source identities remain authoritative. Case-only qualified labels follow BD132; genuinely unbound historical identity stays unavailable. Later changes after Create Draft are information, never a new stale-approval REVIEW. Retain E2 automatic release, E3 hold exclusion, E4 real-failure To review and E5 original retry semantics; never reintroduce PAY_ON_HOLD capture REVIEW.

The one child is opened from progress and Draft review. The information line uses the reader's whole-scope skipped-Timesheet and completed system-released-Candidate counts, not loaded page length; use singular/plural correctly. Keep it separate from the warning for actual To review Candidates. Do not add missing parent gross/deduction/net/channel facts owned by later cards.

SCR-46 has no ticks, bulk action, money field, new Source Checks command or obligatory primary button. Candidate/Payment/Reason native sorting and continuous scrolling are appropriate; if exposed, every sort must apply to the whole reader scope with stable native identity ties. Do not present reason-code ordering as plain-reason ordering. The small-dialog SDP34 exemption remains available. Keep the factual sort/count footer inside the child body; no Load more button or visible cursor.

## Concrete copy finding

**DR15-S01 — required before A4:** current manifest entries `UMBRELLA_WORK_EXECUTION_UNSUPPORTED` and `CASE_TARGET_UNSUPPORTED` contain “pay method”. `tools/policy.json` forbids that phrase (SDP71, checklist item16). Suggested truthful alternatives are “This Timesheet cannot be paid this way; check the Timesheet.” and “The selected payment cannot be paid this way; contact support.” Use “pay channel” only if that exact refusal proves a channel cause. This is a finite copy correction, not a new error or feature. Parent notified.

SP15's existing review remedy remains exact. Not in this Draft may use the retained short omission reason. One sentence per new error under owner A3g, no raw code, stale generic “Review required”, or ambiguous zero. All other visible differences from the supplied illustration must be fixed or explicitly tied to the card/owner/actual reader before approval.

## Screenshot acceptance list

Root should supply actual saved native screens with small local fixed read replies, covering distinct changed states rather than duplicating unaffected old confirmations:

1. Progress and Draft review with the native whole-scope omission information/link; actual REVIEW and PENDING alongside completed omissions, demonstrating the latter do not create a To review blocker.
2. SCR-46 mixed Timesheet and Case-only omissions: representative hold, snooze, not-yet-payable, same-week PAYE, changed-after-click, Source query, WAIT/signed refusal and completed automatic removal. Source review covers the complete finite code census.
3. Genuine non-E2 preparation failure and unknown well-shaped code; affected captured identity, plain Contact support truth, original qualified actions only.
4. No-worker pending / failed enrolment and unavailable historical labels; no dropped rows, fabricated name or unsupported Open action.
5. Successful empty list, loading/continuation, initial failure, retained read failure/recovery and stale scope/actor rejection. Preserve original action recovery if that rendering is touched.
6. Dark, device-light and 390px phone for each changed rendered family; phone scrolled evidence for the last row, wrapped reason, actual Open and footer/count/sort information. Phone Open follows owner A3j; header Close and all exposed touch controls meet 44px.

A4 must inspect actual PNGs beside the read-only supplied PNGs, record every visible difference and each control's payroll purpose, then save all42 policy results. Item16 uses the manual policy wordlists, items23/24 require real image evidence, and owner-exempt item25 / readerAdditions26 are N/A. Check one useful table/frame, no clipped menu/actions, correct tones, no duplicated facts, quiet headers, currency only where actual facts exist, disabled reasons, accessible status/error roles and no blank empty footer. No screenshot approval is claimed here. Native child/business interaction, live queue, large-list performance, runtime acceptance and hosted behaviour remain NOT_RUN.
