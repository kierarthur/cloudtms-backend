# FC-14 independent screen source observations

Status: SOURCE OBSERVATIONS SAVED; A4 PENDING actual captures. Reviewed 11/10/2026 against full FC-14 card, ITEM/D01–D07, BD121, supplied B9 SCR-126 and B8 SCR-111/111b specs, full SCREEN_DESIGN_POLICY and manual tools/policy.json wording lists. Product root-only; lifecycle/cache correctness has a separate independent reviewer.

The saved native module reuses the standard read window, native header Close, bounded GET-only CASE_AUDIT/CANDIDATE_CASE_REPORT reads, exact DTO validator and continuous cursor scrolling. Audit starts When oldest first; report starts Next date soonest first, Active only on, with fixed business sections. Each data heading requests server sorting; phone has Sort by and labelled facts. Whole-list count and report summaries are native reader values. Funded/recovered/written-off/reserved facts are card-required additions, not new financial calculations. Audit/phone Open navigate actual Case/Timesheet identities. No new financial action, journal, Export or Refresh control is added.

Presentation observations requiring real-pixel verification before approval:

- S14-01: money spans, summary/balance dd currently lack data-next-formatted-money; native universal money formatting may strip thousands separators or coerce exact decimals. Confirm real large/negative/nullable money pixels.
- S14-02: headerDetail and empty loading-more sentinel currently lack data-next-live-copy. Existing modernizer removes empty decorative/text nodes; verify human subtitle and later Loading more… survive.
- S14-03: report STATUS currently renders plain span instead of supplied semantic Active/Snoozed badges. Actual states must use the native five tones with text.
- S14-04: table currently lacks the native universal classes/zebra and specifies 10px headings/12px host radius instead of policy 11px/13px. Verify and correct unrequired appearance differences rather than approving unexplained differences.
- S14-05: failed/read-loading windows initially receive no header reader, so human Candidate identity may be absent. Preserve only qualified native seed identity when present; do not invent person/reference data.

Manual wording source observations: fixed failure messages are one plain sentence with next action; money/date/time formatting uses pound/pence, separator, true Unicode minus, UK London time and em dash for missing. Dynamic event/source/details require the same manual vocabulary review on actual native sample captures. No wording/render tool run. No pixel, joined/native-parent, financial, database or runtime acceptance is claimed.

Initial actual PNG findings saved in SCR-126/111/111b review.md: P14-01 human subtitle missing or inline (audit phone squeezes title to one letter per line); P14-02 redundant desktop phone sorter; P14-03 status badge absent; P14-04 failed empty table/filter/sort and secondary Retry; P14-05 phone Close appears below44. Source-only P14-06: row keyboard handler currently implements Enter but no ArrowUp/ArrowDown navigation required by SDP-77. Changes/captures are underway; these findings are not final current-byte defects until rechecked.

Follow-up source P14-07 (reported immediately): intermediate FC14 CSS introduces numerous new !important declarations. Policy SDP-20/checklist11 explicitly prohibit them. Use native classes/adequate scoped specificity or a narrowly required native adapter exception, then recheck final appearance. Semantic report badges must reuse candidate-office-badge native tones. Current money/live-copy markers, Target inc VAT label and region Arrow/Home/End navigation repair the corresponding source observations; final screenshot verification remains pending.

Follow-up P14-08 (factual label): native report source2129:249 emits TIMESHEET_SNOOZE.created_at_utc from the snooze record. Generic Created beneath Timesheet implies Timesheet creation; use Snoozed or omit that subline, keeping Created for CASE.

Capture-only D14-01: initial fixed CREATE_REQUEST/Created and CASE_EVENT/SYSTEM tuples differ from the actual current2129 label/actor branches. Creation requested is accepted request truth; Created belongs to CASE_EVENT/APPROVED. CASE_EVENT actors are HUMAN or UNKNOWN in this current source. Align local GET-only samples with native row semantics; do not claim absent automatic actor evidence. No new SQL/business data is requested by this capture finding.

Final-pass follow-up P14-09 (reported immediately): genuine empty audit/report phone states still show a thin outlined empty table region, while desktop keeps redundant header-only tables. Hide the completed empty region; preserve the one empty-state line, true zero count/summary, and report Active only filter. SDP-59/checklist7/34 and the owner's every-visible-element review do not give the empty phone strip a payroll purpose. Remaining inspected final states have no newly identified essential visual failure; closure awaits this small corrected capture.

Initial source SHA-256 snapshot (superseded by final review, never used as final capture provenance):

| File | SHA-256 |
| --- | --- |
| js/bpay-next-case-audit.js | 85e4ee5f58149cd606a8fd68a3d71d551963b9a13ed9ba8c2cae7af3fe8d3aaa |
| js/bpay-next-case-audit-contract.js | 145ce37b807cc31cf6d2e3f2b6e5672a4110a61c59615562d8966484943a8eff |
| js/bpay-next-office.js | 2a53ec2adb75c94e9aea95321b8f372882142db0b948bee2f0f535a48f650314 |
| js/main.js | 358b817d22ca9b0affaeb210486d655600595e60628afdbf967ccf0cedc072e8 |
| css/bpay-next-office.css | 84ee66dab1fdad5216cfe35f87db590d0806dc026a9c6346354bd384cb9d632f |
| js/bpay-next-ui.js | f535e7e75579428ca7b97b57d406ccaeadaebbf0fe15e5935574f435b772248d |
| index.html | 8f4561ad8b84ae49be7793662bbe8e2e4480bc0aaa49bba64dc74eaefa68913f |
