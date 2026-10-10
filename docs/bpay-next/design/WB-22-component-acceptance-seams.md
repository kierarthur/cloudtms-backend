# WB-22 component acceptance seams

10 October 2026. Read-only findings against the full WB-22 card/checklist, current WB-22-selection-memory.md (BD077), saved final-definer registry and the actual native Create/component/segment definitions. No product edits or execution.

**The proposed pre-descriptor implicit SUBSET is supported by the existing component owners, with one required counter publication before component seal.** No component/segment monetary owner successor is needed merely to store the server-derived selected approved-line set. Preserve the old descriptor/upload branch independently.

## Actual controlling owners

| Exact signature | Current final physical source |
| --- | --- |
| private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) | 10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql:114 |
| private.bpay_next_create_preparation_v1(uuid,uuid,date) | 10102026_1621_banking_pay_next_preparation_pay_date_v1.sql |
| private.bpay_next_append_component_selection_page_v1(uuid,uuid,uuid,bigint,uuid[]) | 1832:936 |
| private.bpay_next_seal_component_selection_v1(uuid,uuid,uuid,bigint,bigint) | 1832:1005 |
| private.bpay_next_materialise_choice_segments_v1(uuid,uuid) | 1832:515 |
| private.bpay_next_work_choice_guard_v1() | 10102026_1236_banking_pay_next_segment_authority_final_v1.sql:1023 |
| private.bpay_next_append_segment_selection_page_v1(uuid,uuid,uuid,bigint,uuid,text[]) | 1236:236 |
| private.bpay_next_seal_segment_selection_v1(uuid,uuid,uuid,bigint,bigint) | 1236:293 |
| private.bpay_next_seal_selection_v1(uuid,bigint,bigint) | 10102026_2232_banking_pay_next_selection_pages_v1.sql |

Paths above are under supabase/repeatable. Registry is current saved docs/bpay-next/final-definer-closures.json; do not select older 0570/1100/1832 seal bodies over their listed successors.

## Genuine Create authority and preholds

1832:199–211 has the original pure retained-descriptor replay before fresh Source admission/locks. Fresh admission is 212–215. Sorted scoped Candidate and Umbrella SHARE locks, Work SHARE locks and revision SHARE locks, and scoped Case-component SHARE locks occur at 218–256, including all scoped selectable Ready parents regardless of ALL_MATCHING/NONE. Extra deliberate raw Work identities are also included. Do not move memory capture or new basis reads into the replay path.

Existing earlier-command checks bracket the original command-clock UPDATE at 257–266; same-run acquisition is NOWAIT at 270. The original three-argument run constructor remains at 274. Scope/context checks and real subordinate native identity validation occur at 294–328. The existing held-parent closure at 338–347 refuses any scoped membership that acquired an unheld Candidate/Work/revision/Case-component. Preserve that closure for server-derived defaults and all memory rows too.

Work membership is one current coherent statement at 349–370; SUBSET members are intentionally ordered before ALL, then Work UUID. The chosen actual revision must remain the retained expected_revision_id, not today's later pointer. Case membership remains its original payment-Candidate qualification at 371–405. E1 checks at 407–462 retain approval/current+applied revision, future-week, native CURRENT TSFIN pay_on_hold, the genuine run snooze event cut, worker basis and Source query gate.

The existing SUBSET signed-line test at 448–462 consults raw descriptor component choices. A server-derived implicit subset has no such raw vector: this one named Create branch must test its actual server-derived positive native approved-line set instead. Use the same READY/selectable/known PAYMENT AVAILABLE-or-ZERO predicates and retained revision identity. Do not apply a generic negative-money ban, add segment overrides, or turn an empty chosen set into an ALL choice.

## Required exact insertion/counter order

1. Derive the original selected approved-line IDs while the original current parents/context are held. Keep all raw intent for memory separately; an inherited/implicit selection must not be expanded into a replacement browser descriptor.
2. For each non-skipped selected Work, preserve original run_selection and work_choice inserts (1832:469–473). A genuine SUBSET starts OPEN with page_count=component_count=0. Preserve the original raw query snapshot insert (474–476) and later captured Candidate settings (480–489).
3. Once the actual native work_choice rows exist, publish genuine run work_choice_count before the first component seal. The additive component migration 04102026_1241_bpay_next_component_selection.sql:10–12 has CHECK sealed_work_choice_count<=work_choice_count. Original Create leaves work_choice_count at zero until 1832:499–502; unchanged component seal increments sealed_work_choice_count at 1038. Therefore append+seal before that count publication fails for the first implicit subset. Setting the genuine current work_choice_count and the actual currently SEALED choice count is sufficient; retain final original counter recomputation. No counter may be guessed from memory rows/skipped members.
4. Call original append_component_selection_page_v1 for deterministic distinct approved-line IDs in nonempty native pages of **at most 100 IDs each**, page numbers 1..N. The array is one-dimensional, lower bound 1, no nulls, 1..100 entries (946–950). This is a per-page bound, not permission for a 100-page/all-ID browser array. Use stable native line order and bounded keyset enumeration.
5. Call original seal_component_selection_v1 with exact actual page_count/component_count while accepted_choice_descriptor is still null. It demands positive page/count and exact retained revision/mode/counters, sets the choice SEALED and increments the run sealed counter once (1012–1038).
6. Install the original descriptor/command pair and genuine final counters through the existing Create owner. Explicitly call unchanged materialise_choice_segments_v1 for each newly server-sealed implicit subset, as the root design specifies. Pre-descriptor component seal intentionally does not call it (1039–1040). This must finish in the same accepted Create transaction, before return/matching/overall selection seal.
7. Preserve final original Workbench/adoption context closure, single PREPARE receive/no-command empty behavior, selected-Candidate capture and original return envelope. Any context/basis/native failure rolls back all the new pages and memory capture; no asynchronous financial recapture is introduced.

The original run guards permit this OPEN/PREPARING metadata progression: accepted_choice_guard only freezes the installed descriptor pair (1832:10–19); preparation_expiry_guard freezes created/deadline and validates CANCELLING transitions (1845–1860); segment_run_boundary freezes the original snooze cut and checks segment completeness only when overall selection changes OPEN→SEALED (1236:173–189). There is no need to weaken these guards.

## Native component/segment constraints retained

The work-choice FK requires its exact native run_selection and (Work, revision) parent. SUBSET must start OPEN/zero counts and can become SEALED only with positive matching page/count (1241 migration:14–31). Selected component PK/unique keys prevent duplicate approved lines, component keys, ordinals and page slots. Composite FK binds (revision, approved_line_id, component_key) to the original approved line. The unchanged 1236 work_choice_guard:1066–1100 verifies every appended page's first ordinal, row cardinality and counters and refuses non-choice UPDATE/DELETE. It is compatible with pre-descriptor use.

Before the descriptor, component append uses the original actual approved/current/applied Work check (1832:981–983). After the descriptor, it accepts only raw explicitly selected descriptor IDs (975–980), so delayed appending of inferred IDs would fail and must not be attempted. Similarly, post-descriptor first-time component seal compares raw selected descriptor count (1032–1036). Genuine SEALED replay returns before that comparison (1025–1027): Office can acknowledge/replay a server-sealed implicit subset from retained native counts without creating raw component IDs.

materialise_choice_segments_v1:523–540 returns when already segment-sealed; otherwise it requires the genuine SEALED choice and zero initial segment counters. It walks immutable approved_segment by C-collated stable segment_key in pages of 100, restricted through selected_component.component_key for SUBSET. It invokes the current native segment append/seal owners unchanged. Append records INCLUDED/SNOOZED from the run's original control sequence/business date; complete segment seal requires the entire selected-component segment inventory and exact counts (1236:315–324). Do not seal only unsnoozed segments or omit negative/zero native members.

No-segment revisions have genuine segment_sealed_count=0 supplied by the existing insert boundary (1236:203–208); its sealed-count trigger accounts for them once (219–234). Snooze IDs/version in selected_segment are typed retained metadata with consistency checks, not an FK to mutable Snooze (1100 segment migration:61–73). Immutable approved segment facts reject UPDATE/DELETE/TRUNCATE and insertion into an already sealed revision (1236:106–170). Generated pages introduce existing native selected-component/segment parents, not a new monetary snapshot or command kind.

## Source receipt

| Saved file | SHA-256 |
| --- | --- |
| 10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql | b629fcc003b76ae61ec58c0d35ad2256a4cf9f736e307f5dc80e0a8fb0e20438 |
| 10102026_1236_banking_pay_next_segment_authority_final_v1.sql | f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d |
| 10102026_2232_banking_pay_next_selection_pages_v1.sql | cc1491f55ff1f4afe48f82e8892015aa2f91500f42d0d41d692f0e6b78dd3e48 |
| 04102026_1241_bpay_next_component_selection.sql | 21716604ef859fea23dd1f15d0e5f642528b2f9457e9e96c7acf32615164fe3d |
| 10102026_1100_banking_pay_next_segment_exclusion_v1.sql | 0a2e8a72f43bd6de767e0b277c8c895e02ed29482588a40c23cd51ebd8ac5ef9 |
| final-definer-closures.json | 922914abd78e312ea8b253a17d62bcd35bed967b96ef28490e8a781d5dd6da81 |
| WB-22-selection-memory.md | 870e5ddc2dc0a0062a5f8f3bca467afaeaaf6248d7a0a2da4c9a32e38c3d025e |

Source design findings only. No SQL compilation/runtime, tests/gates, fixtures, data, hosted calls, secrets or product writes were performed. Full app memory/reopen/changed-basis/large-page acceptance remains NOT_RUN.
