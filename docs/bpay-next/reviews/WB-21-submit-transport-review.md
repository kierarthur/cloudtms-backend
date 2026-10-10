# WB-21 typed submission transport source review

10 October 2026. Independent read-only review of the current typed transport slice; the full sequencer is still being authored. No product edits, tests, imports, runtime, SQL execution, business/data/network or hosted operations.

## Source identity

| Saved source | SHA-256 |
| --- | --- |
| FE `js/bpay-next-workbench.js` | `b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a` |
| FE `js/bpay-next-office.js` | `ea7737dffe26e74d4e064bb1a147be0f8e8459c392d43435423968f53857b646` |
| BE `broker/src/bpay-next-preparation.js` | `c04cdd54164ac1a534f2ddb366485b3b842568fd88fdb8a187476ecd925404b0` |
| Original installed page owner `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Draft choice intake `10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql`, final read | `06ddc74f3a09de2d83f3fda7cc5e3e0a4f272d64c6a751c7d06b87ef47297c78` |

The draft SQL was being edited concurrently. Its descriptor validation, public CREATE projection and component receipt projections were read directly; this review binds the listed saved version only and does not independently approve the whole SQL implementation.

## Findings and contract agreement

No open concrete finding in the reviewed transport slice.

The descriptor has exactly `namespace,intent,revision,workbench_revision,adoption_revision`. Namespace is the five-key agency/epoch/actor/pay-date/scope tuple; scope is exactly Candidate/Client/channel. Intent contains only the four bounded typed native identity arrays and ALL_MATCHING/NONE. Each array is capped at 1,000; no money, personal text, segment guess, current query evidence or parked accepted-input field enters the descriptor. Office and Worker reject extra fields, wrong booleans/modes, duplicate native IDs (case-insensitively), invalid UUID/date/decimal-string counters and descriptors over 32KiB (`Office:155–168`; `preparation:138–150,228–237`). The existing Worker request-body limit independently bounds the complete envelope. Its recursive UUID normalization happens before dispatch; SQL receives lowercase IDs. Actual authority stays in the authenticated injected actor and SQL's real context comparison, rather than trusting namespace claims.

Descriptor CREATE is exactly `{run_id,command_id,descriptor}`; old optional pay-date CREATE remains supported. The new reply is the six original CREATE keys plus `command_id,agency_sequence,choice_work_count,choice_case_count,empty,replay` (12 keys total). Both mirrors correlate run/command/date, enforce `selection_count <= choice_work_count`, and require `empty` to agree with zero selected Candidates and null agency sequence (`Office:246–258`; `preparation:235–276`). This matches SQL's empty cut: no native command receipt is allocated for zero selected Candidates, while retained choice counts can include skipped entries. A nonempty receipt requires a positive decimal-string sequence. The response does not invent a positive sequence for an empty selection or infer that zero attempted choices were retained.

COMPONENT_PAGE/SEAL keep the actual original native signature fields. Both layers correlate run/work/expected revision, page number and exact uploaded item count; SEAL correlates the returned component count with the requested expected count (`Office:30–45,256–267`; `preparation:13–15,273–287`). All IDs arrays are nonempty, unique and capped at 100. SQL's actual append and seal replies expose precisely those fields; no client-side total or economic calculation was added.

WORK_CHOICES matches the original page owner's six fields; SELECTED_COMPONENTS matches its four fields (`2018:3888–3899`). Worker and Office retain exact schemas, bounded rows, strictly increasing native Work UUID/selection-number cursors, and the ALL choice invariant SEALED/zero page count/zero component count (`Office:57–58,77–78,429–439`; `preparation:28–29,394–412`). SELECTED_COMPONENTS is scoped by the actual run/work request; its expected revision is a retained row field, not a newly invented request key. The subsequent sequencer must perform its actual per-work readback using these native fields; that sequence is not asserted complete here.

The expanded finite skip union agrees between Office and Worker and accommodates actual selection hold/snooze/current approval/current revision/signed eligibility/PAYE/Source outcomes. WORK_PAGE replies additionally prove every skipped Work belongs to the submitted native page and follows its input order (`Office:167–169,259–267`; `preparation:150–152,277–279`). New transport code does not replace unknown outcomes with an assumed success or dispatch a second request.

The existing historical ACCEPTED_CANDIDATES/ACCEPTED_WORK decoders are not used by the new choice transport. The financial controller explicitly refuses both kinds before transport (`Office:541`); actual progress reads remain WORKERS/CHANNEL_WORKERS (`:630`). No parked F25 accepted-input/version/certificate/resolver path was reactivated. The local store/Close review is recorded separately.

**Verdict: SOURCE PASS for the current closed descriptor, CREATE/empty receipt, component page/seal and native readback mirrors.** Whole WB21, the complete submit sequencer, later child/control joins, SQL installation and business/runtime behavior remain unclaimed.
