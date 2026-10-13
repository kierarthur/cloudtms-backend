# WB-21 accepted CREATE cut: actual clock and FK lock order

Source-only investigation, 10 October 2026. No SQL, runtime or test execution. This supplements the lifecycle seams and supersedes their simplified description of command-clock acquisition as only a final-tail operation. Keep every installed financial/Snooze owner and the 1236 run-boundary guard unchanged.

## Concrete current edges

| Source | Exact existing edge |
| --- | --- |
| `1236 ...segment_authority_final_v1.sql:173–179` | PayRun INSERT invokes `segment_run_boundary_guard_v1`; line178 reads `command_clock` **FOR SHARE** before the 3-argument CREATE returns. It retains the real Snooze control sequence and London business date. |
| `1346 ...position_projection_boundary_v1.sql:162–178,217–221` | Publication locks Candidate control, Work **FOR UPDATE**, immutable Work revision **FOR UPDATE**, then updates the maintained workbench projection, then `receive_command_v1` locks the command clock **FOR UPDATE**. |
| `1100 ...snooze_v1.sql:209–215,234–236` | Native Snooze locks Candidate control and its Work/CaseComponent before command receipt. It holds the clock **FOR UPDATE** before updating the native Snooze row. |
| `1429 ...current_continuation_v1.sql:7–29,268–278,142–146` | The existing Snooze AFTER callback queues the real Work origin or refreshes the real Case. Queue/schedule updates the shared workbench revision after native Snooze command receipt. |
| `1252 ...workbench_summary_v1.sql:477–486` | The real Case refresh locks Candidate control and Case and updates the same workbench revision. |
| `1404 ...command_intake_v1.sql:32–39` | `receive_command_v1` locks module SHARE, then the single command clock UPDATE. No second clock is needed. |
| `0575 ...preparation_service_v1.sql:12–19` | The original preparation authority helper already locks the actor row SHARE and module SHARE. A new outer Source admission must be positioned before this existing module lock, after equivalent pure service/actor checks. Calling Source admission only inside the new CREATE overload is later than the public wrapper's original authority call. |

The projection-revision row cannot be used as CREATE's locking cut. Clock SHARE → projection SHARE conflicts with publication projection UPDATE → clock UPDATE. Moving projection SHARE earlier fixes that pair but conflicts with **unrelated native Snooze** clock UPDATE → projection UPDATE. No broad financial-owner rewrite is justified by WB21.

A second independent issue is the inherited SHARE upgrade: two new CREATEs can each own command-clock SHARE and then request UPDATE at PREPARE receipt. Neither can upgrade while the other SHARE remains. Taking UPDATE before the inherited INSERT makes its SHARE reentrant and serializes the accepted cut normally.

New 1832 choice/exclusion rows also have real FK checks: Candidate, Work, WorkRevision and CaseComponent. An implicit FK KEY SHARE acquired after the clock can wait for a publisher's Work/Revision UPDATE or a native Case Snooze's CaseComponent UPDATE while that owner waits for the same clock. Explicit Work/Revision preholds alone do not cover Candidate/CaseComponent or deliberate overrides absent from the current READY projection.

## Smallest finite implementation recommendation

1. Preserve pure exact replay and collision behavior. Parse the closed descriptor and perform equivalent pure service/active Office authority first. Acquire the existing Source admission before the original module SHARE helper. Keep the original authority/revalidation statements after admission.
2. Derive the exact scoped prospective native reference tuples, plus deliberate overrides/exclusions, from maintained READY membership using actual identifiers. Prehold the **actual parents needed by the new FKs** in deterministic order before any command-clock lock: public Candidates; Work; WorkRevision; CaseComponent. Include absent-from-READY deliberate Work/exclusion and expected-revision references. Do not lock all agency parents and do not lock the projection revision.
3. Retain those typed reference sets locally. If the membership snapshot later needs a key outside the preheld set, refuse `BPAY_NEXT_CHOICE_CONTEXT_CHANGED` before an INSERT or any new parent lock. Restrict accepted membership to the exact validated set; never silently discard a newly discovered key.
4. Acquire the existing `command_clock(id=1)` **FOR UPDATE**, then call the unchanged original 3-argument CREATE. The 1236 SHARE is now reentrant; the original Snooze cut still comes from its actual guard. PREPARE receipt later reuses this same existing lock and clock. For a genuinely empty accepted result, retain the planned UUID/descriptor but create no PREPARE command or run-command binding.
5. Materialize the complete chosen WORK/CASE membership and the four Rule-B Candidate input fields in **one SQL statement snapshot**, with explicit typed columns, native eligibility and Candidate-wide exclusions. Use returned WORK and PAYMENT Case Candidate sets to include all actual default deductions consistently. Existing Source admission holds the real query-gate source scope; retain each genuine original query result in the existing query-snapshot relation. Do not re-read today's Candidate inputs during SEAL.
6. Read/compare actual maintained HEADER context around capture **without locking the workbench revision**. A changed context refuses/rolls back the new cut; an unchanged context must not be treated as a permanent metadata lock. The accepted immutable rows define the actual captured statement cut. Subsequent maintained changes follow the already agreed Rule-B post-click revision REVIEW behavior. No producer versioning, latch, journal or parked F25 protocol is introduced.
7. After the early clock lock, acquire no new existing Work, WorkRevision, CaseComponent, Candidate-control, metadata, task or projection-revision locks. Only reentrant preheld FK parents and new run/choice/receipt children may be written. The existing clock is held for the short bounded CREATE transaction only; pages and browser review remain separate transactions.

The exact offered membership must be server-complete; preholds must not use first100 truncation or drop missing references. Existing statement/byte limits provide the bounded refusal. This is a recommendation for the root-authored draft, not a claim that its newest bytes already implement these steps or a runtime deadlock proof.

## Exact source snapshot

| File (repository relative) | Raw SHA-256 |
| --- | --- |
| `supabase/repeatable/10102026_1236_banking_pay_next_segment_authority_final_v1.sql` | `f74ea457bf77f0d85ce7f3f21089bdecf9d9d9121d4005850de430d50aa7379d` |
| `supabase/repeatable/10102026_1346_banking_pay_next_position_projection_boundary_v1.sql` | `8934065d152ab69bbe2ec8ee298bd03b66f4c20a82fbbcdced78567db37bb69f` |
| `supabase/repeatable/10102026_1100_banking_pay_next_snooze_v1.sql` | `9b6ceda100c313e6ee879f73102740f38d20cd7e9e6c49bf7bd9affc7117fdf2` |
| `supabase/repeatable/10102026_1429_banking_pay_next_workbench_current_continuation_v1.sql` | `dc539089fae0cd1a8baf4765a89cea9ff1d1e8039200ecda5d80371dda2335dd` |
| `supabase/repeatable/10102026_1252_banking_pay_next_workbench_summary_v1.sql` | `2fe6c4c816613aaa554d012289124c47219fd228a2af229c2ceabdb8f0cc305b` |
| `supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql` | `be3c7dd17ecea6cbff631ed0b39c9b09947e6284b93db04ecdf517e320fed3e1` |
| `supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql` | `4c651558f56b85c29cf10f21a1d3168ce259f75a66001a24a7f53dbc76216a87` |

## Same-run replay / pre-existing run boundary

Early existing-clock UPDATE is safe only if it does not introduce a wait for an existing PayRun after clock acquisition. Original SEAL/expiry hold the run before command receipt. Preserve accepted exact replay ahead of the fresh capture path. Lock and revalidate a clean pre-existing OPEN run before native preholds/clock. For the absent-run race, once clock UPDATE is owned, acquire any now-existing same Run **FOR UPDATE NOWAIT** before invoking the unchanged original 3-argument CREATE; a lock conflict is a bounded pre-accept busy refusal, not financial success. The original insertion's clock-SHARE guard prevents another legitimate new Run INSERT from passing while this clock UPDATE is held. This closes the clock→run versus existing run→clock inversion without rewriting any expiry or financial body. It is source design advice, not confirmation of a built NOWAIT branch.
