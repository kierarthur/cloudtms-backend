# WB-29 W2 lock and scope source review

12 October 2026; sequence 140. **SOURCE PASS — W2-L01 CLOSED; no remaining scoped source finding.** This review covers the authored2320 SAVE/CLEAR transaction and its native lock/scope joins, not installation, business execution, timings, screen approval or whole WB29 completion. No product or database action was performed.

## W2-L01 — missing projection Client prelocks, now closed

The public prerequisite section in2320:136–145 locks the actor, Candidate, current/root/original Timesheets, financial snapshots, submitted Work Contracts, Umbrellas and policies, but no `public.clients` row. Native `mark_processing_v1` in1346:24/33–68 reads `Contract.client_id`, acquires the global Workbench revision row, then writes that Client into group/item rows. Those rows, and the subsequent native summary rows, have actual public Client FKs (migration1252:36/77/185/223). A first or changed projection can therefore request a new Client FK lock after the global row. The related Case projection in1822:1422–1431 additionally derives its Contract/Client from the Case's native Work collection, which need not be a submitted Work Contract.

Required narrow correction: derive the complete finite native projection Contract/Client prerequisite set under the already held Candidate/Work/Case scope, freeze the actual Contract-to-Client identities, and acquire sorted public FK locks before2320's first seed/refresh. Keep native projection formulas, joins and queue behavior unchanged; overflow or missing identity must refuse before mutation. Strong live qualification locks taken after the private scope must retain NOWAIT rather than introduce a public-trigger wait cycle.

The related Case set does not require a broader origin scan: migration1519's `case_id UNIQUE` and current2155:695–710's `case_id=collection.id`, `case_component_id=collection.id` guard prove its sole collection origin. Root's proposed explicit assertion of this exact PK/Case/component/Candidate/Work tuple in the already bounded collection census is sufficient. Once that assertion is present, every related Case Contract is an already qualified explicit Work Contract; prelock those Contracts' actual Client PKs. This narrower proved scope supersedes any assumption that a separate Case-origin discovery is necessary.

**Final closure:** fresh2320:108–112 asserts that every discovered Case has its exact collection PK/Case/component identity, the same Candidate and an already censused explicit Work. Contracts are locked SHARE NOWAIT at149 before150 derives their distinct non-NULL Client IDs;151 locks those Client PKs in UUID order KEY SHARE NOWAIT. Contract SHARE freezes the actual Contract-to-Client tuple, while Client KEY SHARE protects only the required native FK identity. These locks precede the first retention mark at244, seed at247–248, global refresh and all publishers. No wider Case-origin scan or new projection authority was introduced. Fresh final file hash is recorded below.

## Checked source boundaries

Earlier line references below describe the initial reviewed snapshot; the Client/Case closure above gives the final changed anchors. Unchanged semantics were rechecked in the final file.

| Boundary | Result / exact evidence |
| --- | --- |
| Original request replay | PASS.2320:45–52 compares actor/action/Candidate/full request/original epoch and returns the immutable receipt before current epoch, Work, metadata, date or Source qualification. Absent readback performs no mutation.59–65 rechecks under Candidate control. |
| Native admission/fence | PASS. Closed action/request schema, existing service/admin boundary, module SHARE NOWAIT, Candidate UPDATE NOWAIT and the original Candidate financial fence with NULL owner operands. No invented NET ownership or fence reset. |
| Complete submitted scope | PASS. Raw scope validator is used; all submitted Works lock in UUID order before per-Work qualification. Exact Candidate/current/applied/revision number/approval/seal remain required. |
| Full physical clone inputs | PASS. Distinct current/root revision trees invoke2316's physical10001 sentinel and cumulative10000-row/8MiB census before stage. It includes revision, lines, rates, schedules, shifts, breaks, segments and worker basis; retained native expected counts are checked. Position/collection membership has its own aggregate10000-row/8MiB sentinel before locks/Case discovery. No chosen-bucket truncation or partial clone. |
| Position / related Case prerequisites | PASS. Full Positions lock by Work/component; bounded collection Case IDs are deduplicated, capped100, locked by UUID and checked against the same Candidate before seed. Existing ORIGIN task locks and Candidate metadata precede the first refresh. |
| Public / immutable prerequisites | PASS: W2-L01 closed above. Actor SHARE NOWAIT, Candidate, Timesheet, snapshot, Contract, Client, Umbrella and policy prelocks precede seed. |
| Inclusive mapped siblings | PASS. ACTIVE enumeration uses both mapped predecessor anchors without today's target/date compatibility filter; aggregate101 overflow refuses. Exact mapped basis and independent target/Umbrella/VAT/policy/date key are retained. CLEAR checks its exact disclosed ID/version; SAVE replaces only its exact current compatible owner. Other mapped owners carry unchanged. |
| Complete Source plan | PASS for transaction join. Every requested/carried Source component is deduplicated and planned before any seed.2316 preholds the native binding head and calls the original snapshot before seed; the post-seed snapshot only reacquires preheld module/Candidate/binding/Position locks. READY is untouched; only genuinely qualified UNBOUND is seeded. Existing native witness/current/publication/job predicates remain delegated. |
| Response bound before mutation | PASS. Closed requested/carry result planning uses fixed UUID width and bigint maxima;60KiB Work output/64KiB result overflow refuses before retention marking/seed/stage. |
| Stage before publisher1 | PASS. Final243–260 pre-mark all physical retention and finish all Source seeds, then stage every Work before261–263 invokes any publisher. Each later publisher reacquires preheld business rows or its transaction-owned new revision. |
| Audit / final receipt | PASS. Each old CLEARED event is inserted before the old row is cleared; retained/new decision versions and SAVED events are server-authored under the actual child publication. One final immutable request receipt includes every actual publication/sequence/change/carry. Native refresh_many queue semantics are preserved; `processing:true` is not immediate Ready. |
| Existing public command branches | PASS for visible delta. Public2320 retains0312's original snooze/retry bodies and adds only the two closed resolution dispatch choices before original dispatch. No Worker job kind, public financial command kind, producer or parallel financial store was added. |

## Source identity

| File | SHA-256 |
| --- | --- |
| repeatable12102026_2320_banking_pay_next_component_resolution_v1.sql | 1DD72479BA41C1997439F80B8A2546D285F29CD1F791BBDF28DC2FC615385CE2 |
| repeatable12102026_2316_banking_pay_next_resolution_publication_scope_v1.sql | 5E0E66249E76555FE14A65D68AEE704EF7518E3C38883D13D5CC6B656508BC56 |
| repeatable12102026_2135_banking_pay_next_treatment_publication_boundary_v1.sql | 89A98FC7688803AABE93011EDFCF588C7BAA602DC46C903AA6E3000B1E0751E8 |
| repeatable10102026_1346_banking_pay_next_position_projection_boundary_v1.sql | 8934065D152AB69BBE2EC8EE298BD03B66F4C20A82FBBCDCED78567DB37BB69F |
| repeatable12102026_1822_banking_pay_next_classified_work_collection_v1.sql | B9650E9DA89A3DCA3B59BC33F73EFC8ACF132038DAE163DF826D1F18730990FD |

Scope is read-only source inspection plus this review note. Independent helper economics/guard reviews remain their own receipts. All compilation, catalogue, installation, concurrency, business/RPC and end-to-end acceptance are NOT_RUN by this reviewer. WB29 whole-item and later Snooze screen completion remain outside this scoped SOURCE PASS.
