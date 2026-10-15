# Required outcomes for the final specification

15 October 2026. This file is a review checklist and shared design constraint. The final code maps and UI catalogue must identify how each applicable outcome is achieved and tested. These are requirements, not assertions that new implementation has passed.

## Source invoicing

- **SI-01:** A Contract move changes no existing Source invoice identity, invoice-line membership, Source billing movement identity, accepted report membership, net/VAT/gross amount, existing credit relationship or issued document. Invoice/rendered line counts remain once, not duplicated on A and B.
- **SI-02:** Before invoicing, accepted Source invoice amounts and allocation scope remain the same after the move. Archiving A cannot remove legitimate Source invoice eligibility; B cannot become an additional ordinary invoice candidate.
- **SI-03:** Do not copy invoice associations to B in a way that causes ordinary renderers/collectors to count A and B. Current B must display its existing Source invoice through a defined association while original invoice evidence remains intact.
- **SI-04:** Source mileage/expense invoice amounts do not become B's Contract charge quotes. Ordinary Contract-priced invoicing is a distinct explicitly derived branch. A preset whose initial approved Source amount came from its original Contract calculation does not permit repricing that accepted Source amount during a move or later invoice creation.
- **SI-05:** Source invoice-move, unissue, credit or replacement commands are not the Timesheet Contract-move implementation. A genuine later revised Source report follows its existing separate process; a move does not invent that event.

## Timesheet, evidence and worker money

- **TM-01:** Same worker/actual client; B covers actual work/claim dates. Hours, original documents, signatures and old financial history remain factual and unchanged. Old A remains readable in History and links to current B.
- **TM-02:** Universal scope includes ordinary, manager-signed, worker-only, NHSP/HR authoritative, existing legacy, genuine Daily and existing expense-only records. Pending stays pending. Existing signature requirements for a genuine hours correction are not waived by a Contract move.
- **TM-03:** A→B→C and A→B→A retain one direct original work/component identity, with exactly one current placement. Opening archived A resolves the existing move/current record, not another new claim.
- **TM-04:** Candidate/MyTMS list/detail/history/booking/submission/reply lifecycle and manager PHONE evidence must remain valid. Do not rewrite immutable workflow IDs or fabricate a newly signed B. Exact affected protected-boundary objects and their scoped changes must be identified.
- **TM-05:** Combined and expense-only claims move as the same claims with original evidence, approval, payment and invoice history. Carrier creation/reuse is automatic when the existing route requires it. No resubmission, second claim, invented hours or loss of original expense authorisation.
- **PM-01:** Old and new work do not produce an independent full A debt plus a full B payment. Actual prior payments, realised recoveries and current reservations are attributed once to the original work/components.
- **PM-02:** Comparable unpaid earnings100→120 leave total120 due; paid100→120 leaves20 extra; paid100→80 leaves20 overpayment. These figures are earnings examples, not fabricated PAYE cash. Actual pay channel, deductions/VAT and once-only conversion use the existing financial calculation.
- **PM-03:** Paid60/reserved40→80 is not yet a cash debt. If the40 settles, the20 excess exists; if safely released,20 remains due. A saved Draft retains its original amounts, creditor, rates and treatment. Unknown outcomes never become zero or a fresh full offer.
- **PM-04:** Physical movement can have an explicit recoverable financial-pending state where existing commitments/outcomes are unresolved. Quarantine only new unqualified differences. Supply the exact resume/result action and prevent duplicate financial acceptance.
- **PM-05:** Additional codes retain their real quantities and B's applicable pay rules. Mileage keeps miles and separately resolves genuine pay and charge authority; explicit zero differs from missing. Source invoice rules override B charge quotes. No double expense payment or repeated tax/conversion.
- **PM-06:** Preview uses server facts and current permissions. Confirm/retry has one durable operation identity, locks/version checks and exact request replay. Preview and confirmation send no bank payment or external invoice email.

## Office decision to keep Source invoice and not pay

- **NP-01:** Office can choose identified one/several Source shifts, keep their Source invoicing, not pay the worker and not ask the client. Unpaid permitted decisions need no client correction before they can finish.
- **NP-02:** This is not a new invoice hold or a falsified Source/worker/signature fact. A general Query/on-hold flag must not accidentally block Source invoice creation. Pay exclusion belongs to the exact work/components and survives reimports and Contract moves.
- **NP-03:** Paid, protected, frozen, reserved, independently signed and unknown-outcome facts need their exact permitted decision/correction/recovery branch. Do not hide these cases in an unsupported box, erase previous money or call recorded debt recovered.
- **NP-04:** Selected resolved shifts leave active checking/contact counts while history remains. New evidence can prompt a deliberate Office review; an unchanged report cannot restore pay or restart client contact automatically.

## Reports and contacts

- **RC-01:** Relevant actual client/period report coverage and completed current checking review precede report-based worker/manager contacts. New uncovered periods stay in Imports/Checklist. Report-only old unconfirmed or explicitly not-worked pending shifts remain quiet History unless an independent real issue exists.
- **RC-02:** First NHSP complete pair or scoped zero is the selected proposal. Later Released-only updates reuse genuinely covered coherent stored Awaiting evidence. Distinguish positive last-known pending from unsupported negative absence; never infer file completeness/as-of from populated row dates or upload time.
- **RC-03:** Matching declared pending work goes to manager authorisation/finalisation without a separate worker question. Planned-only HealthRoster plus declared work needs manager entry of actuals and finalisation. Real differing facts retain their actual verification. Pending matches never approve worker pay.
- **RC-04:** Optional pending reconfirmation appears only inside a necessary same worker/client/week question. No extra alert, deadline or delay of independently justified manager action. Ordinary manager-signed/Daily report errors go to Temp Staffing through the actual deliberate selection; no automatic request to re-sign because a report differs.
- **RC-05:** Default first manager delay0, no automatic repeat, one deliberate follow-up due after2clientworkingdays, latest completed client week,14calendar-day stop age and existing hard cutoffs. Settings can define zero/multiple ordered intervals and deliberate versus automatic repeats; reimports do not renew allowances. Keep actual existing worker6h/12h clocks unless the code audit establishes their controlling scope differently.
- **RC-06:** Thursday payroll priority uses the actual configured run, not an invented hour. It does not shorten answer clocks or revive expired client campaigns. Genuine late claims retain an actionable later-run/Office path.
- **RC-07:** Manager emails group worker/date/time and state the exact report issue and requested action. Short optional update link. Real per-shift manager answers and partial replies are visible; reports can resolve work without a manager form response.
- **RC-08:** Office sees Sent after accepted sending, Scheduled when not yet due, and plain Failed/Checking result for genuine problems. No queue-versus-delivery lecture. Lost results use the original request; do not blindly send again.

## Contact pause and push timing

- **PA-01:** Owner accepted worker+selected-client scope. Pause worker, manager or both until nextMonday00:00 Europe/London; display exact expiry and allow early Resume. Pausing a manager for Taylor never pauses Alex merely because both share that recipient.
- **PA-02:** All checking-contact creation/render/grouping/claim/start/final-provider paths enforce the same pause epoch. Mixed manager emails exclude paused worker rows from counts/body/links/attachments while other eligible workers can proceed. No stale immutable payload may leak a paused row.
- **PA-03:** Define the in-flight submission barrier honestly; an external submission already started cannot be recalled. Exact in-flight result/permit handling must prevent a false pause guarantee or duplicate retry.
- **PA-04:** Resume/expiry rechecks report scope, current need, recipient, age, client cutoff, send history and quiet push timing. No catch-up burst, allowance reset, deadline extension or silence-as-not-worked. Existing answers, question access and history remain available. Pause affects checking communications, not security, general Rota or bank/provider messages.
- **PA-05:** Unpublished worker questions do not acquire a reply deadline merely while paused. Already activated questions keep their existing deadline. Pausing one audience does not block an independently justified contact to the other.
- **QT-01:** Quiet time affects pushes only; weekday/weekend/BH settings use the existing calendar. Explicit checking-contact pause also covers relevant emails. Recheck obsolete/answered events at actual send time. No new bank-holiday source or quiet-time email delay.

## UI and Payment Summary

- **UI-01:** Exact five-tab design explicitly amends the existing conditional Finalise visibility; no Source-pay command is added for signed check-only work. Stable worker/client/date/shift context across tabs, filters and child modals. Role restrictions remain.
- **UI-02:** Every control has defined visibility/enabled conditions, actual target, exact copy, recipient, mutation effect, success/failure/permission/retry result. No generic demonstration message or implementation prose in normal dialogs. Open Timesheet identifies the correct worker/date record.
- **UI-03:** Needs attention, Waiting, replies and paused states are precise per shift. No irrelevant old/unconfirmed/denied pending rows in active counts. Partial manager replies do not complete unanswered shifts.
- **UI-04:** Each finalisation route explains exactly which pay hours are prepared/approved, which invoice can be drafted and the actual named next action. Finalising does not claim bank payment. Bulk operates reviewed eligible loaded selections, retains each outcome and skips completed reports after navigation/retry.
- **PS-01:** Original payment cash and captured work/client/rates remain unchanged. A move creates no cash row. New genuine payments/recoveries appear once. Unrecovered debt stays in its Finance/Timesheet record, not a fictitious cash movement or £0 Payment Summary row.
- **PS-02:** Payment details provide the A-to-B explanation and old/current Timesheet links. Expense/mileage components retain their own history. Source invoice history is reachable through Timesheet/invoice links; client invoice totals are never mixed with worker cash. PAYE take-home is not invented per Timesheet.
- **PS-03:** The inspected plan's AUTH-PS design gate is not an existing implementation. The final work order must state the precise approved-by-passing design scope and record the additive approval without changing old CM-01 approval bytes. No undefined future design task is handed to a builder.

## Handoff acceptance

- **HD-01:** Two work orders map exact existing plan/card/test labels and actual current code, classify unchanged/reuse/modify/new, and name fields, functions, transaction boundaries, caller changes and failure recovery. No builder is asked to invent missing architecture.
- **HD-02:** Passing the reviewed work order directs immediate implementation/amendment of affected existing code and continuation of the existing build. Preserve unrelated dirty work and old approval evidence; installations/feature activation retain their actual separate controls.
- **HD-03:** Current source observations, local illustrative design tests and future native/application/financial tests are labelled separately. Previous V4 reviews do not qualify changed V5 policy or code. Sol6.1extra-high then GPT6Astrahigh must review the same final package, with corrections rechecked.
