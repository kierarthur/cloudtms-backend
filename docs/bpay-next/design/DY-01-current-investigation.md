# DY-01 current investigation

Read-only investigation, 11 October 2026. Approved sequence **91**, immediately after DY-04. No product/checklist changes, SQL execution, business rows, tests, fixtures or hosted calls. This note is not a build or acceptance receipt.

## Required scope and dependency

Read complete card `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:871–899`, all checklist rows `IMPLEMENTATION_CHECKLIST.md:2538–2560`, exact R6 DY-01 packet, and current build chain `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:67–70,174–176` / `IMPLEMENTATION_SEQUENCE.md:98`. DY-01 depends only on DY-04. Its named DB target is **`private.bpay_next_approval_route_v1(uuid)` unresolved Daily admission successor** (D01). D03 is the future joined unresolved receipt → Office resolve/process/authorise → one stored ordinary Work check.

A genuine receipt may exist with no financial row, Client or rates. Reading/saving a local draft creates no Daily; actual receipts belong to separate booked dates, no Contract Week. NEXT consumes the processed Daily result, without inventing a Client, financial row, Contract, rate or Source HEAD. Role stays from the real Rota job title / physical `job_title_norm`. SCX-059 Weekly Source, SCX-060 legacy HealthRoster import and SCX-061 Candidate Daily receipt objects are explicit no-change scope (`card:875–887`). Daily remains HOURS-only under the 8 October owner statement (`card:869`, `09_OWNER_ANSWERS_BINDING.md`).

Owner A2's 10 October task direction governs appearance: show the card's own reader fields; a supplied design does not authorise additional server features. Rule B stays in the existing accepted-input objects: before-click queued factual approval joins the cut; after-click facts belong to a later Draft. This admission check neither changes accepted selections nor reprices frozen Work or live locked Case balances.

## Current chain after the authored DY-04 successor

The current final-definer registry already points to the following saved **uninstalled DY-04** files. Registry selection is source ownership, not installed/runtime proof.

| Object | Current source | Relevant correct behavior |
| --- | --- | --- |
| Approval route | `supabase/repeatable/11102026_0939_banking_pay_next_contractless_daily_publication_v1.sql:5–102` | Current/nonarchived/nonrevoked physical check, LEFT JOIN Contract, one positive NULL-Contract DAILY/HOURS/nonadjustment branch; existing contracted/Source classifications preserved. |
| Ordinary publication | same `:104–832` | Exact TF/physical row and Daily evidence `:187–212` before Work creation; factual identities/date; current available-rate array `:318–358`; retained source Umbrella/rate IDs and frozen Daily JSON `:382–403`; native day/shift/break publication. |
| Authorisation trigger | same `:836–919` | Original approval transition/NEXT boundary `:851–864`; only ORDINARY_HOURS, DAILY_HOURS, EXPENSE_CARRIER cause publication `:867–870`; actual withdrawal/projection refresh retained. |
| Daily approval evidence / available rates | `supabase/repeatable/11102026_0925_banking_pay_next_daily_five_standard_rate_rows_v1.sql:5–133` | Exact authorised, current/nonstale SELF_REPORTED physical TF, Client/Candidate/context/date/clock/HOURS-only qualification; dated authority match and all five Standard rows/fifteen cells with genuine NULLs. |
| Work identity schema / pointer | `supabase/migrations/11102026_0928_banking_pay_next_daily_work_origin_identity.sql` and `supabase/repeatable/11102026_0928_banking_pay_next_daily_work_identity_guard_v1.sql` | Explicit DAILY origin, immutable original date/booking/physical anchor and nullable Contract rule; no new Work kind. |
| Conditional revision / insert guards | current `0939...contractless_daily_publication_v1.sql:921–1040` | Positive exact Daily frozen evidence and original physical identity; existing immutable/seal/totals checks retained. |
| Missing-rate Authorise support | `supabase/repeatable/11102026_0939_banking_pay_next_daily_authorise_rate_refusal_v1.sql` | Existing Authorise owner names the actual missing used Daily bucket and preserves original status refusal; no auto-authorisation. |

The plan's shared route chain is WB-03 → DY-04 (`08:70`). DY-01 is the explicitly dependent small admission supplement to that final route; it must copy/re-read **0939**, not restore 1450 or the historical pack. The publisher chain continues DY-04 → DY-06 → DY-10 → DR-38 (`08:67`); evidence continues DY-02 → DY-08 (`08:69`). DY-01 does not take ownership of those later changes.

## Proved remaining D01 gap and native operands

Current route `0939:45–51` returns **DAILY_HOURS** immediately once the physical NULL-Contract DAILY/HOURS shape is valid. It does not inspect a current processed financial row or frozen resolved Client. Thus a factual unresolved receipt receives the payable route classification. The current publisher/helper nevertheless refuse missing/unauthorised/unqualified TF before any NEXT Work insertion (`0939:187–212`, `0925:23–76`); this existing protection must remain. The missing work is the card's explicit **closed nonpayable route state**, not a new entitlement producer.

Use the existing factual snapshot, not inferred Client data:

- Canonical context `broker/src/daily-canonical-authority.js:42–49,54–68` supplies actual Candidate, `out_client_id`, role from `job_title_norm`, band, and `out_policy`. Its native output `:281–291` stores actual `TF.client_id`, Candidate/role/band, policy and rate-source references. `:203–206` distinguishes UNASSIGNED and CLIENT_UNRESOLVED.
- The existing settings owner `supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql:831–837` preserves a nonempty captured snapshot. For a Daily it obtains Client only from a **current TF with non-NULL `processed_at_utc`** at `:846–870`.
- Its current after-financial trigger `:893–921` returns if unprocessed, then captures the real processed `new.client_id` with the actual nullable Contract and Daily date. The existing binding is `:1005–1008`; no trigger change is required.
- Current core `supabase/repeatable/06102026_1214_self_bill_candidate_expense_separation_v1.sql:377–396` stores exact `authority_version='CONTRACT_SETTINGS_AUTHORITY_V1'`, `client_id`, `contract_id`, `relevant_date`, `workflow` and values in **`Timesheets.settings_authority_json`**. Do not invent `TF.policy_snapshot_json.client_id` or resolve a Client from display text/current Contract.

Smallest implementation is one complete route-only successor, preserving its signature, STABLE/pure-read posture, fixed search path, owner-only ACL and all existing branches. After the existing genuine Contractless Daily physical gate, perform only the exact native Timesheet/current-TF point qualification: current same physical version, nonstale SELF_REPORTED processed TF, non-NULL factual Client, and existing captured Daily settings authority with matching Client and NULL Contract identity. An absent/unprocessed/resolution-pending native row/snapshot returns one finite private nonpayable text state (for example **DAILY_RESOLUTION_PENDING**); no NEXT row is written. Structural invalidity, foreign/stale/mismatched identity must not be converted into payable classification or fabricated authority. Once real Office processing supplies the matching snapshot, return the original DAILY_HOURS classification; actual authorisation is still enforced by the existing publisher.

No rates are calculated or resolved by this branch. Do not require opposite-channel availability, replace missing rates by zero, or bypass DY-04's named used-rate refusal. Do not require authorisation merely to classify a valid processed Daily; publication continues to require it. No schema, command, public route, new trigger, snapshot rewrite, new reader or Source binding is needed.

Current native authorisation trigger `0939:868` already has an explicit three-route positive allowlist, so a closed nonpayable state does not call the publisher. Direct private publication must continue to fail before writes for an unqualified state; preserve its existing strict guards. If the new branch's placement requires an earlier allowed-route refusal before the existing contracted STRICT lookup at `:199`, name that exact minimal caller support rather than introducing another owner/framework.

## D02 screen conflict and retained existing blocker

Checklist **DY-01-D02** `:2549` / R6-D0159 names `js/bpay-next-office.js :: Daily awaiting resolution nonpayable blocker`. The full card explicitly says **Worker:none; Screen:none; existing Daily Office screens unchanged** (`:885–887`). Record this discrepancy; the checklist cannot silently create an unresolved-receipt reader or payable Work representation that contradicts those rules.

Current NEXT Office/Workbench closed schemas have no DAILY/CLIENT_UNRESOLVED receipt fields; they consume native published Work/Case facts. An unresolved receipt creates no Work, so a new NEXT blocker row would need new server features beyond the card and owner A2. The existing native Daily Office screen already displays factual CLIENT_UNRESOLVED as **Client Required** (`FE js/main.js:116737,116776`), marks that state and enables its actual Client assignment action (`:342859–342868`). Preserve that upstream resolution interaction and its nonpayable NEXT exclusion; do not fabricate a Workbench item, summary contribution or reader response.

**D02 resolution:** retain and source-review the existing Daily Office resolution blocker and the named NEXT Office's exclusion of unresolved/unpublished work, using D01's closed nonpayable route. This is the screen row's required behavior under the full card, not a skipped/NA row. Its completion receipt should cite the actual retained native blocker and unchanged NEXT schemas, state why the card's explicit no-screen-change rule requires no UI product edit, and leave its joined real-app check NOT_RUN. Existing “Client Required” is factual resolution-pending copy; inventing an additional NEXT list or reader would contradict the card. No server reader addition is authorised.

## Honest completion boundary

Only D01's small route supplement is proved as remaining product work here. Preserve SCX-059/060/061, canonical receipt/calculator/settings owners and existing Daily screens; complete D02 as the retained screen behavior/source review above and document its full-card/checklist reconciliation explicitly. All future checks—real unresolved receipt before resolution, actual processed Client/NULL Contract/role snapshot, later authorisation creating exactly one stored Work, distinct booked dates, no read/save creation, unchanged Source/import/Candidate receipt routes and exact Create cut—remain **AUTHORED_NOT_RUN**. No build, installation or app-tested box was changed.

