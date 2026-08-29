# NHSP/HealthRoster Weekly Authoritative Amendments

## Independent-verification handover

Date: 31/07/2026
Environment: TEST only
Plan: **NHSP/HealthRoster Weekly Authoritative Amendments Plan — Final corrected nine-function repair plan**
Implementation commit: `1973054` on backend branch `test`
Database installation: complete on TEST Supabase at `2026-07-31 20:05:42 UTC`

The exact locked implementation plan is included beside this report as:

`locked-final-nine-function-plan.md`

## Mandatory instructions to the independent verifier

Independently inspect the committed code, the installed TEST definitions, this handover, and the exact locked plan.

Return:

1. an overall verdict of `FULLY IMPLEMENTED` or `FURTHER WORK REQUIRED`;
2. evidence for every finding;
3. a function-by-function report covering all nine target functions;
4. if further work is required, a highly detailed function-by-function implementation plan containing **only** the functions that demonstrably still require amendment to fulfil this locked brief;
5. the exact focused tests needed for every remaining defect.

This handover is not permission to broaden the work. Do not propose changes to Banking Pay, any `pay_*` function, payments, batches, CSVs, provider execution, settlement, remittances, invoice creation/issue/unissue/credit logic, VAT, invoice totals, general TSFIN calculators or queues, Worker code, frontend code, Bulk Authorise, HealthRoster Daily, HealthRoster Weekly validation-only processing, or non-authoritative imports.

If the nine functions already fulfil the brief, say `FULLY IMPLEMENTED`. Do not manufacture adjacent work.

## Business result required

For each exact NHSP Weekly or import-authoritative HealthRoster Weekly source shift:

```text
B = complete signed economically effective frozen invoiced Weekly-hours position
M = current active, non-archived, wholly uninvoiced mutable correction position
A = latest authoritative imported schedule and hours
```

The governing equation after the existing TSFIN process settles is:

```text
B + corrected outstanding position = A
```

For a standard new correction generation:

```text
reversal schedule        = normal schedule representing B
reversal TSFIN           = -B
corrected-hours schedule = A
corrected-hours TSFIN    = existing TSFIN result for A
```

The import decides which timesheet rows and schedules are needed. Existing TSFIN remains the sole authority for pay, charge, margin, rates, components and rounding. Banking Pay remains entirely outside this implementation and simply consumes whatever payable truth is outstanding later.

## Required route matrix

| Current economic/source state | Required route |
| --- | --- |
| Ordinary unpaid, uninvoiced source | `AMEND_SOURCE` |
| Ordinary paid, uninvoiced source | `AMEND_PAID_UNINVOICED_SOURCE` |
| Wholly uninvoiced mutable generation, including a deleted/changed member | `AMEND_EXISTING_REPLACEMENT` |
| Complete effective invoiced position requiring another change | `CREATE_REVERSAL_REPLACEMENT` |
| Exactly one economically proved role invoiced | safe partial-invoice blocker |
| Role evidence cannot be proved | safe scope-unprovable blocker |

A current mutable generation can have zero, one or two surviving live members. Missing rows are recreated and changed rows are normalised. Archived rows are audit-only and never contribute to `B` or `M`, never become mutation targets, and never enter authorisation arrays.

If an archived row occupies the unique old `correction_id + correction_kind` key, the live generation is repaired under one deterministic fresh correction ID. The archived row is untouched.

Repeated fully invoiced generations remain supported because `B` is the complete signed frozen invoice ledger, not an assumption about one original timesheet or one latest pair.

## Exact implementation surface

Only these nine existing SQL functions were amended:

1. `public._import_review_effective_invoice_balance_core_v1`
2. `public._import_review_action_catalog_core_v1`
3. `public._import_review_apply_envelope_core_v1`
4. `public.import_review_correction_generation_transition_v1`
5. `public.timesheet_paid_uninvoiced_rollover_v1`
6. `public.hr_weekly_apply_transactional`
7. `public.nhsp_weekly_apply_transactional`
8. `public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`
9. `public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

No schema object, table, column, trigger, index, migration, Worker file, frontend file, invoice function, Banking Pay function, Daily function, or validation-only function was added or changed in this completion delta.

The existing four-argument HealthRoster phase-3 overload remains present and unchanged.

## Function-by-function implementation and verification contract

### 1. `_import_review_effective_invoice_balance_core_v1`

Canonical file:

`supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Implemented responsibilities:

- derives `B` only from economically effective `ISSUED`, `PAID`, or `ON_HOLD` frozen Weekly-hours invoice evidence;
- ignores separable expenses, mileage, travel, accommodation and other non-hours lines;
- resolves credit hours from the original frozen TSFIN segment, negating hours exactly once while treating credit financial values as already signed;
- excludes archived timesheets from `B`, `M`, route selection, live member IDs and all mutation inputs;
- retains archived IDs/roles only as audit evidence;
- reconstructs reversal/replacement role evidence from current rows, request/apply operation evidence, correction operation contracts, audit evidence, and invoice/frozen-TSFIN evidence;
- classifies generations from economic role evidence as fully invoiced, genuinely partially invoiced, mutable, or unprovable;
- does not infer partial invoicing merely because a live member is missing;
- exposes active mutable members, physically missing roles, archived audit roles, the reviewed correction ID, repair identity mode, standard `B` schedule, effective-net predicates, and deterministic fingerprints;
- leaves the intentionally accepted safety boundary for a genuinely one-sided economic invoice.

Verifier checks:

- archived rows contribute zero to `B` and `M`;
- the retired `IMPORT_REVIEW_ARCHIVED_GENERATION_ACTIVE_MEMBER_CONFLICT` route is absent;
- both roles economically invoiced remains fully invoiced even when a live row was deleted;
- neither role invoiced with one or both members missing remains mutable when durable evidence proves the generation;
- exactly one economically invoiced role remains a genuine partial blocker;
- non-hours lines do not affect hours, pay, charge, margin or route selection;
- credit signs are correct and not double-negated;
- source, line, role and invoice evidence are bounded and fingerprinted.

### 2. `_import_review_action_catalog_core_v1`

Canonical file:

`supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Implemented responsibilities:

- evaluates every import source deterministically in internal batches of at most 100 evidence sources;
- the 100 value is an internal helper evidence-window bound and does **not** limit an import to 100 shifts;
- selects the four authoritative routes from `B/M/A` and the effective-net predicates;
- carries repair identity, missing-role, archived-audit, policy-basis and reconciliation evidence into the action summary;
- keeps each action in the normal Import Review Ready/blocker approval model;
- continues to exclude HealthRoster Daily and validation-only Weekly from financial correction generation.

Verifier checks:

- every source is processed, including imports exceeding 100 rows;
- route selection uses economic evidence and not surviving-pair assumptions;
- ordinary paid-but-uninvoiced sources select the paid rollover route and do not enter phase 3 solely because they were paid;
- blockers remain source-specific and friendly through the existing review contract.

### 3. `_import_review_apply_envelope_core_v1`

Canonical file:

`supabase/repeatable/21072026_1820_00_import_review_internal_core.sql`

Implemented responsibilities:

- freezes the approved request authority for exact `B/M/A`, schedules, invoice evidence, active member IDs, missing roles, archived audit evidence, reviewed existing correction ID, repair identity mode, source identity, policy-basis evidence and fingerprints;
- uses the updated `unit-v2` fingerprint contract;
- does not invent future generated member IDs.

Verifier checks:

- the envelope is a faithful immutable request record;
- generated/applied IDs appear only in the applied-result record;
- archived IDs are audit data, never lifecycle targets.

### 4. `import_review_correction_generation_transition_v1`

Canonical file:

`supabase/repeatable/21072026_1820_01_import_review_lifecycle_rpcs.sql`

Implemented responsibilities:

- loads the three independent authorities separately on non-`PREPARE` calls:
  - reviewed request unit;
  - operation-bound correction-policy unit;
  - applied-result unit;
- cross-checks action ID, source identity, source shift, root/source timesheet, reviewed unit fingerprint and reconciliation fingerprint;
- accepts zero, one or two active mutable members at `PREPARE`;
- excludes archived IDs from lifecycle capabilities;
- validates exact applied reversal/replacement IDs, one shared correction ID/parent, member-set fingerprint, policy envelope and post-TSFIN financial/schedule equation;
- validates paid-rollover applied results separately from pair results;
- preserves idempotent `VALIDATE → AUTHORISE → VALIDATE` behaviour.

Verifier checks:

- wrong action/source/member substitution fails closed;
- missing applied or policy units fail closed;
- policy comparison uses the canonical operation envelope, not a whole policy-snapshot hash;
- `AUTHORISE` uses only exact actual applied member IDs and the reviewed intent;
- ordinary paid-source validation cannot be confused with a correction pair.

### 5. `timesheet_paid_uninvoiced_rollover_v1`

Canonical file:

`supabase/repeatable/21072026_1235_07_timesheet_paid_uninvoiced_rollover_v1.sql`

Implemented responsibilities:

- retains the existing correction-member rollover route;
- adds the ordinary authoritative `AMEND_PAID_UNINVOICED_SOURCE` route;
- validates the exact request unit and operation correction-policy unit without requiring a live correction-chain envelope for an ordinary source;
- requires `B=0`, no invoice IDs/lines, exact current paid TSFIN identity, current preflight fingerprint and no invoice lock;
- creates the existing current pending-calculation shell and preserves the historical paid TSFIN;
- persists operation, source, route, unit and frozen policy lineage;
- keeps same-operation replay idempotent.

Verifier checks:

- stale preflight, stale TSFIN, invoice evidence, wrong operation/policy unit or duplicate current shell fails closed;
- no TSFIN economics are calculated here;
- correction-member behaviour is unchanged.

### 6. `hr_weekly_apply_transactional`

Canonical file:

`supabase/repeatable/21072026_1820_06_hr_weekly_apply_transactional.sql`

Implemented responsibilities:

- applies the four routes only inside import-authoritative HealthRoster Weekly;
- routes paid ordinary sources through the rollover helper before the existing amendment/TSFIN flow;
- processes paid units in deterministic timesheet-ID order;
- reuses a prior shell only when its originating operation is complete, its own operation contract and historical paid digest are valid, and operation-independent policy facts still match;
- sends pair routes, but not paid ordinary sources, into phase 3;
- carries both pair member IDs and paid applied IDs into affected/postcondition state;
- restores `REAUTHORISE`, `AUTHORISE`, or `LEAVE_UNAUTHORISED` from reviewed intent;
- validates paid history/current shell and applied-result fingerprints before commit;
- preserves HealthRoster Weekly validation-only and Daily logic.

Verifier checks:

- no paid route can fall through to the old `PAID_UNINVOICED_ROLLOVER_REQUIRED` dead end after the operation-bound shell is created/reused;
- pair-only phase-3 keys do not include paid ordinary sources;
- validation-only and Daily paths cannot enter reconciliation generation;
- no archived ID enters affected, unauthorise or authorise arrays.

### 7. `nhsp_weekly_apply_transactional`

Canonical file:

`supabase/repeatable/21072026_1820_07_nhsp_weekly_apply_transactional.sql`

Implemented responsibilities and verifier checks are the NHSP equivalent of function 6, while preserving NHSP Policy A, cancellation, booking/reference evidence, and NHSP-specific source metadata.

The verifier must test NHSP independently; passing HealthRoster does not prove NHSP.

### 8. `hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

Canonical file:

`supabase/repeatable/21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql`

Implemented responsibilities:

- reads the exact operation-bound reviewed repair unit;
- locks the reviewed live scope, parent and contract-week allocation scope;
- handles zero, one or two surviving active members;
- leaves archived rows untouched;
- retains the old correction ID when its required unique role keys are usable;
- derives a deterministic fresh correction ID when an archived role occupies the old unique key, and rekeys surviving active members only;
- reuses or creates exactly one reversal and one replacement;
- normalises **both** schedules and metadata:
  - reversal = reviewed standard `B` schedule;
  - replacement = authoritative `A` schedule;
- reuses or creates one direct adjustment contract week per role and blocks ambiguous duplicates;
- writes actual correction/member/parent/repair/fingerprint fields into the applied result;
- enforces exact two-member, one-role-each, shared-parent, shared-week, exact-schedule and provenance postconditions;
- leaves no active member on the archived-tainted old ID when fresh-ID repair is required.

Verifier checks all deleted/changed/archived-role permutations, replay and transactional rollback after an injected mid-repair failure.

### 9. `nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

Canonical file:

`supabase/repeatable/21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql`

Implements the same repair algorithm and postconditions as function 8 while retaining NHSP-specific booking/reference/source evidence and contract-week metadata.

The verifier must independently prove the NHSP permutations and repeated-generation chain.

## Required behavioural test matrix

The independent review must map code and tests to, at minimum:

1. archived reversal + live replacement;
2. archived replacement + live reversal;
3. both old roles archived;
4. archived old generation beside a later valid generation;
5. archived rows byte-for-byte unchanged;
6. missing mutable reversal;
7. missing mutable replacement;
8. both mutable roles missing with durable generation proof;
9. changed reversal;
10. changed replacement;
11. both changed;
12. duplicate active role blocked;
13. missing parent/source authority blocked;
14. missing direct contract week created exactly once;
15. ambiguous contract weeks blocked;
16. replay reuses the same rows;
17. forced failure after first-role mutation rolls back the full Weekly transaction;
18. both roles economically invoiced with a deleted live role remains fully invoiced;
19. neither role invoiced with a deleted live role remains mutable;
20. exactly one role economically invoiced is the genuine partial blocker;
21. missing role without durable proof is scope-unprovable;
22. full credit and aggregate credit signs;
23. separable non-hours exclusion;
24. ordinary paid rollover for NHSP;
25. ordinary paid rollover for HealthRoster authoritative Weekly;
26. prior valid shell reuse without comparing full cross-operation fingerprints;
27. repeated fully invoiced sequence independently for both sources:
   `+10 → -10/+11 → -11/+12 → -12/+10`;
28. latest mutable uninvoiced positive is amended/repaired in place instead of creating another generation;
29. HealthRoster Weekly validation-only unchanged;
30. HealthRoster Daily unchanged;
31. import with more than 100 shifts is fully catalogued.

## Source, database and deployment evidence

Backend implementation commit before the handover-only follow-up commit:

```text
1973054 Complete weekly authoritative amendment repairs
branch: test
remote: github.com/kierarthur/cloudtms-backend
```

An unrelated remote Banking Pay commit arrived while this work was in progress:

```text
662ad80 Fix Banking alert and frozen breakdown details
```

The implementation commit was rebased cleanly on top of it. No file in that incoming Banking Pay commit overlapped this nine-function change.

TEST Supabase target:

```text
project: test-cloudtms
project id: yakevhtttcsljosbdpov
region: eu-west-2
status at install: ACTIVE_HEALTHY
PostgreSQL: 17
```

Pre-install atomic gates:

```text
active non-complete import_apply_operations: 0
active TSFIN follow-ups: 0
active review follow-ups: 0
exact target signatures present: 9
four-argument HealthRoster phase-3 overload present: 1
exact pre-install definition hashes matched rollback capture: true
```

The nine definitions were installed in one transaction in this order:

1. effective balance helper;
2. action catalogue;
3. apply envelope;
4. lifecycle transition;
5. paid rollover;
6. HealthRoster phase 3;
7. NHSP phase 3;
8. HealthRoster Weekly caller;
9. NHSP Weekly caller.

Installed live definition hashes:

```text
_import_review_effective_invoice_balance_core_v1  cc59734d26461f2d96aeb29ee54208a1
_import_review_action_catalog_core_v1             8f5803be69a54fccac3b5866502373c0
_import_review_apply_envelope_core_v1              10ef34abaff68a221f051b79d34eab4c
import_review_correction_generation_transition_v1 1d3481867a4326c471812ef7c6326f76
timesheet_paid_uninvoiced_rollover_v1              2632b3b506dcd2bd06a77dddad01e76d
hr_weekly_phase3_apply_adjustment_truth            788e9d8926c91cf654a9d36634944d94
nhsp_weekly_phase3_apply_adjustment_truth          2f8bd6ae765ae7fa7687979e9d29b150
hr_weekly_apply_transactional                      dd718b4805fcdfc281d7ad3e77025d71
nhsp_weekly_apply_transactional                    71bc064feb89a64bd8159c2a3c821970
```

Post-install proof:

```text
exact target definitions: 9
unchanged four-argument HR phase-3 overload: 1
retired archived/live blocker in target definitions: 0
active non-complete operations: 0
active TSFIN follow-ups: 0
```

No migration was required by the locked plan. The existing canonical repeatable definitions were replaced in place; no duplicate/latest function file was created.

No Worker or frontend deployment was required or performed. This is a database-function deployment only.

## Verification completed

Local focused suites after implementation:

```text
import authoritative reconciliation contract: 11/11 passed
combined reconciliation + follow-up suites:    27/27 passed
```

The combined suite covers:

- frozen `B/M/A` contract fields;
- deterministic batches beyond the helper's 100-source evidence window;
- the four routes;
- request/policy/applied evidence re-attestation;
- ordinary paid rollover;
- both Weekly callers;
- both phase-3 repair functions;
- archived exclusion;
- missing-role/fresh-ID repair markers;
- exact role/member/fingerprint postconditions;
- unchanged HealthRoster Daily;
- unchanged Worker follow-up contract.

Database checks:

- all nine functions compiled together in one rolled-back transaction;
- the full local nine-function set ran the latest NHSP and HealthRoster action catalogues and apply envelopes in a rolled-back transaction;
- after installation, the live latest HealthRoster catalogue returned 11 actions and the live latest NHSP catalogue returned 26 actions without runtime error;
- those two latest live imports had no current correction-route rows, so they did not provide a non-destructive live exercise of the destructive repair branches.

`plpgsql_check` is not installed in TEST, so no `plpgsql_check_function_tb` report is claimed.

## Rollback

Exact pre-install TEST definitions were captured from `pg_get_functiondef` before installation, in reverse dependency order, with their pre-install database MD5 hashes:

`31072026_2057_nhsp_healthroster_weekly_authoritative_amendments_rollback.sql`

The rollback changes only the nine target definitions. It does not alter data, migrations, schemas, Workers, frontend code, invoices, TSFIN results, or Banking Pay.

Rollback was captured and syntax/source-length validated, but was not executed because the installation passed.

## Deliberately untouched

- all `pay_*` and Banking Pay functions;
- payment batches, payment execution, CSV/provider/settlement/remittance paths;
- invoice generation, line creation, issue, unissue, credit, VAT, totals and documents;
- `invoice_correction_pair_scope_v1`;
- general TSFIN calculators, rates, rounding, queues and Workers;
- `import_review_apply_guard_v1`;
- `_import_review_apply_complete_core_v1`;
- generic authorise/unauthorise functions;
- general correction-chain functions;
- Worker JavaScript;
- frontend JavaScript;
- Bulk Authorise;
- HealthRoster Daily;
- HealthRoster Weekly validation-only behaviour;
- ordinary non-authoritative imports.

## Safety and repository state

```text
Production accessed/deployed: no
TEST Supabase changed: yes, explicitly authorised; nine functions only
Normal TEST Worker deployed: no
Frontend deployed: no
Payment/provider action: no
Invoice/timesheet/import data mutation during verification: no
Destructive SQL or rollback executed: no
Secrets printed or committed: no
Policy X drift: none; Banking Pay untouched
```

The pre-existing unrelated local edit below was preserved and excluded from both implementation and handover commits:

`codex_outputs/banking-pay-pending-owner-recovery/closure-correction-and-independent-verification-handover.md`

The older untracked verification pack already present in the output directory was also preserved and excluded.
