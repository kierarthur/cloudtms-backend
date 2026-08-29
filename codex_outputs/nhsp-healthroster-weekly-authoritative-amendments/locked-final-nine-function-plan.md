# NHSP/HealthRoster Weekly Authoritative Amendments Plan

## Final corrected nine-function repair plan

## 1. Final verdict and implementation surface

The latest review is correct.

Two requirements remain unimplemented in the deployed TEST definitions:

1. Archived timesheets are still participating in current reconciliation decisions through an archived/live conflict check.
2. The current phase-3 mutable route still requires an intact two-member pair and updates only the positive replacement.

The source-specific phase-3 functions are the only existing functions that own normal NHSP and HealthRoster correction-row creation and schedule mutation. They therefore must be included.

The final repair surface is **nine existing SQL functions**:

```text
1. public._import_review_effective_invoice_balance_core_v1
2. public._import_review_action_catalog_core_v1
3. public._import_review_apply_envelope_core_v1
4. public.import_review_correction_generation_transition_v1
5. public.timesheet_paid_uninvoiced_rollover_v1
6. public.hr_weekly_apply_transactional
7. public.nhsp_weekly_apply_transactional
8. public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)
9. public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)
```

No new function, table, column, trigger, index, backend file, frontend file, Worker file, or migration is required.

No implementation or database change is authorised by this plan.

---

# 2. Locked scope

The repair applies only to:

```text
NHSP Weekly import-authoritative processing
HealthRoster Weekly import-authoritative processing
```

It remains confined to:

```text
effective frozen invoice-history interpretation
active mutable-generation interpretation
Import Review route selection
request/apply evidence contracts
ordinary paid-source rollover execution
source-specific correction-row repair or creation
post-TSFIN reconciliation validation
```

It does not change:

```text
Banking Pay
any pay_* function
payment batches
payment execution
payment exports
provider submission
settlement
remittances
Policy X

invoice generation
invoice-line creation
invoice issue
invoice unissue
credit-note creation
VAT
invoice totals
invoice documents
invoice PDFs
invoice Workers

general TSFIN calculation
TSFIN rates
TSFIN rounding
TSFIN component generation
TSFIN queues or Workers

general lifecycle functions
general correction-chain functions
invoice_correction_pair_scope_v1
Bulk Authorise
frontend
backend follow-up Worker
HealthRoster Daily
HealthRoster Weekly validation-only
other non-authoritative imports
```

---

# 3. Governing reconciliation rule

For each exact source shift:

```text
B = complete signed economically effective frozen invoiced position

M = all current, active, non-archived, mutable and uninvoiced
    correction members for the source

A = latest authoritative imported schedule and hours
```

After repair and existing TSFIN settlement:

```text
B + corrected outstanding position = A
```

For an ordinary two-member correction generation:

```text
reversal schedule        = the normal schedule representing B
reversal TSFIN           = -B
corrected-hours schedule = A
corrected-hours TSFIN    = A financial result
```

Existing TSFIN remains the sole authority for:

```text
pay
charge
margin
rates
rounding
financial components
processing status
```

No import helper writes or independently calculates final TSFIN economics.

---

# 4. Archived-timesheet contract

## 4.1 Archived rows are audit-only

An archived timesheet is the safe equivalent of a removed row.

It must:

```text
contribute zero to B
contribute zero to M
not establish an active mutable generation
not influence route selection
not enter PREPARE
not enter phase 3 as an existing row
not enter VALIDATE
not enter AUTHORISE
not be reactivated
not be made current
not be amended
not be reused as a correction member
not be reused as a parent
not be queued for TSFIN
```

Archived IDs may be returned only under audit diagnostics:

```text
archived_history_timesheet_ids
archived_history_roles
```

They are not blocker inputs or mutation targets.

The helper must remove the current:

```text
v_archived_active_conflict
IMPORT_REVIEW_ARCHIVED_GENERATION_ACTIVE_MEMBER_CONFLICT
```

route entirely.

No replacement archived-specific blocker is introduced.

## 4.2 Archived role with a live sibling

An archived reversal plus a live replacement, or an archived replacement plus a live reversal, is not blocked merely because the archived row exists.

The archived role is ignored.

The live sibling remains part of active `M` and must be normalised into one valid current pair.

The installed unique index is:

```sql
UNIQUE (correction_id, correction_kind)
WHERE correction_id IS NOT NULL
```

and includes archived rows. Therefore, a new row cannot reuse the archived row’s old:

```text
correction_id + correction_kind
```

The exact repair is:

1. Leave the archived row unchanged.
2. Derive a fresh deterministic repair correction ID.
3. Re-key the surviving active sibling to that fresh correction ID.
4. Create the absent active role under the same fresh ID.
5. Normalise both active schedules.
6. End with exactly one active non-archived pair.
7. Leave no active row under the archived generation’s old correction ID.

This is not an additional overlapping financial generation. It replaces an unusable active orphan identity with one complete active generation.

## 4.3 Entirely archived old generation

Where both old roles are archived:

```text
M = 0
```

The old correction ID is ignored.

The route is selected from current `B` and `A`:

```text
B = 0
→ ordinary fresh/include source route

B > 0
→ fresh reversal/replacement generation
```

## 4.4 Archived history beside a later valid generation

Archived rows are ignored.

The later active generation is selected and processed normally.

---

# 5. Deleted and manually changed mutable members

A current mutable generation must not fail merely because:

```text
the reversal was physically deleted
the replacement was physically deleted
both roles were physically deleted
the reversal schedule was manually changed
the replacement schedule was manually changed
either role’s correction metadata was altered
```

provided all are true:

```text
the durable generation identity is uniquely proved
neither role has effective invoice evidence
neither role is in a pending invoice
no surviving role is archived
no surviving role is paid
the source, parent and frozen policy are provable
```

## 5.1 Missing role

Where one active mutable role is physically absent:

* retain the existing correction ID if no archived row occupies that role;
* recreate the missing role through phase 3;
* reuse the surviving active role;
* normalise both schedules and metadata.

## 5.2 Both roles missing

Where one unique wholly uninvoiced generation is proved by durable operation and audit evidence:

* retain its correction ID if no archived role occupies it;
* create both roles;
* create their normal direct adjustment contract weeks;
* do not create a second correction generation.

## 5.3 Manually changed reversal

The reversal must be updated, not left untouched.

Its final state must have:

```text
actual_schedule_json = reviewed B reversal schedule
correction_kind      = CHANGED_HOURS_REVERSAL
correction_id        = target repair correction ID
parent_timesheet_id  = reviewed shared parent
source provenance    = reviewed source
policy envelope      = current operation contract envelope
```

The existing TSFIN process must then recalculate it and post-TSFIN validation must prove:

```text
reversal TSFIN = -B
```

## 5.4 Manually changed replacement

Its final state must have:

```text
actual_schedule_json = A authoritative schedule
correction_kind      = CHANGED_HOURS_REPLACEMENT
correction_id        = target repair correction ID
parent_timesheet_id  = reviewed shared parent
source provenance    = reviewed source
policy envelope      = current operation contract envelope
```

## 5.5 Both roles changed

Both are normalised in the same phase-3 transaction.

The repair must not update only the replacement.

---

# 6. Exact treatment of one-sided invoice evidence

A one-sided live shape is not automatically a partially invoiced generation.

The helper must first reconstruct both expected roles from durable evidence and independently determine each role’s economically effective invoice status.

## 6.1 Fully invoiced despite a missing live row

Where both expected roles have effective frozen Weekly-hours invoice evidence:

```text
generation state = FULLY_INVOICED
```

This remains true even if:

```text
one live row was physically deleted
both live rows were physically deleted
one or both surviving rows were manually changed
```

The live shape does not override the frozen invoice evidence.

The next authoritative change may create a fresh generation from complete `B`.

## 6.2 Wholly uninvoiced mutable despite a missing row

Where neither expected role has effective or pending invoice evidence:

```text
generation state = MUTABLE
```

A missing or altered member is repaired through the same correction generation as described above.

## 6.3 Genuine partial invoice

A genuine partial state exists only after durable evidence proves:

```text
exactly one expected role has effective invoice evidence

AND

the other expected role is positively proved to have existed
but has no effective invoice evidence
```

That state remains blocked because the standard current correction record cannot be normalised without one of the following:

```text
altering the immutable issued role
retiring or re-homing the unissued role
leaving duplicate outstanding liability
creating a non-standard positive reversal
```

Those actions are outside the approved correction shape.

Use:

```text
IMPORT_REVIEW_CORRECTION_GENERATION_PARTIALLY_INVOICED
```

This blocker is based on genuine economic state, not on a missing or modified live row.

## 6.4 Unprovable missing role

If the missing role cannot be determined to be:

```text
effectively invoiced
or
wholly uninvoiced and mutable
```

return:

```text
IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE
```

Do not guess and do not label it partial.

---

# 7. Three immutable operation authorities

## 7.1 Reviewed request unit

Location:

```text
response_json.request_envelope.reconciliation_units[]
```

Contains pre-apply authority:

```text
action_id
source_identity
source_shift_id
route
reconciliation_mode

B/M/A evidence
active member IDs
missing role names
archived audit IDs

reviewed existing correction ID, where applicable
repair identity mode
review policy-basis fingerprint

reconciliation fingerprint
unit fingerprint
```

It must not contain future generated member IDs.

## 7.2 Operation-bound policy unit

Location:

```text
response_json.correction_operation_contract.correction_units[]
```

Contains:

```text
action_id
root_timesheet_id
source_row_key
source_shift_id
expected roles
operation-bound policy envelope
policy_envelope_fingerprint
```

This is the policy authority for resulting current correction members.

## 7.3 Applied result unit

Location:

```text
response_json.reconciliation_units[]
```

Contains actual applied state:

```text
action_id
source_identity
source_shift_id
reviewed_unit_fingerprint
reconciliation_fingerprint

actual correction_id
applied_member_ids
reversal_timesheet_id
replacement_timesheet_id
parent_timesheet_id

repair_identity_mode
applied_result_fingerprint
```

## 7.4 Mandatory cross-check

The transition must require exact agreement on:

```text
action ID
source identity
source shift ID
reviewed unit fingerprint
reconciliation fingerprint
root/source timesheet
```

The resulting rows must match:

```text
actual correction ID
actual member IDs
one reversal role
one replacement role
one shared parent
```

No evidence from another action or source may be substituted.

---

# 8. Policy-envelope authority

The request unit stores only:

```text
review_policy_basis_kind
review_policy_basis_fingerprint
```

That is a pre-apply settings/evidence fingerprint.

It is not the future operation-bound correction envelope.

After apply, the transition must:

1. Select the exact operation correction unit.
2. Read each resulting member’s canonical envelope through:

```sql
public._ctms_correction_policy_envelope_read_v1(timesheet_id)
```

3. Compare:

```text
member envelope fingerprint
=
operation correction unit policy_envelope_fingerprint
```

4. Never compare that value with a hash of the complete `policy_snapshot_json`.

The reconciliation schema remains:

```text
IMPORT_AUTHORITATIVE_RECONCILIATION_V1
```

---

# 9. Non-hours and credit treatment

Only Weekly-hours components enter `B`.

## 9.1 Included

```text
line_type = HOURS_WEEKLY
```

or an exact legacy frozen TSFIN segment proving the same Weekly-hours component.

## 9.2 Ignored

Separable:

```text
expenses
mileage
travel
accommodation
other non-hours additions
```

They do not contribute to:

```text
B hours
B pay
B charge
B margin
route selection
```

They do not create a blocker merely because they coexist on the same timesheet or invoice.

## 9.3 Inseparable artifact

Where Weekly hours cannot be separated from non-hours economics:

```text
IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE
```

with internal reason:

```text
WEEKLY_HOURS_COMPONENT_NOT_SEPARABLE
```

## 9.4 Credit hours

For an effective credit:

* resolve the original invoice line;
* resolve its exact frozen source component;
* use negative original component hours;
* use the credit’s already-signed financial values once;
* do not negate those financial values twice;
* permit aggregate lines where the original frozen segment isolates the source.

---

# 10. Ordinary paid-source route

The existing paid rollover helper must be narrowly amended because its current correction-envelope preflight cannot succeed for an ordinary source row.

The final route remains:

```text
canonical unauthorise where required
→ validate or create current rollover shell
→ amend same timesheet ID
→ existing TSFIN recalculation
→ reviewed authorisation outcome
```

It must never enter phase 3 solely because it was paid.

Existing-shell reuse validates the shell against its own originating completed operation and compares only operation-independent policy facts with the current review.

Complete operation-bound fingerprints from different operations are not expected to match.

---

# 11. Final route matrix

| State                                               | Route                                               |
| --------------------------------------------------- | --------------------------------------------------- |
| Ordinary unpaid, uninvoiced source                  | `AMEND_SOURCE`                                      |
| Ordinary paid, uninvoiced source                    | `AMEND_PAID_UNINVOICED_SOURCE`                      |
| Intact wholly uninvoiced mutable pair               | `AMEND_EXISTING_REPLACEMENT` / retain ID            |
| Deleted mutable reversal                            | `AMEND_EXISTING_REPLACEMENT` / recreate reversal    |
| Deleted mutable replacement                         | `AMEND_EXISTING_REPLACEMENT` / recreate replacement |
| Both mutable roles deleted                          | `AMEND_EXISTING_REPLACEMENT` / recreate both        |
| Mutable reversal changed                            | `AMEND_EXISTING_REPLACEMENT` / normalise both       |
| Mutable replacement changed                         | `AMEND_EXISTING_REPLACEMENT` / normalise both       |
| Archived role plus live sibling                     | `AMEND_EXISTING_REPLACEMENT` / fresh repair ID      |
| Entire old generation archived                      | Ignore archived generation; select fresh route      |
| Archived rows beside later valid generation         | Ignore archived rows                                |
| Both expected historical roles effectively invoiced | `CREATE_REVERSAL_REPLACEMENT`                       |
| Both effectively invoiced but one live row deleted  | `CREATE_REVERSAL_REPLACEMENT`                       |
| Neither role invoiced but one live row deleted      | Mutable repair                                      |
| Exactly one role effectively invoiced               | True partial blocker                                |
| Missing role has insufficient durable evidence      | Scope-unprovable blocker                            |
| HealthRoster validation-only or Daily               | Existing validation/reference route                 |

---

# 12. Function-by-function implementation

## 12.1 `public._import_review_effective_invoice_balance_core_v1`

### Signature

```sql
public._import_review_effective_invoice_balance_core_v1(
  uuid,
  jsonb,
  integer,
  integer,
  integer,
  integer
)
RETURNS TABLE(source_identity text, balance_json jsonb)
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_00_import_review_internal_core.sql
```

### Current responsibility

Calculates effective invoice history, mutable generation state, authoritative schedule, representability, and reconciliation fingerprints.

### Proven defects

The current definition:

* derives partial state from surviving timesheet rows;
* creates an archived/live conflict blocker;
* allows archived rows to influence generation state;
* treats missing roles incompletely;
* mis-signs credit hours;
* allows non-hours line contamination.

### Exact amendment

1. Remove:

   ```text
   v_archived_active_conflict
   IMPORT_REVIEW_ARCHIVED_GENERATION_ACTIVE_MEMBER_CONFLICT
   ```
2. Keep archived IDs only under:

   ```text
   archived_history_timesheet_ids
   archived_history_roles
   ```
3. Exclude archived rows from:

   ```text
   B candidates
   M members
   mutable-generation identity
   route inputs
   active member IDs
   ```
4. Reconstruct expected roles using:

   ```text
   reviewed request unit
   applied result unit
   operation correction contract
   correction audit
   invoice-line/frozen TSFIN evidence
   surviving active rows
   ```
5. Classify each expected role independently as:

   ```text
   EFFECTIVE_INVOICED
   PENDING_INVOICE
   ACTIVE_MUTABLE
   PHYSICALLY_MISSING_MUTABLE
   ARCHIVED_AUDIT_ONLY
   UNPROVABLE
   ```
6. Determine:

   ```text
   FULLY_INVOICED
   MUTABLE
   PARTIALLY_INVOICED
   UNPROVABLE
   ```

   from expected-role evidence, not live row count.
7. Derive:

   ```text
   active_mutable_member_ids
   physically_missing_mutable_roles
   archived_ignored_roles
   reviewed_existing_correction_id
   repair_identity_mode
   ```
8. Set:

   ```text
   RETAIN_EXISTING_CORRECTION_ID
   ```

   where the absent role is physically missing and no archived row occupies its unique key.
9. Set:

   ```text
   FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED
   ```

   where an archived role occupies the old unique key.
10. Detect manually changed schedules by comparing each active role with the reviewed target schedule.
11. Include only Weekly-hours invoice components.
12. Correct credit-hour signs from the original frozen component.
13. Return explicit role-evidence and ignored-non-hours diagnostics.
14. Include all new state in the effective and reconciliation fingerprints.

### Locking

Read-only.

Caller-held locks remain authoritative.

### Failure behaviour

```text
IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE
IMPORT_REVIEW_EFFECTIVE_CREDIT_AMBIGUOUS
IMPORT_REVIEW_CORRECTION_GENERATION_PARTIALLY_INVOICED
```

No archived-history business blocker remains.

### Idempotency

The same source, role evidence, invoice evidence, mutable members, and authoritative input produce the same fingerprint.

### Focused tests

```text
archived reversal + live replacement
archived replacement + live reversal
entirely archived generation
archived history beside later valid generation

deleted reversal
deleted replacement
both deleted
changed reversal
changed replacement
both changed

both historical roles invoiced but one live row deleted
neither invoiced and one live row deleted
true one-sided invoice
missing role unprovable

credit sign
non-hours exclusion
```

### Preserved behaviour

No data write and no financial calculation change.

---

## 12.2 `public._import_review_action_catalog_core_v1`

### Signature

```sql
public._import_review_action_catalog_core_v1(
  uuid,
  integer,
  integer
)
RETURNS TABLE(...)
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_00_import_review_internal_core.sql
```

### Current responsibility

Creates Import Review route, evidence fingerprint, selectability, and proposed outcome.

### Exact amendment

Route precedence:

```text
helper blocking code
→ blocked action

active mutable generation
→ AMEND_EXISTING_REPLACEMENT

effective Weekly-hours position positive and representable
→ CREATE_REVERSAL_REPLACEMENT

effective Weekly-hours position zero
→ ordinary B=0 route

paid ordinary source
→ AMEND_PAID_UNINVOICED_SOURCE

otherwise
→ AMEND_SOURCE
```

Archived history is not considered in route selection.

For mutable repair, add:

```text
reviewed_existing_correction_id
repair_identity_mode
active_mutable_member_ids
physically_missing_mutable_roles
archived_ignored_roles
reversal_repair_required
replacement_repair_required
```

The existing user-facing route remains:

```text
AMEND_EXISTING_REPLACEMENT
```

No new frontend label is required.

### Locking

Read-only.

### Failure behaviour

True partial and unprovable evidence retain their existing friendly blockers.

### Idempotency

Every mutable-role and archived-exclusion fact enters the action evidence fingerprint.

### Focused tests

Every route in section 11.

### Preserved behaviour

Daily, validation-only, mapping, cancellation, email, reference, and UI contracts remain unchanged.

---

## 12.3 `public._import_review_apply_envelope_core_v1`

### Signature

```sql
public._import_review_apply_envelope_core_v1(uuid)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_00_import_review_internal_core.sql
```

It must not be moved or duplicated in the lifecycle RPC repeatable.

### Current responsibility

Freezes reviewed pre-apply intent.

### Exact amendment

The request reconciliation unit contains:

```text
action_id
source_identity
source_shift_id
source_timesheet_id

route
reconciliation_mode

B/M/A reviewed evidence
reviewed existing correction ID
active member IDs
physically missing roles
archived audit roles
repair identity mode

review policy-basis kind
review policy-basis fingerprint

reconciliation fingerprint
unit fingerprint
```

For fresh pair or fresh archived-role repair, it does not contain:

```text
generated correction ID
actual reversal ID
actual replacement ID
future operation-bound policy-envelope fingerprint
```

For retained-ID mutable repair, it may contain:

```text
reviewed_existing_correction_id
```

### Locking

Read-only.

### Idempotency

Same review gives the same request unit and request hash.

### Failure behaviour

```text
IMPORT_REVIEW_RECONCILIATION_UNIT_INVALID
```

### Focused tests

* Correct canonical source file.
* No generated IDs before apply.
* Repair mode and missing-role directives frozen.
* Archived IDs audit-only.
* Policy basis, not future envelope, frozen.

### Preserved behaviour

Top-level apply-envelope version remains unchanged.

---

## 12.4 `public.import_review_correction_generation_transition_v1`

### Signature

```sql
public.import_review_correction_generation_transition_v1(
  uuid,
  uuid,
  text,
  text,
  uuid,
  text[],
  timestamptz
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_01_import_review_lifecycle_rpcs.sql
```

### Current responsibility

Performs operation-bound `PREPARE`, `VALIDATE`, and `AUTHORISE`.

### Exact amendment

#### Evidence authority

Select and cross-check exactly:

```text
request_envelope.reconciliation_units[]
top-level response_json.reconciliation_units[]
correction_operation_contract.correction_units[]
```

#### Archived exclusion

`PREPARE` targets only:

```text
request.active_mutable_member_ids
```

Archived audit IDs are never joined to mutation rows.

If an applied result names an archived row, return:

```text
IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID
```

This is a phase-3 contract failure, not an archived-history business blocker.

#### PREPARE

* If no active member survives, return successful no-op.
* If one active member survives, unauthorise only that active member through the existing operation-bound lifecycle path.
* Missing roles are created later by phase 3.
* Archived roles are ignored.

#### VALIDATE

After phase 3, require the applied result to identify exactly:

```text
one active non-archived reversal
one active non-archived replacement
```

Compare each member’s canonical policy envelope with the operation correction-contract envelope.

Validate:

```text
reversal TSFIN = -B
replacement schedule/hours = A
final equation holds
```

#### AUTHORISE

Authorise both actual applied member IDs according to the reviewed intent.

No archived ID may enter the capability set.

### Locking

Existing operation, row, TSFIN, contract-week, and invoice locks remain.

### Failure behaviour

```text
IMPORT_REVIEW_RECONCILIATION_EVIDENCE_CONTRACT_INVALID
IMPORT_REVIEW_RECONCILIATION_APPLIED_RESULT_MISSING
IMPORT_REVIEW_RECONCILIATION_POLICY_UNIT_MISSING
IMPORT_REVIEW_RECONCILIATION_POLICY_MISMATCH
IMPORT_REVIEW_RECONCILIATION_MEMBER_SET_MISMATCH
IMPORT_REVIEW_RECONCILIATION_BALANCE_MISMATCH
```

### Idempotency

Repeated `PREPARE`, `VALIDATE`, or `AUTHORISE` returns successful idempotent state when the applied result remains unchanged.

### Focused tests

```text
one surviving active role
no surviving active roles
archived IDs excluded
phase3 applied result contains exactly two active members
wrong source/member substitution rejected
policy-envelope comparison
VALIDATE → AUTHORISE → VALIDATE
```

### Preserved behaviour

Existing lifecycle capability, operation, actor, and retry controls remain.

---

## 12.5 `public.timesheet_paid_uninvoiced_rollover_v1`

### Signature

```sql
public.timesheet_paid_uninvoiced_rollover_v1(
  uuid,
  uuid,
  uuid,
  uuid,
  text,
  timestamptz
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1235_07_timesheet_paid_uninvoiced_rollover_v1.sql
```

### Current responsibility

Rotates one paid uninvoiced current TSFIN into historical paid truth plus one current pending-calculation shell.

### Exact amendment

Retain the current correction-member path unchanged.

Add the ordinary-source path:

1. Require selected route:

   ```text
   AMEND_PAID_UNINVOICED_SOURCE
   ```
2. Validate the operation request unit.
3. Validate the operation correction-policy unit independently of correction-chain envelope lookup.
4. Run ordinary paid-source preflight without requiring a live correction-chain envelope.
5. Require exact current paid TSFIN identity and fingerprint.
6. Create the shell exactly as the current function already does.
7. Persist current-operation envelope and rollover lineage.

Cross-operation shell reuse remains the callers’ responsibility.

### Locking

Existing operation/timesheet/TSFIN locks remain.

### Failure behaviour

```text
PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_OPERATION_UNIT_INVALID
PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_POLICY_INVALID
PAID_TSFIN_ROLLOVER_ORDINARY_SOURCE_PREFLIGHT_INVALID
```

### Idempotency

Same-operation replay remains unchanged.

### Focused tests

Ordinary paid source, correction-member non-regression, stale preflight, invoice blocker, replay, duplicate-shell prevention.

### Preserved behaviour

No TSFIN calculator or financial-path change.

---

## 12.6 `public.hr_weekly_apply_transactional`

### Signature

```sql
public.hr_weekly_apply_transactional(
  uuid,
  jsonb,
  uuid
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_06_hr_weekly_apply_transactional.sql
```

### Current responsibility

Applies authoritative HealthRoster Weekly source truth.

### Exact amendment

#### Paid source

Implement the exact paid-source rollover route already defined in the seven-function plan.

#### Mutable repair

1. Build repair keys from reviewed:

   ```text
   AMEND_EXISTING_REPLACEMENT
   ```
2. Call transition `PREPARE` with only active non-archived member IDs.
3. Permit:

   ```text
   zero existing members
   one existing member
   two existing members
   ```
4. Call HealthRoster phase 3.
5. Require phase 3 to return:

   ```text
   actual correction ID
   exactly two applied member IDs
   one reversal
   one replacement
   ```
6. Add both applied members to:

   ```text
   affected_timesheet_ids
   ```
7. Persist the top-level applied reconciliation unit.
8. Preserve reviewed authorisation intent.

Archived IDs must not enter any mutation or authorisation array.

### Locking

Uses guard and phase-3 locks.

### Failure behaviour

Any incomplete phase-3 repair rolls back the whole Weekly transaction.

### Idempotency

Committed operation replay returns the stored response.

### Focused tests

Paid route plus all HealthRoster mutable repair cases.

### Preserved behaviour

Validation-only, Daily, cancellation, phase 1, phase 1.5, evidence, and existing TSFIN follow-up remain unchanged.

---

## 12.7 `public.nhsp_weekly_apply_transactional`

### Signature

```sql
public.nhsp_weekly_apply_transactional(
  uuid,
  jsonb,
  uuid
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1820_07_nhsp_weekly_apply_transactional.sql
```

### Current responsibility

Applies authoritative NHSP Weekly source truth.

### Exact amendment

Implement the same:

```text
paid-source route
mutable repair orchestration
active-member PREPARE
phase-3 applied-result postconditions
affected and authorisation arrays
```

Preserve:

```text
NHSP Policy A
NHSP cancellation behaviour
references
source evidence
audit terminology
```

### Locking, failure, and idempotency

Same as HealthRoster.

### Focused tests

The complete NHSP paid and mutable-repair suite independently.

### Preserved behaviour

All unrelated NHSP logic remains unchanged.

---

## 12.8 `public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

### Signature

```sql
public.hr_weekly_phase3_apply_adjustment_truth(
  uuid,
  text[],
  uuid
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql
```

The four-argument overload remains unchanged.

### Current responsibility

Creates or amends normal HealthRoster correction timesheets and adjustment contract weeks.

### Proven defects

The current mutable branch:

```text
requires count(*) = 2
raises CORRECTION_PAIR_INCOMPLETE if a role is missing
updates only the replacement schedule
leaves a changed reversal unchanged
```

### Exact operation-bound repair branch

For `AMEND_EXISTING_REPLACEMENT`:

1. Read the reviewed repair unit.

2. Lock:

   ```text
   active unarchived rows under the reviewed correction ID
   active reviewed member IDs
   source parent row
   contract-week allocation scope
   ```

3. Read archived rows only to determine whether the old unique key is occupied.

4. Determine target correction ID:

   ```text
   RETAIN_EXISTING_CORRECTION_ID
   ```

   when no archived role occupies the old ID.

   ```text
   FRESH_CORRECTION_ID_ARCHIVED_ROLE_IGNORED
   ```

   when an archived row occupies one of the old correction-role keys.

5. For fresh-ID repair:

   * derive a deterministic ID from operation ID, source identity, old ID, archived-role evidence, and reconciliation fingerprint;
   * re-key every surviving active unarchived role to that new ID;
   * update its operation provenance and policy envelope.

6. Resolve or create the reversal:

   * reuse the active row if it exists;
   * otherwise insert a normal HealthRoster reversal row;
   * set schedule to reviewed `B_standard_schedule_json`;
   * set role, correction ID, parent, source and current operation policy.

7. Resolve or create the replacement:

   * reuse the active row if it exists;
   * otherwise insert a normal HealthRoster replacement row;
   * set schedule to `A_schedule_json`;
   * set role, correction ID, parent, source and current operation policy.

8. Normalise both rows, not only the replacement.

9. For each resulting role:

   * reuse one exact direct adjustment contract week where valid;
   * create one through the existing sequence-allocation logic where the role was recreated or no row exists;
   * block ambiguous duplicate contract weeks.

10. Update the transaction-local reconciliation unit with:

    ```text
    correction_id
    reversal_timesheet_id
    replacement_timesheet_id
    applied_member_ids
    parent_timesheet_id
    repair_identity_mode
    ```

11. Enforce postconditions:

    ```text
    exactly one active non-archived reversal
    exactly one active non-archived replacement
    one correction ID
    one parent
    no other active row under the old archived-tainted ID
    schedules exactly equal B and A targets
    source and policy provenance match
    ```

12. Return both member IDs as updated or created.

### Direct financial calculation

None.

### Locking

Deterministic row and sequence locks inside the existing Weekly transaction.

### Failure behaviour

```text
IMPORT_REVIEW_MUTABLE_GENERATION_EVIDENCE_UNPROVABLE
IMPORT_REVIEW_MUTABLE_GENERATION_ROLE_DUPLICATE
IMPORT_REVIEW_MUTABLE_GENERATION_PARENT_INVALID
IMPORT_REVIEW_MUTABLE_GENERATION_CONTRACT_WEEK_AMBIGUOUS
IMPORT_REVIEW_MUTABLE_GENERATION_REPAIR_POSTCONDITION_FAILED
```

### Idempotency

* Existing unique correction ID/kind index remains the insert backstop.
* Deterministic fresh repair ID remains stable for the same operation.
* Retry reuses resulting rows and verifies them.
* No duplicate active role is created.

### Focused tests

```text
deleted reversal
deleted replacement
both deleted
changed reversal
changed replacement
both changed
archived reversal + live replacement
archived replacement + live reversal
entire archived generation
replay
forced failure after first role mutation → total rollback
```

### Preserved behaviour

Legacy non-reconciliation calls and normal HealthRoster correction creation remain unchanged.

---

## 12.9 `public.nhsp_weekly_phase3_apply_adjustment_truth`

### Signature

```sql
public.nhsp_weekly_phase3_apply_adjustment_truth(
  uuid,
  text[],
  uuid
)
RETURNS jsonb
```

### Canonical file

```text
supabase/repeatable/
21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql
```

### Current responsibility

Creates or amends normal NHSP correction timesheets and adjustment contract weeks.

### Proven defects

The current mutable branch:

```text
requires an intact two-member pair
raises CORRECTION_PAIR_INCOMPLETE on a missing role
updates only the replacement
does not restore a changed reversal schedule
```

### Exact amendment

Implement the same operation-bound repair algorithm as HealthRoster, preserving NHSP-specific:

```text
source metadata
reference fields
booking identity
NHSP basis
audit wording
contract-week metadata
```

### Locking, failure, idempotency, and tests

Identical contracts to the HealthRoster function, executed independently.

### Preserved behaviour

Legacy NHSP paths and TSFIN calculation remain unchanged.

---

# 13. Complete focused test matrix

## Archived history

1. Archived reversal plus live replacement:

   * no blocker;
   * archived row untouched;
   * live replacement re-keyed to fresh repair ID;
   * new reversal created;
   * exactly one active pair.

2. Archived replacement plus live reversal:

   * equivalent result.

3. Entire old generation archived:

   * no active mutable generation;
   * archived IDs never enter lifecycle or phase 3;
   * fresh route selected from `B` and `A`.

4. Archived old generation plus later valid active generation:

   * later generation selected;
   * archived generation ignored.

5. Archived row digest remains identical before and after.

## Mutable repair

6. Missing reversal recreated under same ID.

7. Missing replacement recreated under same ID.

8. Both missing recreated under same ID.

9. Changed reversal restored to reviewed `B` schedule.

10. Changed replacement restored to `A`.

11. Both changed restored atomically.

12. Duplicate role fails before mutation.

13. Missing parent/source authority fails before mutation.

14. Missing contract week for a recreated role is created once.

15. Ambiguous contract weeks block.

16. Repeat apply reuses same rows.

17. Forced failure after reversal update but before replacement update rolls back both.

## Invoice classification

18. Both roles invoiced, reversal live row deleted:

    * fully invoiced;
    * not partial.

19. Both roles invoiced, replacement deleted:

    * fully invoiced.

20. Neither role invoiced, reversal deleted:

    * mutable repair.

21. Neither role invoiced, replacement deleted:

    * mutable repair.

22. Exactly one role invoiced:

    * true partial blocker.

23. Missing role with no durable evidence:

    * source-unprovable blocker.

## Existing resolved defects

24. Full credit nets hours/pay/charge correctly.

25. Aggregate credit uses original frozen segment.

26. Expense/travel/accommodation lines ignored.

27. Current operation policy envelope validates resulting members.

28. Ordinary paid source rollover succeeds for NHSP.

29. Ordinary paid source rollover succeeds for HealthRoster.

30. Prior valid shell reused without comparing full cross-operation fingerprints.

31. Repeated correction sequence remains:

```text
+10 → -10 +11
+11 → -11 +12
+12 → -12 +10
```

independently for both source systems.

---

# 14. Atomic deployment plan

## 14.1 Pre-installation gates

Immediately before installation:

```text
active non-complete import_apply_operations = 0
active TSFIN follow-ups = 0
```

Also require:

```text
no unresolved local target-file changes
all nine prior complete definitions captured
one existing definition per exact signature
```

## 14.2 Canonical files

```text
21072026_1820_00_import_review_internal_core.sql
  _import_review_effective_invoice_balance_core_v1
  _import_review_action_catalog_core_v1
  _import_review_apply_envelope_core_v1

21072026_1820_01_import_review_lifecycle_rpcs.sql
  import_review_correction_generation_transition_v1

21072026_1235_07_timesheet_paid_uninvoiced_rollover_v1.sql
  timesheet_paid_uninvoiced_rollover_v1

21072026_1820_06_hr_weekly_apply_transactional.sql
  hr_weekly_apply_transactional

21072026_1820_07_nhsp_weekly_apply_transactional.sql
  nhsp_weekly_apply_transactional

21072026_1235_24_hr_weekly_phase3_apply_adjustment_truth_3arg.sql
  hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)

21072026_1235_26_nhsp_weekly_phase3_apply_adjustment_truth.sql
  nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)
```

## 14.3 Installation order

Install all nine complete definitions in one transaction:

```text
1. _import_review_effective_invoice_balance_core_v1
2. _import_review_action_catalog_core_v1
3. _import_review_apply_envelope_core_v1
4. import_review_correction_generation_transition_v1
5. timesheet_paid_uninvoiced_rollover_v1
6. hr_weekly_phase3_apply_adjustment_truth
7. nhsp_weekly_phase3_apply_adjustment_truth
8. hr_weekly_apply_transactional
9. nhsp_weekly_apply_transactional
```

No migration is required.

No JavaScript deployment is required.

## 14.4 Definition-count gate

Require exactly one definition for:

```text
_import_review_effective_invoice_balance_core_v1
  (uuid,jsonb,integer,integer,integer,integer)

_import_review_action_catalog_core_v1
  (uuid,integer,integer)

_import_review_apply_envelope_core_v1
  (uuid)

import_review_correction_generation_transition_v1
  (uuid,uuid,text,text,uuid,text[],timestamptz)

timesheet_paid_uninvoiced_rollover_v1
  (uuid,uuid,uuid,uuid,text,timestamptz)

hr_weekly_apply_transactional
  (uuid,jsonb,uuid)

nhsp_weekly_apply_transactional
  (uuid,jsonb,uuid)

hr_weekly_phase3_apply_adjustment_truth
  (uuid,text[],uuid)

nhsp_weekly_phase3_apply_adjustment_truth
  (uuid,text[],uuid)
```

The four-argument HealthRoster phase-3 overload remains one unchanged definition.

## 14.5 Rollback

Capture the nine pre-repair definitions.

Rollback restores those nine bodies in reverse dependency order in one transaction.

The existing reconciliation objects, audit index, frontend, Worker, Bulk Authorise, invoice compatibility function, and TSFIN functions remain installed.

---

# 15. Hard deployment blockers

Deployment stops if any of the following fail:

```text
archived rows excluded from B and M
archived rows absent from every lifecycle target
archived/live sibling repair
deleted reversal repair
deleted replacement repair
both-missing repair
changed reversal normalisation
changed replacement normalisation
atomic rollback
true partial classification
deleted-invoiced-role full classification
operation evidence cross-check
credit signed-hours correction
non-hours exclusion
policy-envelope validation
ordinary paid-source rollover
cross-operation shell reuse
post-TSFIN B/A validation
NHSP suite
HealthRoster suite
definition-count gate
active-operation deployment gate
```

Deployment also stops if any implementation diff introduces:

```text
Banking Pay
payment functions
invoice functions
TSFIN calculation functions
frontend
Worker
Bulk Authorise
general lifecycle
HealthRoster Daily
validation-only processing
```

---

# 16. Final no-change boundary

The following remain unchanged:

```text
_import_apply_operation_claim_core_v2
_import_review_apply_complete_core_v1
import_review_apply_guard_v1
invoice_correction_pair_scope_v1

import_timesheet_financial_preflight_v1
enqueue_ts_financials_priority
all TSFIN calculators and Workers

timesheet_authorise_bulk_atomic
timesheet_unauthorise_bulk_atomic
timesheet_authorise_generic_atomic
timesheet_unauthorise_atomic
timesheet_correction_chain_scope_v1

broker/src/import-review-follow-up.js
all frontend files
all Bulk Authorise functions

all invoice functions
all Banking Pay and payment functions
```

---

# 17. Final implementation-readiness conclusion

The two omitted requirements are now resolved:

1. **Archived rows are fully ignored.** They contribute nothing to `B` or `M`, never select a route, never enter lifecycle or phase 3, and never cause an archived/live business blocker. Where an archived role occupies the old unique correction key, the surviving active liability is repaired under one fresh deterministic correction ID while the archived row remains untouched.

2. **Deleted and changed mutable members are repaired.** The two source-specific phase-3 functions now form part of the repair surface. They recreate missing roles, normalise both reversal and replacement schedules, preserve or deterministically replace the correction ID as required, create the necessary contract weeks, and leave exactly one complete active pair.

A missing or modified live role is never automatically classified as partially invoiced:

```text
both roles effectively invoiced
→ fully invoiced

neither role effectively invoiced
→ mutable repair

exactly one role effectively invoiced
→ genuine partial blocker

role evidence unprovable
→ source-unprovable blocker
```

The final production repair is **nine SQL functions only**.

No implementation or database change has been performed.

