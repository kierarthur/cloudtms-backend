# Locked implementation and verification brief

## Plan name

NHSP/HealthRoster Weekly Authoritative Amendments — Terminal Schedule Authority Completion.

## Business rule

This is an Import Review reconciliation rule only. It applies equally to:

- NHSP Weekly import-authoritative processing.
- HealthRoster Weekly contracts classified as import-authoritative.

It does not apply to HealthRoster Weekly validation-only processing or HealthRoster Daily.

For an authoritative changed-hours import, the complete effective history must reconcile to the newly imported schedule. Historical invoice components remain immutable. When an invoiced correction chain exists, every effective physical Weekly-hours component contributes once to `B`; the next reversal must use the schedule of the exact terminal effective positive member, not an older surviving positive row and not an arbitrary schedule whose hour buckets happen to match.

For example:

```text
T0              +10
Generation 1    -10 +11
Generation 2    -11 +12
Generation 3    -12 +10
```

The cumulative position is `10`, but the authoritative reversal schedule is the exact positive member from Generation 3. A schedule belonging to `T0` or Generation 1 must never be substituted merely because it also totals 10 hours.

## Locked production scope

Exactly one production function may change:

```text
public._import_review_effective_invoice_balance_core_v1(
  uuid,
  jsonb,
  integer,
  integer,
  integer,
  integer
)
```

Canonical source:

```text
supabase/repeatable/21072026_1820_00_import_review_internal_core.sql
```

The function signature, bounds, invoker security, search path and direct grants must remain unchanged.

## Required implementation

1. Continue calculating `B` from the canonical, signed, source-scoped Weekly-hours ledger across every valid completed correction generation.
2. Determine the terminal fully invoiced generation deterministically from the already validated generation evidence.
3. Prove exactly one terminal positive replacement member for that generation.
4. Build frozen-schedule candidates with physical provenance: timesheet ID, correction ID/kind, invoice-line ID, exact normalized schedule, exact hour buckets and canonical policy fingerprint.
5. Consider a frozen candidate only when it belongs to the exact terminal positive member. Older positive candidates are not eligible even if their buckets equal `B`.
6. Prefer the exact terminal positive frozen segment when it survives and its canonical policy envelope re-attests.
7. If that timesheet/TSFIN is physically absent, use only the exact validated completed operation for the same correction ID and replacement member, taking its reviewed `A_schedule_json`, `A_hours`, operation ID and policy-envelope fingerprint.
8. When both frozen and completed-operation evidence survive, require their material schedule fields and canonical policy fingerprints to agree. Material fields are date/work date, start, end, break, shift ID and external row key.
9. Fail closed when the terminal member is unprovable, schedule authorities conflict, policy authorities conflict or the selected authority lacks a canonical policy fingerprint.
10. Preserve zero-position behaviour: a genuinely zero `B` has authority `NONE`, an empty standard schedule and no policy fingerprint.
11. Preserve every previous correction to cross-import operation discovery, archived re-key lineage, physical-member reconstruction, signed credits, multi-source segment allocation, zero-hours/non-zero-money blocking and effective-zero source safety.

## Required authority output contract

The helper must emit:

```text
B_standard_schedule_authority
B_standard_schedule_authority_timesheet_id
B_standard_schedule_authority_correction_id
B_standard_schedule_authority_operation_id
B_standard_schedule_authority_policy_fingerprint
B_standard_schedule_authority_fingerprint
```

Allowed authority values are:

```text
ORIGINAL_SOURCE_FROZEN_SEGMENT
TERMINAL_REPLACEMENT_FROZEN_SEGMENT
TERMINAL_COMPLETED_OPERATION_A_SCHEDULE
NONE
```

It may also emit an internal diagnostic explaining a safe failure. The authority contract must participate in the role-evidence, effective-invoice and reconciliation fingerprints so a Review/Apply evidence change becomes stale instead of silently changing the reversal basis.

## Required failure behaviour

Conflicting or missing terminal evidence must set the existing safe blocker:

```text
IMPORT_REVIEW_INVOICE_COMPONENT_SCOPE_UNPROVABLE
```

An otherwise positive but non-representable effective position continues to use:

```text
IMPORT_REVIEW_EFFECTIVE_POSITION_NOT_STANDARD_REPRESENTABLE
```

No new frontend reason code is required.

## Mandatory verification

Run independently for NHSP and HealthRoster authoritative Weekly:

- Repeated `+10 → -10/+11 → -11/+12 → -12/+10` economics.
- Older positive schedule with the same buckets as terminal `B`; require the exact terminal member authority.
- Terminal positive row and TSFIN absent; require the exact completed-operation schedule and policy authority.
- Terminal frozen segment and completed operation agree; require frozen-segment preference.
- Material terminal schedule conflict; require safe blocking.
- Exact authority timesheet, correction, operation, policy and authority fingerprints.
- Existing cross-import deleted-member, archived supersession, multi-source credit, zero-money, invalid-operation and replay/source-contract regressions.
- Real apply-envelope producer contract plus exact source-contract re-attestation of both installed NHSP and HealthRoster phase-3 applied-result producers.

Disposable database fixtures may directly seed frozen invoice and TSFIN evidence. They must roll back and must not run against CloudTMS TEST or production.

## Hard no-change boundary

Do not amend:

- `_import_review_action_catalog_core_v1`
- `_import_review_apply_envelope_core_v1`
- `import_review_correction_generation_transition_v1`
- `timesheet_paid_uninvoiced_rollover_v1`
- either Weekly phase-3 function
- either Weekly transactional caller
- invoice or credit-note writers
- TSFIN calculation, rates, rounding or Workers
- frontend, Bulk Authorise, HealthRoster Daily or validation-only processing
- Banking Pay, `pay_*`, payment execution, CSV/provider processing, settlement, remittances or Policy X

## Reviewer instruction

Review the committed and deployed code against this brief. Give a function-by-function verdict for the nine named Import Review functions. If anything remains, provide a highly detailed implementation plan only for functions with a demonstrated defect required to fulfil this brief. Do not use the handover as authority to redesign or open Banking Pay, invoicing, TSFIN, frontend, Daily, validation-only or unrelated Import Review work.
