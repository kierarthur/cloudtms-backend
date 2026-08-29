# Locked implementation plan and function map

## Objective

Implement and verify the narrow Import Review rule for NHSP Weekly and HealthRoster Weekly import-authoritative changed hours:

```text
B = complete signed effective frozen Weekly-hours invoice position
M = current active mutable uninvoiced correction position
A = latest authoritative imported schedule and hours
```

The outstanding timesheet position after Apply must make the complete chain equal `A`. Historical invoice artifacts remain immutable.

## Required decision model

1. If there is no effective invoice history and the current ordinary source is safely mutable, amend that source through the existing route.
2. If the ordinary source is genuinely paid and uninvoiced, use the existing paid-uninvoiced rollover.
3. If a complete uninvoiced correction generation exists, amend/repair it in place.
4. If the effective frozen position is invoiced and no mutable generation exists, create one new reversal/replacement generation:
   - the reversal is the negative of the complete effective terminal position;
   - the replacement is the authoritative `A` schedule;
   - both members share one new correction ID and one parent;
   - the parent/terminal authority comes from the terminal effective positive member, not from an arbitrary surviving row.
5. If a historical member has been archived, treat it as audit-only. If an active sibling remains, repair under a fresh correction ID without mutating the archived row.
6. If a historical member has been physically deleted, reconstruct it only from validated completed operation, audit, TSFIN, and exact frozen invoice evidence.
7. Accumulate every valid successive completed generation. Deduplicate money only by physical invoice-line identity.
8. Use one signed Weekly-hours ledger for `B`, role invoice state, credits, partial-generation classification, and settled-zero history.
9. Ignore separable non-hours components.
10. Fail closed on ambiguous ownership, malformed credit provenance, a genuine one-sided economic invoice, or a zero-hours/non-zero-money position.
11. HealthRoster Weekly validation-only and HealthRoster Daily must remain non-mutating.

## Function-by-function implementation

### `public._import_review_effective_invoice_balance_core_v1`

- Discover source history across current and earlier imports through validated source/action/outcome/operation evidence.
- Validate completed request/applied/policy triples and their fingerprints.
- Canonicalise archived-member re-key supersession.
- Add applied member IDs before invoice-line scoping.
- Build one source-scoped signed `HOURS_WEEKLY` ledger.
- Allocate aggregate credits from exact frozen source segments.
- Derive terminal effective-positive schedule and policy authority.
- Return complete `B`, `M`, `A`, blocker, representability, repair identity, evidence and fingerprints.
- Treat fully credited current-source safety explicitly.

### `public._import_review_action_catalog_core_v1`

- Consume the corrected helper output.
- Preserve route precedence: blocker, mutable correction, invoiced correction, paid-uninvoiced source, ordinary source.
- Compare complete B/M/A buckets, not only a scalar total.

### `public._import_review_apply_envelope_core_v1`

- Freeze the reviewed B/M/A evidence, terminal authority, repair identity and fingerprints.
- Do not pre-allocate future generated member IDs.

### `public.import_review_correction_generation_transition_v1`

- Re-attest immutable B evidence and lifecycle state.
- Exclude the transition's own current operation from in-progress detection.
- Validate and finalise exact applied member identities after TSFIN.

### `public.timesheet_paid_uninvoiced_rollover_v1`

- Preserve the paid historical TSFIN.
- Create/reuse one current recalculation shell for a genuinely paid-and-uninvoiced source.
- Do not use this route for invoice-lined or fully credited historical rows.

### `public.hr_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

- Apply the operation-bound zero/one/two-member correction repair.
- Preserve archived rows and use a fresh correction identity when an archived unique key is occupied.
- Reconstruct the required financial carrier from the validated reconciliation unit when the protected historical root cannot be a mutable target.

### `public.nhsp_weekly_phase3_apply_adjustment_truth(uuid,text[],uuid)`

- Apply the same source-neutral correction contract while retaining NHSP-specific source evidence.

### `public.hr_weekly_apply_transactional`

- Apply only HealthRoster Weekly import-authoritative reviewed actions.
- Preserve validation-only exclusion.
- Exclude protected historical roots from TSFIN follow-up targets.
- Persist applied-result evidence and reviewed authorisation intent.

### `public.nhsp_weekly_apply_transactional`

- Apply the equivalent NHSP routes and exclusions.

### Supporting functions

- `_wkimp_bucket_hours_from_policy`: return every calculated bucket correctly.
- `correction_financials_policy_resolve_v1`: resolve consistent frozen line evidence when current header VAT evidence is absent.
- `_invoice_generation_advance_batch` and `_invoice_issue_advance_batch`: allow direct root operations used by the established invoice route while retaining manifest-member gating.

## Hard no-change boundary

Do not broaden this work into:

- Banking Pay or `pay_*` functions;
- payment execution, provider processing, settlement, CSV, remittances, or Policy X;
- invoice calculation, invoice totals, VAT policy, or credit-note writing;
- TSFIN rates, rounding, or Worker architecture;
- frontend or shared modal changes;
- Bulk Authorise;
- HealthRoster Daily;
- HealthRoster Weekly validation-only mutation;
- a new financial pathway.

## Acceptance requirements

- Real TEST reversal/replacement and repeated-generation proof for both authoritative Weekly sources.
- Uninvoiced in-place amendment proof.
- Validation-only +1/-1 non-mutation proof for HealthRoster Weekly and Daily.
- Executable database fixtures for deletion, archive/re-key, credit allocation, terminal authority, invalid evidence, rollback and repeated arithmetic.
- Focused tests green.
- Installed TEST definitions equal committed canonical definitions.
- No unrelated file in the commit.
