# Saved resolution writer/consumer reconciliation — evidence and design

Status: `PROVED PRE-DRAFT CONTRACT DEFECT / LOCAL CORRECTION DESIGN ONLY`

Runtime authority is Miget TEST. The repository directory named `supabase` is a historical source-path name only.

## Observed failure

The current `public.pay_finance_component_resolutions_apply` writer successfully saves the user-approved target pay method, resolution mode and unchanged finance-owner amounts. On the next canonical preview, both current readers classify that newly saved evidence as `STALE_SAVED_RESOLUTION`, select zero finance constituents and produce zero Draft writes.

Rollback-contained first use reproduced this independently on PostgreSQL 17.11 and PostgreSQL 18.6 for:

- a saved Umbrella-to-PAYE replacement-rate resolution: expected target ex-VAT `88.00`, VAT `0.00`, total `88.00`;
- a saved PAYE-to-Umbrella equivalent-basis resolution: expected target ex-VAT `91.04`, VAT `18.21`, total `109.25`.

Those saved target figures are resolution evidence, not permission to recover more than the unchanged recovery/headroom owner allows in the current pay run. With a source outstanding amount of `80.00` and sufficient headroom, the current recovery owner deliberately freezes `80.00` ex-VAT: PAYE remains `80.00` total, while VAT-chargeable Umbrella becomes `80.00` ex-VAT, `16.00` VAT and `96.00` inclusive. The reconciled Draft fixture asserts both layers independently so a transport fix cannot silently turn a saved conversion into a policy change.

The target-pay-method mismatch negative fails before mutation. Both positive saves return the exact stored fingerprint, then the unchanged canonical consumer recomputes a different fingerprint. No batch, Draft item, allocation or reservation is written.

## First divergent boundary

`SAVED_RESOLUTION_WRITER_TO_CANONICAL_PREVIEW_CONSUMER`

The writer and consumer use the same existing `public.pay_finance_component_fingerprint` owner, source identity, target method, source basis, source amount and ERNI input. They disagree only on the final `target_basis_json` argument:

- writer: a private transient `v_target_basis_json` object that is not persisted;
- current preview and clear-owner consumers: the persisted `saved_resolution_payload_json` (falling back to the saved result).

The current strict owner also requires both saved JSON objects to declare `resolution_family = TAXABLE_CHANNEL_RESTRUCTURE`; the writer emits neither declaration. Therefore a fingerprint-only correction is insufficient.

## Source and ownership evidence

- Historical writer owner, which must remain byte-identical: `supabase/repeatable/26052026_2100HRS_NEW_FUNCTIONS.sql`, SHA-256 `8b3cb3e112ae227a80bf2e661272264c3d6145d0e2ff6ad8d88e8eee2db1553f`.
- Current preview and cancellation owner: `supabase/repeatable/17082026_2052_pay_finance_resolution_cancel_authority.sql`, SHA-256 `5579e9cd33c6a4d413f467451c75172cfcec3bfb00258f9e31d1b31021cc4d5a`.
- Writer was introduced by commit `7396beebb`; the later strict consumer authority was installed by commit `b9ce91fe`.
- Installed metadata to preserve: `SECURITY DEFINER`, volatile, parallel unsafe, `search_path=public`, owner `postgres` after logical-owner normalization, EXECUTE only for owner and `service_role`.

## Smallest policy-neutral correction

Add one later single-function replacement for `public.pay_finance_component_resolutions_apply(uuid,jsonb,uuid,uuid,text)`. Keep every input validation, row lock, rate/amount conversion call, money calculation, event, result field, signature, default, metadata and ACL unchanged. Change only the saved evidence envelope:

1. Add `resolution_family: TAXABLE_CHANNEL_RESTRUCTURE` to both saved payload and saved result.
2. Calculate `resolution_fingerprint` from the exact persisted saved payload that every current reader already uses, rather than from the non-persisted private basis object.

This aligns the writer with the existing later reader contract. It does not choose a pay method, calculate a new amount, change gross/net treatment, change VAT, alter selection policy or touch a frozen Draft.

Changing both current consumer functions instead is rejected as a larger correction: it would duplicate reconstruction of a private writer-only object in preview and cancellation paths, create a second evidence derivation, and still leave the missing family identity unresolved.

## Required green proof before release

- Preserve the current dual-engine red evidence unchanged.
- Apply only the additive replacement in disposable PostgreSQL 17.11 and 18.6.
- Re-run both saved-resolution cases through the canonical producer, preview, certificate, Draft allocation/item/finance/reservation/finalizer chain.
- Prove exact saved amounts and every PAYE/Umbrella field are unchanged from the writer's current result.
- Prove the saved decision is `REUSABLE_SAVED_RESOLUTION`, selection reaches the Draft once, and clear/cancel ownership remains exact.
- Prove old rows lacking the strict family remain stale/fail closed and require an explicit user re-save; do not silently upgrade historical evidence.
- Prove target mismatch, tampered fingerprint, wrong family, changed source basis, changed target pay method and malformed payload fail closed with zero Draft writes.
- Prove replay and response-loss produce no duplicate event, item, allocation or reservation.
- Prove metadata, ACL, source order, NEW, UPGRADE and idempotent reapply on both engines.
- Compare all durable Draft and downstream Execute Payment projections against the frozen policy contract.
- Keep all existing statement and lock budgets unchanged.

No Miget write, Worker deployment, real Draft, payment, provider, settlement or remittance action is authorised by this document.
