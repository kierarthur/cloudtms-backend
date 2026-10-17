# Kept-as-paid and identical-rekey current-source successor

This is the finite HealthRoster implementation contribution against I2 commit `5ecc4c73113660edf48a5bca2f3a478453a81c2b`. The files under `payload/` are complete proposed managed/runtime sources. They have not been mounted in I2, installed or executed against PostgreSQL. The feature branch owns this contribution; root owns integration, independent review, the generated contract, native proof and release.

The owner-approved recurring-facts behavior is implemented: a new reviewed A→B→A request reuses the immutable A decision identity and appends a new typed native audit acknowledgement. An exact request replay returns its original acknowledgement. Actor, timestamp, sequence, original request/hash and result remain on that actual acknowledgement. Original decision/audit rows are never updated, deleted or relabelled. There is one original-decision relation and no second acknowledgement store.

The new acknowledgement counter is a canonical positive decimal **string** at both immutable audit/result JSON boundaries, the latest-ACK read projection and the Worker decoder. Its bound is PostgreSQL bigint `9223372036854775807`; no JavaScript Number conversion occurs. The native guard validates string/canonical/range before casting. Lowercase canonical UUID requests/results admit the complete hexadecimal UUID domain without version/variant-bit restrictions.

The read context selects the latest genuine event acknowledgement **before** current reason/fingerprint comparison, so an older A acknowledgement cannot reappear as the latest after B. The actual service writer validates active payment authority and current event/group/client/date `VIEW_PAY_WORK` scope before both historical replay branches. New decisions retain full current Office approval, candidate/root locking, request/event serialization, scope/facts CAS and immutable native audit admission. It performs no pay/invoice write.

The identical-rekey helper and its two complete current callers retain the exact old-key-absent, same full tuple and unique prior-identity requirements. This is the bounded identical case; it is not the general paid-overlap Same/Separate implementation.

The actual invoice edit owner gains its early complete target admission before its existing first invoice-mutability lock. Genuine pointed-credit additions and removals use the same candidate/root→group/cycle→invoice order and exact same transaction/parent/full-payload frame. The actual adjustment owner rechecks the complete facts census before its own first DML. The complete current V6 `16102026_0955` invoice owner, including `invoice_contract_move_selected_financial_authorise_v1`, remains intact outside the bounded early hook. Both private `13102026_1705` helper successors remain `SECURITY DEFINER` with their existing behavior outside the explicit target metadata changes.

## Exact integration inputs

`PAYLOAD_MANIFEST.json` lists the 20 target files with SHA-256 and byte counts. The three names below were created with the repository's normal `db:new` scaffolder at the actual UK date/time, then moved directly into this review payload. No old migration was changed and no installed ledger/hash was edited.

| Stage | Proposed target |
|---|---|
| One-time original decision schema and indexes | `supabase/migrations/17102026_0113_weekly_source_kept_10_decision_schema.sql` |
| One-time native audit guards | `supabase/migrations/17102026_0113_weekly_source_kept_20_audit_guards.sql` |
| Complete recursive function/ACL successor | `supabase/repeatable/17102026_0113_weekly_source_kept_pay_rekey_v1.sql` plus its 14 included pages |
| Strict command module | `broker/src/weekly-source/kept-pay-command.mjs` |
| Current complete command parent | `broker/src/weekly-source/routes.js` |
| Complete current final-owner registry successor | `docs/bpay-next/final-definer-closures.json` |

Apply the existing-owner and parent edits to the saved current integration source using their unique exact anchors, or accept the complete successor only after its pinned input matches. Never transplant this complete parent over newer saved changes.

- `CURRENT_OWNER_EDITS.json`: five complete current SQL owners, exact original and review-correction hunks, input/output hashes and complete byte reversals. The final review corrections reverse first, then the original contribution reverses to each entire current owner.
- `PARENT_EDITS.json`: five unique route hunks, input/output hashes and complete byte reversal. The complete current Source pay HTTP route/import/contract composition is retained.
- `CLASSIFIER_EDITS.json`: six exact hunks and full reverse of the complete current literal classifier/ACL source. It adds one immutable relation, six private helpers and one service RPC, increasing its existing exact relation count from 105 to 106. All other classifications/ACL logic are retained.
- `REGISTRY_EDITS.json`: 18 exact old/new entries and full serialized-byte reversal. Every unrelated registry/editor entry is unchanged. It retains predecessor physical/root/closure identities and registers the new complete final definitions. The two formerly unique private1705 helpers now have explicit predecessor/final chains.

The recursive repeatable includes every complete function page, then the complete current classifier/ACL owner, then the contribution's exact owner/private/service ACL closure and PostgREST reload. Its changed-only closure does not depend on an unchanged final ACL file being selected later. The original-decision migration bootstraps only the exact current immutable trigger owner required before repeatables on NEW; an unexpected existing body/security/owner fails closed. The audit migration installs its complete guard before its four native triggers. Canonical migration execution remains schema10→audit20, followed by the complete repeatable.

Root must review the actual mounted graph/source gates, append only the new migration hashes, reconcile the existing mandatory verifier inventory, generate the actual PostgreSQL17 contract and commit those installed-source inputs together before any protected APPLY. This payload does not invent canonical catalogue hashes or claim a generated contract. The existing-source migration lock is deliberately untouched here.

## Verification actually run

- 58 actual complete command-parent/strict-decoder tests with mocked RPC and source regression checks passed. They cover current WP-PP actors, denied/lost authority, forbidden browser authority fields, duplicate JSON keys, finite receipts/errors, single unknown-outcome attempt, bounded streaming, split/fatal UTF-8, latest-ACK selection, both fresh replay-admission sites, lossless counters above2^53 and the PostgreSQL maximum, malformed/numeric/zero/leading-zero/overflow counter refusals, lowercanonical full-domain UUIDs and literal SQL replacement without JavaScript dollar-pattern expansion.
- 12 finite payload/complete reversal/classifier/registry/recursive-order checks passed. All five current SQL owner reversals and current Source HTTP parent composition are retained.
- PostgreSQL17.7 offline grammar passed for 32 source/payload files, 217 statements and 40 PL/pgSQL bodies. This does not resolve catalogues, execute first use or prove financial behavior.
- `git diff --check` passed. The feature branch's pre-existing `npm run db:check` failure remains `Unrecorded migration: supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql`; no unrelated lock repair was attempted.

The command module was reviewed against the current [Cloudflare Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/): requests are bounded while streaming, async work is awaited and there is no global request state or new binding/configuration.

Run `prepare-current-census.mjs`, `build-runtime.mjs`, `build-package.mjs`, `test.mjs`, `test-package.mjs`, then `parse-source.py` for deterministic source refresh. The saved `MOUNT_FILENAMES.json` keeps these exact fresh filenames stable. The parser uses the migrated D: dependency path. Native compilation, catalogue/ACL/RLS/API first use, concurrency, financial/invoice, Office/Checklist presentation, browser and phone acceptance, contract seal, managed mount and installation are **NOT_RUN**.

## Genuine remaining source dependencies

These are outstanding source implementation/integration obligations, not native tests hiding otherwise completed source:

- **Generic Source pending/current disposition:** this package supplies a Keep acknowledgement and exact rekey admission. It does not complete generic pending/omitted-work disposition or its preparation/Office/Checklist/current readers.
- **`exclude_cancel`:** retain genuine latest-export absence separately from its genuine historical payable/invoice basis, on the existing transition/capture/selector protocol. The current Source context's planned `kept_by_default`/hold presentation is not proof that the native default-retention writer exists. Do not activate that journey from this packet. Never fake `NO_CHANGE`/present membership, suppress the required ordinary reversal, add a parallel money ledger or redefine a Banking-owned selector/capture under a Health filename.
- **Later Accept source:** construct a genuine complete removal proposal through the existing proposal/native later-change approval/publication owners and their full basis/currentness/digest checks. Do not manufacture protection, a proposal bundle or a paid label. This packet does not implement that producer/consumer closure.
- **Paid-overlap Same/Separate and pay effect:** the general differing tuple, old-key-still-present, multiple-match and duplicate raw-row/snapshot cases still require actual native choice, projection, preparation/finalisation and read/Office consumers. The bounded unique identical-rekey helper is insufficient for them.
- **Non-admin NEXT paid evidence:** this Health context deliberately reports `UNKNOWN` where no published purpose-bound native paid-evidence seam is available. A genuine payment authoriser/golden actor must not be substituted with an admin or served a duplicated Source money classifier. The paid-not-invoiced non-admin path remains incomplete until the exact authorised native producer seam is delivered and consumed.

The retained bounded design references are the prior packet's `DEFAULT_EXCLUSION_AND_ACCEPT_SOURCE_RECONCILIATION.md` and `SOURCE_NEXT_PAID_PERMISSION_SEAM.md`; their dated inputs are historical design evidence, not current Bank-body qualification. No Banking worktree/body was edited or Banking chat messaged. Owner authority is SEND_5 D9 plus the matching V6 final release and the explicit recurring-facts decision; no new business approval is requested.

For root's native acceptance, the material cases are: A→B→A creates two immutable decisions/three ordered actual ACKs; replay A's old key returns its old ACK without claiming current applicability; reason/hash change cannot resurrect an older ACK; scope/payment authority loss denies replay; same/new-key concurrent writers serialize once and reject a collision; direct forged audit INSERT and UPDATE/DELETE/TRUNCATE refuse; pointed-credit addition/removal in both concurrency orders retains native invoice locks/guards and rolls back stale facts; all allowed payment actors and denied foreign-scope actors traverse the real API. These cases are **NOT_RUN**, and require the genuine retained-basis producer where omission eligibility needs it.

No database/lab/Docker, TEST setup data, Worker/release/deploy, email/provider/payment/bank or secret operation occurred. No native result or financial completion is inferred from these source checks.
