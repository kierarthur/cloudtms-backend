# Weekly Source C1 upstream compatibility specification

Date: 16 September 2026  
Status: CloudTMS producer-side review specification; not a Banking Pay implementation instruction  
Audience: HANDOVER 2, final reviewers and the Weekly Source release gate  
Boundary: Weekly Source may publish complete approved Weekly entitlement. Banking Pay/Workbench remains separately owned.

## 1. Purpose

This document tells HANDOVER 2 exactly what the Weekly Source implementation now produces, what it deliberately does not produce, and what must be checked for compatibility with the approved C1 Banking Pay/Workbench design.

HANDOVER 2 is asked to review this contract only. This document does not ask HANDOVER 2 to implement, merge, deploy or mutate anything.

The governing design remains the sealed C1 design incorporated by Plan 6. The ordinary Weekly Timesheet is the sole public Timesheet and the immutable economic root. Weekly Source does not create an alternative payment, recovery, Case, Draft, provider, settlement or remittance route.

## 2. Plain-English outcome

Weekly Source always publishes the candidate's complete currently approved entitlement for one Candidate + Contract + Weekly scope. It never publishes a difference.

Examples:

- Source entitlement is £100 and Office protects £120: Weekly Source publishes £120.
- £120 has already settled and Office later reconciles to a final source entitlement of £100: Weekly Source publishes £100, not `-£20`.
- Office protects ten hours and the later final source proves eleven hours: Weekly Source publishes the newly calculated eleven-hour complete entitlement, not a one-hour payment instruction.
- Office records that the shift was not worked and the complete Weekly target is zero: Weekly Source publishes a certified complete zero entitlement. It does not create a zero-value payment item.
- Office chooses Wait: the current protected target remains the complete entitlement and no new economic publication is made merely because source changed.

Only the existing Workbench may compare that complete entitlement with settled, recovered, reserved or frozen history and decide whether ordinary pay, existing overpayment recovery or no row remains.

## 3. Exact CloudTMS ownership

CloudTMS owns:

1. Candidate evidence, current final source facts and Office protected-hours decisions.
2. The source/protection schedule composition for the complete Candidate + Contract + week.
3. Reuse of the established server-side Weekly calculator to derive worked-time buckets, Additional Rates, applicable expenses, pay method, VAT evidence and the complete TSFIN snapshot.
4. Construction of the closed C1 evidence stream.
5. Deterministic private publication/component identities.
6. Durable stage, checkpoint, unknown-outcome recording and explicit receipt-first recovery.
7. Publication of one complete certified generation through the approved C1 interface.
8. Source invoice manifests, which remain independent of protected pay.

CloudTMS does not own and must not perform:

1. Residual, underpayment or overpayment calculation.
2. Recovery headroom or Case/Resolution decisions.
3. Draft creation, reservation, freeze or repeat-Draft behavior.
4. PAYE net entry or Umbrella payment preparation.
5. Provider selection, execution, cancellation, settlement or remittance.
6. A second payment route or any Banking Pay user-interface change.
7. Any invoice decision based on protected-pay state.

## 4. Office actions and exact producer behavior

The only supported Office actions are:

| Office action | Internal action | Producer result |
|---|---|---|
| Approve protected hours | `APPROVE` | Creates or reuses the server-resolved Candidate + Contract + week family, composes the complete target and stages/publishes one C1 generation. |
| Change protected shift | `AMEND` | Uses the expected family version, replaces the relevant protected schedule in the complete target, recalculates and publishes one new complete generation. |
| Stop protected pay | `WITHDRAW` | Removes protection from the relevant event, recomposes the complete target from remaining source/protected decisions and publishes it. |
| Wait for source | `WAIT` | Records the source proposal and preserves the existing target-vector hash. It does not calculate or publish a new economic generation. |
| Accept source and reconcile | `RECONCILE` | Replaces the protected event with the accepted current source fact inside the complete target and publishes the newly calculated complete entitlement. |
| Record did not work | `RECORD_NOT_WORKED` | Removes that event from the complete target. If the family becomes zero, publishes certified zero. |

The browser supplies identifiers, an expected family version, schedule fields for approve/amend, a reason and an idempotency key. Browser-supplied money, C1 records, entitlement digests, rate values, payment differences, provider decisions or invoice decisions are rejected.

## 5. Schedule and entitlement construction

### 5.1 Schedule rules

The producer uses `composeWeeklyProtectedTargetSchedule` with:

- current final source segments; and
- immutable protected decisions.

The rules are:

1. Ordinary events use current final source truth.
2. An active waiting protected event uses the fixed Office-approved schedule.
3. Source and protected hours are never added together for the same work event.
4. A protected event may exist without a source row or candidate Timesheet.
5. Reconcile follows the accepted source fact, including a changed or absent source position.
6. Record did not work removes only the selected event.
7. Duplicate event identities, invalid schedules and unknown decisions fail closed.

### 5.2 Server calculator

The producer calls the established Weekly calculator owner, identified in evidence as `buildWeeklyScheduleSegmentsSnapshot`. The producer copies that server-built result; it does not calculate pay in the browser or in the C1 adapter.

The calculator result must belong to the immutable ordinary root Timesheet. A root mismatch is refused.

The complete financial snapshot supplies:

- `TS_DAY` worked-time components by work date;
- day, night, Saturday, Sunday and bank-holiday hours;
- existing pay and charge facts from the established calculation;
- Additional Rates as `ADDITIONAL_CODE` components;
- expenses under their correct ordinary or source-expense authority; and
- permitted non-advance adjustments only.

An advance marker is never created, changed or consumed by this route.

### 5.3 Zero family

If the family has no previous ordinary financial root, the service prepares an explicit zero ordinary TSFIN snapshot before publication. It has:

- the ordinary immutable root Timesheet identity;
- zero hours, zero Additional Rates and zero expense totals;
- a complete `SEGMENTS` breakdown with no segments; and
- the existing pay method, Contract/rate-policy authority and financial provenance.

No fake zero component or selectable zero payment item is created.

## 6. C1 request identity and start envelope

One request is bound to:

- `request_id`
- `request_sequence`
- `actor_user_id`
- `candidate_id`
- `contract_id`
- `root_timesheet_id`
- `week_ending_date`
- `source_mode`
- `expected_head_revision`
- `expected_source_count`
- `expected_component_count`
- `expected_payload_bytes`
- `source_manifest_sha256`
- `entitlement_sha256`
- `approval_sha256`
- `is_zero_entitlement`
- `financial_row_id`

The outer request digest is generated only after the exact source and component streams and Office approval digest exist.

The only source modes are:

- `NHSP_WEEKLY`; and
- `HEALTHROSTER_WEEKLY`.

Configurable roster sources, including source-fixed-expense/whole-shift-rate clients, use generic `HEALTHROSTER_WEEKLY` for this boundary. Banking Pay receives no Magnit-specific classification.

## 7. Closed source evidence stream

Every source record contains:

- contiguous `source_ordinal`;
- deterministic private `source_id`;
- `authority_kind`;
- `source_system`;
- external identity and revision;
- original evidence SHA-256;
- exact payload byte and part counts;
- optional work date;
- immutable ordinary root Timesheet, Candidate and Contract identities;
- row digest; and
- record type.

Permitted authority kinds are closed to:

- `CLIENT_SOURCE`
- `CANDIDATE_SUBMISSION`
- `ROOT_FINANCIAL`
- `PROVIDER`
- `APPROVED_COMPONENT`
- `SOURCE_EXPENSE`
- `ORDINARY_EXPENSE`
- `NONADVANCE_ADJUSTMENT`
- `OFFICE_APPROVAL`

At least one complete client-source observation is mandatory. A source observation explicitly records both completeness and presence, so an absent source position is evidence rather than a missing record.

Candidate submission evidence is optional. If present it retains submitting Candidate, immutable submission identity, submission time and submitted minutes. It is never rewritten as the processed source entitlement.

Provider authority records source and target pay method, Umbrella identity where relevant, current-enabled/VAT authority and the provider-authority digest. Missing, disabled, stale or ambiguous authority fails before publication.

The Office approval record binds the actor, request, root, Candidate, Contract, week and exact complete entitlement digest to decision `APPROVE_ENTITLEMENT`.

## 8. Closed component stream

Every component contains the exact closed schema enforced by the adapter, including:

- contiguous component ordinal and deterministic component UUID;
- source ordinal and canonical physical source key;
- component kind and economic key type/value;
- stable component member identity;
- segment identities where applicable;
- work date and optional source reference;
- day/night/Saturday/Sunday/bank-holiday hours;
- Additional Rate code, count and rates;
- expense code;
- adjustment identity;
- pay ex VAT and optional charge ex VAT;
- `exclude_from_pay`; and
- origin `WEEKLY_SOURCE_APPROVED_TARGET`.

Supported physical component families are:

1. Worked time: `WORKED_TIME` / `TS_DAY`.
2. Additional Rates: `ADDITIONAL_UNIT` / `ADDITIONAL_CODE`.
3. Expenses: `EXPENSE` / `EXPENSE_CODE`.
4. Non-advance adjustments: `ADJUSTMENT` / `ADJUSTMENT_CODE`.

Worked-time and Additional Rate components use `APPROVED_COMPONENT` evidence. Expenses retain either `SOURCE_EXPENSE` or `ORDINARY_EXPENSE` ownership. Generic and categorised expense ownership cannot overlap. Source expense totals must exactly own the corresponding TSFIN expense aggregate or publication is refused.

An established `exclude_from_pay=true` segment publishes zero candidate pay while retaining its separate charge fact. The C1 producer does not change that established policy.

Components are sorted by canonical source key. Duplicate keys, component authorities, expense authorities or adjustments fail before publication.

All money and rate tokens use canonical decimal strings. All counters are lossless integers/BigInts. Unsafe JavaScript Numbers, negative zero, extra fields and malformed tokens fail before the RPC.

## 9. Determinism, hashes and identities

The producer uses domain-separated C1-TLV-1 hashing for:

- source records;
- component records;
- source manifest;
- complete entitlement;
- Office intent;
- Office approval; and
- outer request.

The exact domains are:

- `C1/SOURCE/1`
- `C1/COMPONENT/1`
- `C1/OFFICE_INTENT/1`
- `C1/OFFICE_APPROVAL/1`
- `C1/REQUEST/1`

Null and empty are distinct. Original evidence bytes/digests cannot be substituted with a normalised document. Source and component ordering is deterministic.

Private request and component UUIDs are deterministic from the protected family, the Office idempotency key and the component key. A repeated identical Office operation therefore resolves to the same durable publication identity.

## 10. Publication lifecycle

The producer follows this order:

1. Prepare or resume the Office action under one shared idempotency namespace.
2. Read whether the exact C1 request has already been staged.
3. If staged, resume from its durable record and digest without recalculation.
4. Otherwise load server authority, compose the complete target, calculate the full Weekly snapshot and build the closed stream.
5. Persist the complete staged request before C1 calls.
6. Start, stage bounded pages, validate, certify and publish.
7. Record every returned cursor/checkpoint durably.
8. Mark the staged request complete only after C1 returns `PUBLISHED`.

Publication is complete-entitlement-only and all-or-nothing. A partial generation cannot become current.

Automatic retry is disabled at both raw transport and adapter layers.

If a call has an unknown outcome:

1. The exact request body, digest, operation/scope identity and checkpoint are recorded once.
2. Normal execution stops.
3. An unknown initial START has no operation identity with which to call status. Its deterministic `request_id` + request sequence + request digest are therefore replayed exactly once. That identical START replay is the authoritative idempotent receipt lookup: it either returns the already committed START receipt or creates the request once.
4. Every later call that already has an operation or scope identity asks the owning status route first.
5. If status proves the exact call committed, its receipt is consumed without replay.
6. Only when status/checkpoint proves that later exact call was not applied may the identical sealed call be replayed once.
7. Read-only status calls may be repeated; mutating calls may not be retried automatically.
8. A second unknown outcome, digest mismatch, cursor conflict or tampering refuses recovery.

Time alone never releases an unknown or protected state.

## 11. Invoice isolation

Candidate pay and self-bill invoicing are independent.

The Weekly Source implementation proves these producer-side rules:

1. Self-bill invoices are admitted only from immutable final-source movements.
2. Protected approval, amendment, waiting, reconciliation, did-not-work, C1 staging, publication status, positive residual, recovery or frozen Banking state cannot alter source movements.
3. A protected shift absent from final source creates no invoice line.
4. A final-source shift remains invoice-eligible even if candidate pay is protected or unresolved.
5. NHSP backing-report positives and full reversals are invoiced from the report manifest.
6. HealthRoster additions, corrections and cancellations use the final-source movement rules.
7. Source expense invoice and candidate-pay facets have separate immutable identities.
8. Moving one source shift between compatible idle unissued invoices changes placement only. It does not change the source movement, candidate entitlement or C1 generation.
9. Issuing validates complete movement allocation and renders frozen source invoice lines/report identities. It never reconstructs invoice value from protected pay.
10. Two selected finalised source weeks remain two source invoice outcomes per Client/cycle policy.

The final joined differential must compare invoice movement IDs, hours, pence, VAT, report identity, invoice grouping and rendered output with protection absent, waiting, published, reconciled and zero. They must be identical for the same final source.

## 12. Existing Workbench contract expected by CloudTMS

CloudTMS expects the separately owned C1 implementation to make the certified complete entitlement visible through the one approved `LIVE_ENTITLEMENT_INPUT` projection seam while preserving the ordinary root identity.

CloudTMS does not depend on a new public Banking Pay row or action. Expected downstream meanings remain:

- positive remaining entitlement: existing `TIMESHEET_PAYMENT` / `SEGMENT_DELTA` with `TS_DAY`;
- settled pay above current entitlement: existing `OVERPAYMENT_RECOVERY` under current headroom rules;
- equal current entitlement and paid/reserved history: no new row;
- pay-channel conflict: existing Case/Resolution route.

The existing dependency owner identified by P186 is `private.pay_workbench_timesheet_dependency_closure_v2(uuid,jsonb,integer)`. Weekly Source must satisfy its relationship contract; that owner and its caller must remain unchanged.

The only approved existing financial-owner edit is the narrow C1 predicate in `private.pay_workbench_unit_economic_occurrence_page_v1(...)`. Weekly Source has not changed that function in this worktree and cannot certify its downstream behavior until the separately owned implementation is available.

## 13. What HANDOVER 2 is asked to check

HANDOVER 2 is asked to confirm, without implementing from this request, whether:

1. Publishing complete entitlement rather than a residual is exactly the expected C1 input.
2. The ordinary root Timesheet, Candidate, Contract and week identities are sufficient and correctly retained.
3. The closed source/component schema and authority kinds match the expected C1 boundary.
4. `NHSP_WEEKLY` and generic `HEALTHROSTER_WEEKLY` are the expected source modes, including configurable roster/Magnit-style clients using the latter.
5. Certified zero, absent source and reappearing source are represented as expected.
6. The provider authority/fail-before-publication boundary matches the approved design.
7. Source-expense versus ordinary-expense ownership is compatible.
8. No advance marker or protected-pay-specific adjustment is expected.
9. The no-automatic-retry rule, deterministic exact START-replay exception, later status-first receipt recovery and exact one-replay limit match the expected C1 protocol.
10. The producer supplies every provenance fact that the separately owned projection seam requires.
11. There is any producer field, identity, relationship, digest or state that HANDOVER 2 expected but which this specification omits.
12. Any described producer behavior would require an unapproved Workbench, Draft, recovery, provider, cancellation, settlement, remittance or UI change.

An answer should identify an exact incompatibility if one exists. Silence or design familiarity is not treated as runtime approval.

## 14. Producer-side test evidence completed on 16 September 2026

### 14.1 Focused C1/protected-action boundary suite

Result: **62/62 passed; 0 failed; 0 skipped**.

It covers:

- exact RPC names and closed request/response schemas;
- lossless signed 64-bit integer handling;
- generic HealthRoster mode for configurable roster/Magnit-style sources;
- refusal of browser fields, unsafe numbers and malformed records;
- no hidden retry;
- deterministic exact recovery of an unknown initial START, later status-first committed-receipt recovery and the exact one-replay limit;
- deterministic source/component/request hashes;
- null-versus-empty and original-document binding;
- worked time, Additional Rates, ordinary expenses, source expenses and non-advance adjustments;
- exclusion-from-pay copying without charge erasure;
- certified zero with no invented component;
- durable stage/checkpoint/complete flow;
- approve, amend, withdraw, reconcile, did-not-work and wait behavior;
- source-absent first approval with explicit zero root;
- superseded Correct-final history exclusion;
- service-only SQL action surfaces; and
- absence of Banking Pay, Draft or invoice capabilities in the raw transport contract.

### 14.2 Protected schedule, finalisation and invoice-isolation suite

Result: **52/52 passed; 0 failed; 0 skipped**.

It covers:

- source truth for ordinary events and fixed Office truth for waiting events;
- protection without source or candidate Timesheet;
- changed/disappearing source reconciliation;
- did-not-work removal without sibling change;
- finalisation-first and one server-built ordinary projection per root;
- one refused root not blocking sibling roots;
- durable unknown outcomes and explicit recovery;
- final source with no affected roots;
- rejection of browser extra fields;
- movement-only invoice eligibility;
- zero-source protected-root isolation;
- frozen manifest economics;
- separate source-expense invoice/pay identities;
- compatible idle-unissued single-shift invoice movement;
- complete allocation at issue;
- final-manifest-only batching;
- two selected source weeks remaining two exact outcomes;
- source and ordinary invoice-route isolation; and
- a calculator preview seam that performs no write to Banking Pay, Draft, invoice or provider routes.

### 14.3 Wider service/parser suite

Result: **545/545 passed; 0 failed; 0 skipped**. This repeatable service harness includes the focused producer/C1/invoice checks above and uses the supplied real NHSP, HealthRoster and source-expense files.

The signed result envelope is:

`backend/.codex-tmp/weekly-source-step6-results-current-v6/service-parser-profiles.json`

Service phase digest:

`e6e6a0345fd1c1dd43eec4b94a100b61af7de4982ba99fa0b249486913cc161f`

The evidence gate currently credits this envelope with 5/64 requirements and 13/918 acceptance rows. No unrelated row is claimed by inference.

## 15. Current producer artefact hashes

These hashes identify the reviewed uncommitted worktree artefacts. They are evidence references, not a release commit:

| Artefact | SHA-256 |
|---|---|
| `broker/src/weekly-source/protected-action-orchestrator.mjs` | `a2e3ced554eda501ebef0808a80329e8697c0c8535bd09ccec0c4b11759b761e` |
| `broker/src/banking-pay/weekly-source-c1-adapter.mjs` | `3ba458c2f6de879e39df4be61fb17a30d5580e888d60a29fe9be2a97b44fbaaa` |
| `broker/src/banking-pay/weekly-source-c1-authoring.mjs` | `8c4b13f07d3fd8c85e5810a895c716d08ee395053d34fc8af6d77bcb8f02cfd0` |
| `broker/src/banking-pay/weekly-source-c1-components.mjs` | `192fb53c0831a3c3f1f573c2193d356942bf76b843ab9a77e3b66d8908ac871a` |
| `broker/src/banking-pay/weekly-source-c1-stream.mjs` | `a2f830773d5c0be5fc1f1f870025a8cc5b0d7882f8b23a75e6fd59bc89e9691d` |
| `broker/src/banking-pay/weekly-source-c1-publication.mjs` | `2a9a1f6d06eed3637b1de2f11de3023e87cd3d71ab686a847fc616a0d405be7e` |
| `broker/src/banking-pay/weekly-source-c1-durable-publication.mjs` | `cb39107459dd0e002234db92316ecb8a91a0b80f6874f1e2ee7275ab233d5e18` |

## 16. Tests that remain mandatory after HANDOVER 2 implementation exists

Producer-side passing tests are not a substitute for the downstream differential. The joined release gate must still prove:

1. £120 complete entitlement with £100 already settled creates exactly one ordinary £20 residual.
2. £120 settled then reconciled to £100 creates exactly one existing £20 overpayment, not a second payment and not a new recovery type.
3. £120 settled then reconciled to £120 creates no row.
4. £120 settled then reconciled to £130 creates exactly one ordinary £10 residual.
5. Ten protected hours, source reversal, Office Wait and later equal source reappearance produce no duplicate pay or recovery.
6. Partial settlement is compared with complete current entitlement rather than with the nominal target change.
7. Full, partial and zero same-candidate/same-channel recovery headroom remain unchanged.
8. PAYE, Umbrella, PAYE-to-Umbrella and Umbrella-to-PAYE use existing owners and interfaces.
9. Provider missing/disabled/stale/ambiguous cases fail before publication with zero financial, Draft or invoice writes.
10. Active Draft, scheduled, executing, settled, cancelled and recreated states remain frozen/unchanged under current rules.
11. Repeated Draft creation, cancellation and lost responses do not duplicate entitlement, payment or recovery.
12. Remittance/payment-notice output contains the ordinary item exactly once with no protected/exceptional label.
13. Ordinary Weekly, Daily, invoice, reports, exports and existing Banking Pay API/UI are byte/schema/action equivalent outside the approved seam.
14. The dependency closure and caller remain byte-identical and collapse root plus every private generation member into one unit from every seed.
15. The only existing financial decision change is the reviewed narrow `LIVE_ENTITLEMENT_INPUT` predicate.

No Weekly Source release can be described as complete until these joined checks pass against the separately owned implementation.

## 17. Review disposition

HANDOVER 2 compatibility confirmation should be recorded as one of:

- `COMPATIBLE`: the producer contract is exactly what C1 expects;
- `COMPATIBLE WITH NAMED CORRECTION`: list the exact producer field/relationship/protocol correction required; or
- `INCOMPATIBLE`: identify the precise approved C1 rule that this producer violates.

This review is not implementation approval, runtime acceptance, release authority or permission to change Banking Pay.
