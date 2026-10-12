# WB-29 exact carry INSERT guard

12 October 2026. Design authored by the read-only investigation agent for root implementation; this is not independent product SOURCE or runtime approval. Implements EO03/BD161 within the existing private decision/event owners. Root alone writes product. No product edit, SQL/RPC/database call, fixture, test, hosted action or commit occurred.

## Minimal boundary

Use a dated full successor of `private.bpay_next_component_resolution_guard_v1()` in current2100. No new schema column, table, public input, command kind, receipt or caller protocol is required. The existing immutable `request_body` object (<=8192 bytes) can distinguish a server-built carry from fresh selected intent:

```json
{"carry":{"decision_id":"<exact prior UUID>","decision_version":"<positive bigint decimal text>","request_id":"<original W2 request UUID>"}}
```

All objects have exactly those keys, all values are non-null strings, UUIDs canonical lowercase, and version is checked in the actual native bigint range before cast. A body containing `carry` but failing this closed shape must refuse; it must not fall through to fresh SAVE. The public/Worker W2 closed request schema does not admit this body. The private W2 constructor builds it from its locked complete mapped closure and authenticated original request identity, never by copying a caller-supplied carry flag.

`request_id` records which immutable W2 action caused this mapping. At decision INSERT it is typed audit metadata, not an independent authority witness: the final W2 receipt is intentionally inserted after the decision/event mappings. Requiring that not-yet-written receipt here would break the atomic sequence or invent preflight storage. Exact native publication and event identity below supply the admission proof. The final existing W2 receipt records this request's actual child publications and old→new carry pairs before commit.

## Positive native carry proof

At INSERT, load the named old row by its exact `(id,decision_version)` point. Require old exists, `state='CLEARED'`, and new.id differs from old.id. Do not select latest/max, search history for another matching choice, or treat merely similar JSON as provenance.

Join its unique native CLEARED event by exact decision ID/version/event kind, and require:

- event.command_id equals new.command_id, and event.actor_user_id equals new.actor_user_id;
- event Work/Candidate/family/code equal both the exact old row and the new row;
- event.revision_id/approved_line_id equal the old row's **based-on** pair, as current2100 event guard150–156 requires. The old event is not restamped to the new published line.

This event was admitted while old was ACTIVE by the existing event guard. The existing old-row UPDATE guard permits only ACTIVE→CLEARED and retains all other metadata. An arbitrary previously-cleared row/event from another publication therefore cannot qualify.

Join the actual `bpay_next_publication` by new.work_id/new.published_revision_id. For carry require the published pair is non-null, publication.command_id=new.command_id, Candidate matches, and publication.predecessor_revision_id=new.based_on_revision_id. Its existing native POSITION_APPLY child must be sealed, with the exact Candidate member/one-member binding. Qualify the actual sealed payment-treatment revision, rather than an arbitrary ordinary publication: it has the existing non-null `payment_treatment_approval_revision_id` and original2135 root/parent/member continuity. Root may reuse the existing exact treatment identity helper for this point; no alternate Source/root authority is introduced.

Prove exactly one of the two native old-boundary arms:

```text
old.(based_on_revision_id,approved_line_id) = new.(based_on_revision_id,approved_line_id)
and old.original_basis = new.original_basis
OR
old.(published_revision_id,published_line_id) = new.(based_on_revision_id,approved_line_id)
and old.published_basis = new.original_basis
```

The second arm requires a real non-null published pair. Native basis is still derived by the existing basis helper from the new predecessor line/family/code; equality is structural exact JSON, not a digest, component-key guess or old treatment revival. Work/Candidate/physical Timesheet/component/family/code stay exact. Source-root and child payload identity are preserved by the original native publication checks.

Null-safe equality must retain **all** selected operands against this exact old row: resolution_mode, manual_replacement_rate, manual_amount_ex_vat, target_pay_channel, target_umbrella_id, target_umbrella_vat_chargeable, valuation_policy_id, valuation_date, reason, and original_position_basis. Retain the old Position JSON verbatim; do not project it through DTO strings or reset A/R/H/source_basis_channel. Different target/date/VAT variants are legitimate carried siblings under BD161. They do not have to match today's Candidate/policy window and are not quoted as fresh current intent.

## The single Position exception

Keep current2100 basis/mode checks80–85 and live Position checks86–95. Carry changes only the original_position_basis equality operand:

```text
fresh SAVE: new.original_position_basis = exact current Position JSON
proved carry: new.original_position_basis = exact old immutable Position JSON
```

For **both** branches retain live pos.applied_revision_id=new.based_on_revision_id, pos.approved_source_ex_vat equal to the native predecessor component Source amount, and existing finite realised/held checks. Do not bypass the Position read, early-return from the guard, update Position, weaken current Work/publication admission, or make carry a new consent for money. The quote owner still rejects a carried manual amount if its saved Position tuple differs from the actual residual scope.

Preserve the complete published-basis/Source guard96–119 after this exception. The new published native basis must still equal new.published_basis, exact child/predecessor must match, and physical/kind/component/date/unit/tax/units/policy/source kind/head/channel/Umbrella/Source amount/rate/charge checks remain byte-retained. Source payload is unchanged even when TARGET treatment differs. The source constructor and outer W2 already own current/applied/physical/expected context, complete clone, locks and overflow checks.

Implementation outline, not executable SQL:

```text
unchanged UPDATE/delete/state guard
unchanged native predecessor Basis/mode qualification
if request_body contains carry:
    strict closed shape/types
    exact old CLEARED row + same-child/current-actor CLEARED event
    exact actual treatment publication/child/member/predecessor
    exact old original-or-published mapped basis
    unchanged selected operands + exact old original_position_basis
unchanged live Position checks, except fresh JSON equality only when not proved carry
unchanged complete published-basis and Source checks
return NEW
```

Use `IS [NOT] DISTINCT FROM` for nullable tuples and explicit missing-row checks. Reuse named native refusal codes for malformed identity, stale/basis/mapping or incompatible publication; do not add a new financial state or broad error allowance. Version/new ID allocation remains deterministic under the native Work lock and existing exact uniqueness; a carry pointer does not authorize version reuse or collision suppression.

## Ordering, audit and locks

After complete finite closure qualification/staging and before any decision INSERT, original2135 publishes once and creates the real sealed child/member/publication. For each retiring old version: insert CLEARED while old ACTIVE; update only old.state; INSERT the carried/new version; insert its SAVED while new ACTIVE. The same transaction writes the original immutable W2 receipt. Existing deferred audit requires final old CLEARED/new SAVED consistency; do not force constraints immediate between steps. Failure rolls back publication, events, versions and receipt together.

new.actor_user_id/new.saved_at_utc and both new child events identify the authenticated **current carry operation**. This is acceptable audit metadata; it must not pretend that this actor originally selected the retained rate. The exact predecessor pointer leaves old actor_user_id/saved_at_utc/request_body/selected values immutable and reachable, including through subsequent carry links. No old timestamp or actor is overwritten; no history backfill or fabricated original chooser is needed.

The outer W2 owner keeps Candidate→all explicit Work→Position/related Case locks before staging/publication. The guard uses immutable exact points and the already-owned native rows; it adds no public/header/Candidate/module lock, clock or reverse lock acquisition. Existing PK/version/event uniqueness and publication Work/revision uniqueness bound the proof. Full mapped-closure count/byte caps and independent VAT/event-key support remain the separate EO01/02/04 work; this guard must not silently truncate that closure or replace an incompatible sibling.

## Exact sources and readiness

| Source | Evidence / SHA-256 |
|---|---|
| Current2100 guard/events/audit | guard65–122; event124–158; existing immutable state/deferred audit. `54a3b81cf74d1f86f7547b8948bca3d24a48493b23ed47e9cb949f9142b2f556` |
|2044 schema +2100 migration | decision request_body<=8192; owner-only immutable metadata; original_position_basis object<=2048. No new column is necessary. |
|2135 publication boundary | real sealed revision/pointer then child command/member/publication; Position remains on predecessor during append. `89a98fc7688803aabe93011edfcf588c7baa602dc46c903aa6e3000b1e0751e8` |
|2135 treatment identity | native source root, sealed predecessor, exact native child/member and complete child-set payload. Existing helper/native marker is authoritative. |
| BD161 inclusive carry | `176e92503e55a99c813a504e27b22423adfe888e849bf4ee23d8568a76a4ad51` |
| EO01–04 event-order review | `9951c28ce1bdd80b1eea7aa71a641d4a7e90f7cb6d38034ed832e08f40f2a624` |

Ready for root implementation of this narrow guard successor. Product SOURCE/compile and complete W2/whole-WB29 acceptance remain to be reviewed; no execution was performed here.
