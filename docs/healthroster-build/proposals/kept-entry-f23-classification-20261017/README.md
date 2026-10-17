# Finite Kept + correction-entry F23 proposal

Review target: committed integration `24c57e6c7af4a8fb07d5e3126ab9ac76e9448560`, with exact a34 Kept and 4a954 correction-entry source receipts. **Source type/construction classification only; native NOT_RUN.** This proposal changes no SQL, permission, predicate, financial effect, release policy, registry or historical review. It is not mounted in integration.

The candidate helper and four exact gate hunks cover nineteen current physical inputs: two Kept migrations, fourteen Kept pages, entry admission and two entry pages. Their actual raw census is 94 occurrences / 93 distinct keys:

| Kind | Occurrences | Basis |
| --- | ---: | --- |
| whole-table tuple | 46 | Exact declared `%ROWTYPE`, complete single-table projection, actual relation/DDL tuple |
| UUID cardinality | 5 | Two newly reviewed exact singleton producers; three copied original statements retain original classification |
| JSON concatenation | 2 | Two replay points of one native typed JSONB object |
| aggregate scalar | 5 | Three copied Invoice statements plus one original classifier point in each relocated copy |
| dynamic/inert construction | 36 | Two new migration points; seventeen original classifier points in each copy |

The entry reader and its one-time admission have no raw special-construct hazards. Their exact source pins still participate; this does not add an entry capability or convert its advisory response to READY.

## Actual types and provenance

The complete tuple proof uses the existing `provesSingleTableTuple` implementation. It requires the same actual relation as the variable's `%ROWTYPE`, selects the complete tuple, and rejects partial or joined projections. Seventeen relation proofs are drawn from the actual `current-contract.json` (`da0e0870442c151534c04d108757a0d76a812879aafae71f01486c7077ce674a`) or the new decision-store DDL. The latter has eight actual columns: UUID identities, TEXT decision/reason, BYTEA fingerprint and TIMESTAMPTZ creation time. PostgreSQL17 AST extraction independently confirms these declarations. These are source catalogue/DDL facts, not a claim about an installed catalogue.

The first new UUID producer counts the same native group-client rows that feed `MIN(gc.client_id::text)::uuid`. A non-singleton result nulls the inferred client before use; an explicit Office choice remains the existing independent authority. The second counts the same exact native prior-event candidates that feed `MIN(e.id::text)::uuid`: more than one raises ambiguity, zero returns null, and only one can return the UUID. The native fields are UUID; COUNT is BIGINT and MIN consumes TEXT. There is no MIN(UUID), guessed id type or arbitrary representative. Both complete producer and actual cardinality guards are checked. This classification grants no actor, client or financial authority.

Both JSON points extract `result` from native `audit_events.after_json` (JSONB) and concatenate the fixed `jsonb_build_object('idempotent_replay',true)`. The complete writer declares the native audit row type, constructs the original result with fixed JSON object keys, stores that object in its typed ACK, and checks exact actor/request/hash at both replay points. Offline PostgreSQL17 AST evidence confirms the root operator is concatenation with JSON extraction on its left; text extraction cannot borrow this proof. This is an object/operator type proof. Genuine native ACK/audit invariants still require their separate native verification.

The migration's first apparent dynamic point is an inert expected-body operand of a catalogue comparison. It equals the exact complete immutable guard body; it is never executed as SQL. Existing body, security-definer, function kind and actual owner drift refuse before bootstrap. The other point executes one fixed CREATE POLICY statement against the new public decision store. Its single escaped `%I` value is CURRENT_USER; service_role is fixed, and RLS is enabled/forced with direct table privileges revoked. No user SQL, arbitrary identifier, query or DML is admitted by that construction proof.

## Original evidence stays original

All five copied Health owners reverse through every current successor hunk, including the later a34 changes, to their actual complete current predecessors. A copied non-tuple hazardous statement must occur with the exact original signature/literal key and multiplicity in the real predecessor census. Its existing frozen or typed review must still admit it. Full reversal alone creates no allowance. `ORIGINAL_ALLOWANCE_TRANSFER.json` records these six transfers and their actual original files. Earlier source review artifacts remain unchanged.

Both classifier images fully reverse to the original `14102026_0709` classifier through their exact kept/entry literal deltas. The candidate runs the original aggregate/dynamic classifiers against that original physical file and transfers only the same exact statement and count; outer-file identity alone is rewritten. The lifecycle aggregate consumes the original declared integer ordinal. Dynamic table identifiers originate in the closed typed table contract; lifecycle trigger arguments use the original literal quoting; catalogue function identifiers use the original `%I` / native identity-argument construction; service signatures originate in the closed literal service contract. The new decision relation is exactly the appended immutable relation, and the three entry signatures are exactly the later closed additions. There are seventeen actual dynamic diagnostic points per relocated copy, not an assumed smaller count. Their existing construction/provenance reviews remain mandatory at runtime.

Whole-source and review pins detect drift after these semantic checks establish what is being classified. A hash by itself never establishes a type or construction rule. No global name/alias allowance or generic F23 waiver is introduced.

## Finite mount recipe

Mount the new `scripts/healthroster-kept-entry-type-review.mjs` and this package's `CLASSIFICATION.json`. Apply the four exact `GATE_EDITS.json` hunks to `scripts/bpay-next-source-gates.mjs`; the recipe includes exact input/output hashes and complete reverse. It reuses the original tuple checker, retains the original classifiers, and wraps only the allowance map passed to the existing special-construct assertion. All other gate checks remain in their existing order. The injected prior callback is precisely the existing tuple, UUID, aggregate, joined and dynamic classifiers on the original file; it supplies no new approval data.

This proposal intentionally does not copy the integration gate into the feature branch or edit integration. Root owns the actual mount, peer review and subsequent complete gate run. The nineteen scoped runtime SQL pins remain valid across the separately reported verifier/replay-policy work because that work does not alter them. Future Missing/LaterAccept source is outside this fixed nineteen-file scope and needs its own review.

## Verification and remaining work

`node .../test.mjs`: thirteen meaningful offline cases PASS. All nineteen actual sources and 94 occurrences pass with the original classifications and the finite new proofs. Negative cases refuse weakened cardinality gates, guessed UUID types, changed JSON extraction/request checks, joined/partial tuples, unsafe owner-policy formatting, duplicate/new hazards, source/schema drift and classifier transfer without the original review. The six frozen Health baseline files used by these tests are individually recorded; no broad source gate or Banking body inspection is performed.

`python .../ast-proof.py`: pglast v7.20 / PostgreSQL17.7 expression and eight-column DDL AST checks PASS. `build-gate-hunks.mjs` proves four unique forward anchors, complete reverse and Node syntax for the proposed gate image. `node --check scripts/healthroster-kept-entry-type-review.mjs` passes. These checks do not prove native PL/pgSQL catalogue compilation, RLS behavior, trigger execution, concurrency, API behavior, Office behavior or a release. No database, Docker, full gate, lock or deployment run occurred. The earlier four catalogue-dependent Missing-source body failures remain NOT_PROVED in their separate review.

Configured cwd, physical project/repository/Git/common paths and all write destinations were verified on D:. Existing unrelated untracked folders are preserved. Every Git mutation uses task-scoped `gc.auto=0` and `maintenance.auto=false`; this proposal performs no shared metadata repair or prune and makes no broader claim about the earlier maintenance incident.

Forward utility correction: bounded SQL reverses and gate-builder splices use literal callback replacements. The regression includes a SQL regex end anchor and literal dollar-ampersand, dollar-backtick and dollar-apostrophe values; all reverse byte-for-byte. Historical frozen commits remain unchanged.
