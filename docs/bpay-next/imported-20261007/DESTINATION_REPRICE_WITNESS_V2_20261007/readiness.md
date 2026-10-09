# Destination repricing witness V2 — acceptance snapshot correction

Status: E-only, unadopted, uninstalled, not native-qualified and not release-ready. MOV17 remains OPEN. No W, native database, hosted, commit, deployment, score, payment or provider mutation occurred. Source V1 and Banking comparison V1 remain frozen. This is the defined Source correction only; it does not add financial integration.

## Result and exact artifact scope

V2 removes both broad eight-relation SHARE fences and the added Contract FOR SHARE reads. A new decision uses the existing I1 locks, an exact replay reread and one fresh explicit STABLE context SELECT under a VOLATILE READ COMMITTED entry. The reviewed complete destination economics and actual1550 stage inputs/output become one immutable certificate. Verification never rebuilds that context against today's rates/settings. The actual1550 destination-HEAD branch freezes the accepted channel, complete available rate schedule and approval labels; ordinary/unwitnessed/first-authorisation paths keep their original authority.

Artifacts:

- `07102026_0105_weekly_source_destination_reprice_witness.sql`: byte-identical frozen V1 proposed certificate schema, SHA256 `36b7cb975e4f0d98707e43a62cb9f2879d24fa18a5d3d76eb20dff5dd208d552`. Same `private.weekly_source_destination_reprice_witness_v1(id)`, no extra plan table or balance fields. Not a new timestamped migration adoption.
- `07102026_0135_weekly_source_destination_reprice_v2.sql`: complete proposed functions, including the closed stage-snapshot integrity checker and exact stage certificate reader; UK creation time 01:35, 7 October. Existing public/private function identities retained, certificate relation unchanged.
- `destination-reprice-preview.mjs`: unchanged V1 adapter exporting/calling the existing actual row calculator, economic snapshot and comparator owners. No new pricing implementation.
- `build-candidate.mjs`: exact W preimage/in-memory inverse builder. `--emit` writes only mechanical E artifacts, never W or frozen siblings.
- `destination-reprice.test.mjs`: 13 retained actual-owner/boundary checks plus 5 focused V2 ownership/static checks; no database or hosted service.
- `source-owner.patch`, `source-owner.inverse.patch`, `exact-preimages-and-replacements.json`: five exact current source modifications and raw/LF pins. The inverse is source evidence, not an installed-data rollback.
- `artifact-sha256.json`: all artifact hashes, nine unchanged W authority dependencies and six frozen sibling/review dependencies. It excludes its own self-hash.

The five modified existing source files are: actual upload-publication row-calculator export; `17092026_0300` closed canonical destination certificate extension; winning `04102026_0556` moved-content witness check; actual `03102026_1548` chosen-detail witness branch; and actual `03102026_1550` frozen destination stage branch. The first four seams remain V1's exact transformations. Only1550 is new. Existing1550/1548 Contract/root FOR KEY SHARE identity locks are untouched; the private immutable witness row FOR SHARE remains an ordinary exact row lock, not a configuration fence.

No proposed object is in W's migration/definition lock, ACL contract or seal. Root adoption must reread current saved disk, use protected authoring/release tooling, and install the correct complete winning definitions in release order. Filenames and passing static checks grant no installation authority.

## Acceptance boundary

For exact prior witness replay, the original input SHA/actor conflict checks and existing saved-request publication replay execute before any new isolation/configuration read. No re-pricing of historical replay. For a new decision, non-READ COMMITTED fails before I1. The entry remains VOLATILE, then executes existing I1 → exact replay reread → separate `SELECT private.weekly_source_destination_reprice_context_v1(...) INTO v_context` → actual quote/review digest equality → certificate insert → existing proposal recorder → existing real immediate publisher. A stale review fails before certificate/proposal writes; later publisher failure rolls back the whole transaction.

Pricing linearizes at that fresh internal statement, not at outer RPC entry, preview time, transaction timestamp or a flag. The retained server `accepted_at_utc` is the existing transaction timestamp, not a claimed pricing snapshot identifier. Configuration committed before the fresh read must change the rederived review and reject the old preview. Changes after the read do not reprice the accepted certificate. This is accepted-at-read economics, not latest-at-release economics; genuine later Source work/root/HEAD/Final currentness still belongs to the existing publisher guards.

The context itself, constructor, policy/settings/finance owners and pricing descendants retain their existing STABLE/IMMUTABLE declarations. The constructor's nested `public.timesheet_lifecycle_guard_signature_v1(...,false)` remains default-VOLATILE: its false branch reads work/Contract-week/TSFIN signature facts, while its separate true branch can log diagnostics. V2 does not globally relabel it STABLE. One STABLE label alone is not proof of coherent nested work currentness; existing I1/current-work/CAS and native volatility/currentness evidence remain required.

## Full genuine stage snapshot

The same context directly reads actual destination Contract, current optional TSFIN, Candidate and Client rows. It records genuine root/version/raw booking/Contract/Candidate/Client/optional TSFIN identities; actual display/reference/job/band/Timesheet labels and their original inputs; full PAYE/UMBRELLA/charge rates; bucket labels; Additional rates; and each actual TSFIN→Contract→Candidate/Client mileage fallback input. It calls existing immutable `private.bpay_next_contract_rate_rows_v1` with exactly1550's existing coalesce semantics and stores its complete typed output ordered by family/code. No missing rates, labels, work clocks, bucket units or fallback inputs are invented or trimmed.

The snapshot integrity checker requires exact keys, types, supported family/code shape, label/storage precision bounds, duplicate-free rows and all five positive selected-channel/charge standard rates. It rederives complete helper output only from SAVED JSON inputs, never mutable tables, and checks metadata and actual fallback resolution against their saved inputs. That frozen-input integrity check is not a release-time live-helper read. Unsupported helper output fails before Source writes.

The1550 helper resolves the exact current destination HEAD's bundle/revision to the same immutable certificate and checks accepted destination root/HEAD, raw booking, version, Candidate/Contract lineage and immutable request/review integrity. A's exact companion HEAD returns NULL and stays on the ordinary path. Certified B gets the accepted channel, optional financial evidence ID, complete schedule row count, labels and typed-row INSERT. Unwitnessed1550 paths still use their original live helper and labels verbatim. Existing route, current TS/HEAD/authorisation, complete I7 approval-basis, original detail/clocks, Source total and WORK identity guards are not bypassed.

Verifier integrity is rederived from saved context plus saved component calculations: quote/review SHA, complete component/detail outputs, canonical request and control digests, actor, decision bundle/revision and existing bundle whole-root review actor/time. The only quote→accepted transformations are exact server witness UUID/review digest and server whole-root Office acknowledgement. Deferred mode normalizes only publication mode/pending ID to the accepted IMMEDIATE digest. No mutable rate/settings/current-context read occurs in verify or chosen-detail certification; existing0556 live work/root/HEAD/before-inventory and all-or-none publication guards remain authoritative. This prevents own-B authorisation from falsely staling its own pricing context.

The original raw input SHA remains the exact replay authority, including caller calculation-array order. It is not falsely reconstructed from the canonical sorted component output. Frozen review/output integrity is independently rederived; exact replay still compares the original hash. Verifier preserves the actual accepted review-time JSON representation and checks its typed instant against saved server time, avoiding a session-timezone-dependent reserialization.

## Arithmetic and scope retained

Existing weekly-rate-owner rounding remains two-decimal bucket hours before rates (e.g.451 minutes→7.52h), not raw minutes/60. The existing V1 SQL quote check uses the original genuine canonical rounded hours. V2 adds no alternative calculator, rate conversion, history scan or inferred units. Binary JS/SQL monetary boundary and signed/half-cent equivalence still require native joined qualification; these18 tests do not settle it.

The positively proposed lane remains HEAD-origin WORKED_TIME with genuine immutable original economic snapshot/clocks/break/buckets, same classification policy and all five actual destination pay/charge rates. All four pay-channel directions use destination rates. Initial TSFIN, Additional/amount/unit-led/expense components and changed bucket policies remain explicit unsupported/refusal obligations, not accepted omissions. Full Office authenticated route/complete financial prior-paid review before Source commit remains OPEN.

No financial snapshot/plan/receipt hooks were added. Root's separate joint work may version narrow server-owned `financial_context` and `financial_plan` fields into this exact context/quote review and bind their certificate ID at actual1301; this packet does not accept generic caller extras or fabricate future binding/APPLIED evidence. It returns `financial_comparison_resolved:false`. Positive/negative/held/repeated-move/both-native-channel/full Case outcomes are not closed by SourceV2 or the frozen zero-only BankingV1.

## Evidence and required native gates

Completed finite evidence: `node --test destination-reprice.test.mjs` passes18/18; both module syntax checks pass; `node build-candidate.mjs --emit` verifies five unique current preimages and exact inverses; `git apply --check --unidiff-zero source-owner.patch` passes without applying. W's read-only `node scripts/cloudtms-db-release.mjs check` passes297 migrations/793 repeatables before/after. Frozen SourceV1/BankingV1 and nine unchanged arithmetic/settings/rate-owner dependencies are pinned in the manifest. Unchanged1920 calculator/S2-R28 differential and legacy24/12 runs were not repeated.

Root must separately source-review and prove in its authorised native rollback envelope:

1. Exact compile/installed volatility/search_path/ACL and closed actor/request denial; fresh restoration after rollback. Validate the actual nested false lifecycle branch, not only STABLE declarations.
2. A real I1 wait followed by a configuration/rate/policy/date insertion committed before the fresh SELECT rejects stale review. Higher isolation is denied for new acceptance; exact historical replay retains saved economics.
3. Configuration changes after acceptance cannot change accepted channel, labels, complete standard/Additional/mileage schedule, money, chosen detail, deferred release or replay. Test full rows/counts, not only pay totals. No new global writer lock or trigger fence exists.
4. Genuine changed Final/HEAD/root/work/before detail is rejected by unchanged Source/currentness owners. Own-B authorisation/staging does not false-stale accepted pricing; failure leaves no partial certificate/proposal/head/detail/binding state.
5. Exact vs conflicting replay, actor mismatch/denial, forged outputs/control/review/witness, signed/half-cent/odd-minute boundary, ordinary/query/invoice/freeze/first-authorisation siblings and all-or-none pair ordering. Keep chosen detail before activation→1570/1301 binding→atomic enqueue.
6. Protected complete-definition/ACL/lock/seal adoption and full joined banking/Office family proof before any release claim. No independent zero-only native/adoption is represented as full MOV17 completion.

The removed V1 table-lock proposal is superseded by this precise acceptance snapshot semantics; its earlier broad-lock writer-census/latency claim is not carried into V2. Normal existing owner locks still require their actual native order/currentness proof. No release, adoption or native success is inferred from static evidence.
