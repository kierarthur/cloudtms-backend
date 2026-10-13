# Install 1 WP-PP composition source critic — 14 October 2026

**ACCEPT the focused saved source after the current-user transport repair described below.** One concrete P1 integration defect was found, reproduced without a network call, and corrected by the integrator during this review. No remaining focused source blocker was identified. This verdict is not a clean native rehearsal, generated contract, installation, runtime, API, activation or Banking NEXT compatibility receipt.

This specialist lane was read-only except for this report. It did not edit runtime, SQL, tests, Git refs, progress or Banking files. It made no database, Docker, browser, remote Git, release or deployment call.

## Authority and exact source

Read the workspace and candidate backend AGENTS, SEND5/D9, SEND5B and SEND7, the current two-install plan, the source-readiness review, original PP implementation handoff and WP0 shared-owner ledger. The latest owner two-install plan supersedes SEND5B's historical 3–5 estimate. The integrator supplied the actual Banking completed/released window and froze Install 1 to WP-PP only, features OFF. This lane did not independently inspect Banking's completion record or protected run.

| Pin | Value |
|---|---|
| Candidate worktree | `C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install1-14102026` |
| Candidate branch / HEAD | `codex/healthroster-install1-14102026` / `07e66fb2b2742fbe634a41582f2ad3e004eed3b9` |
| Current source status | Saved, uncommitted changes/new files atop the Bank HEAD; all preserved |
| Bank composition predecessor | `07e66fb2b2742fbe634a41582f2ad3e004eed3b9` |
| Original PP predecessor / proposal | `e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92` / `547c74da5f0a05b09297c042bc7f3cc0c0105b85` |
| Original proposal use | Bounded PP definitions/hunks only; broad 547 source is not this candidate |
| Current plan saved SHA-256 | `fef150aa6955aa3b6ca4bb589dc8d8123c76726615deec8312376884450df95a` |
| Readiness review saved SHA-256 | `8be87ec1dacdd55a059139459ef02ecbfc6451cd807600e88f4e1967d94f5718` |
| WP0 ledger saved SHA-256 | `66769c43905d335712b34d73b38fac2badd571ed4e37f54e3e7a314541398881` |

Saved source hashes at the final review read:

| File, relative to candidate worktree | SHA-256 of saved bytes |
|---|---|
| `supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql` | `8f65ee0c9cf46b5465c920057212e6d2499b119e05c4c1dedd2f0cfecb89eea7` |
| `broker/src/index.js`, after transport repair | `5d6912b6e502af72b586d6205dd6f99fdc1347c73e4bdeae98dc36b6c73ba5d8` |
| `broker/src/weekly-source/routes.js` | `034b96260f679a4c320648ec29a9ff1d19f982d800c50c6aceccfb69ea99fc67` |
| `broker/src/payment-actor-permission.js` | `cca0e4fa2d6ca5627d900538ea1d9176d4ae31dc29bb775067c997f3dff70c4e` |
| `supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql` | `fd284e10ede8d670cc79b6aad63d8a14a5325e81c87f81c68f8c340ec0e3ab7a` |
| `supabase/release/current-release.json` | `285e2fc9a9a50772e2f27390a28d69a216e65265f45ba018a642c9cd40d7a4df` |
| `supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql` | `2234a1645fded87fd79dff419ccb341c987c6bd65e6dd1800f5ab6e5cae91dd7` |
| `supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql` | `6a25299914fc93cdac9938b240eb0dfa721875e11de2bbff8c7a9d31b6bd3f9d` |
| `docs/bpay-next/final-definer-closures.json` | `2f96168ce04f18091c256b1965f7aad80b48ecc8393efee522403904365c9552` |
| `tests/weekly-source/weekly-source-pay-auth-transport.test.mjs` | `4cf1d573cf2545759c7fd938eb8066b0b6ee8e8b2e16c3455deaa5ccf1a2bc09` |
| `tests/weekly-source/weekly-source-office-presentation-integration.test.mjs` | `41419003af1ddb0be2f1b0048ae50dfdcc4a22cef4d8f40e90c40fb50eb45633` |
| `docs/healthroster-build/install1-source/PP_SOURCE_COMPOSITION_RECEIPT.json` | `315f5a5eaa491bd2d099fee67893c097897a095347874b9a31b96f7406afe8c6` |
| `docs/healthroster-build/install1-source/PP_CURRENT_USER_PROJECTION_FIX.json` | `d8087257f2e0258daad4d2d4798336de2ff8547d05a4d4be2b0e8b63c0bd702e` |

These are file-byte/source identities. They are not PostgreSQL canonical definition or contract hashes. The original composition and byte-preservation receipts precede the one-line current-user repair; the separate repair receipt records index.js from `3f82affc…` to `5d6912b6…`. Future commit-bound evidence must describe the repaired source, not only the original receipt.

## P1 found and closed: the real current-user query omitted both flags

At the initial reviewed source, `broker/src/index.js:137018` selected only `id,email,role,is_active,password_hash,session_version` in `sbGetUserById`. The PP changes to `requireUser` converted `user.payment_authoriser` and `user.payment_golden_key` to strict booleans, but PostgREST could not return either omitted column. Every real non-admin pay actor therefore became `{payment_authoriser:false,payment_golden_key:false}` and failed the widened routes. The existing PAY tests injected already-populated actors and could not reveal this.

Independent pure counterexample: execute the actual extracted `sbGetUserById` and `requireUser` with a valid active `user`, matching session version, and a simulated PostgREST projection of a database row whose `payment_authoriser=true`. Observed projection was `id,email,role,is_active,password_hash,session_version`; the returned actor had both flags false. No endpoint or real token was used.

Required repair was narrowly additive: include the two existing flags in the same current-user projection, retain every original field and authentication/session/tenant decision, and test the actual transport-to-guard-to-route chain. The integrator saved precisely that repair. The new regression models column projection, executes the actual `sbGetUserById` and `requireUser`, then uses the real Weekly Source dispatcher. It covers both physically legal flag cases and preserves current inactive, missing-user, session-version mismatch and forged-token flag/role denials. The SQL golden-key fixture correctly respects the existing golden-implies-authoriser constraint; this review does not request an invalid SQL fixture or weaken that constraint.

**Closed at the final saved hashes above.** Actual authenticated HTTP still remains required after installation.

## Accepted fixture correction: native Summary array identity

The earlier presentation test required `result === rows`. The exact Bank Summary reader creates `rows=[]`, appends each native result page and returns the assembled array; it preserves row objects and order but cannot preserve the response array identity. PP did not change that reader.

The integrator's forward fixture correction is valid: deep equality proves values/order, strict equality for each row proves no category/action/financial object rewrite, and the added 150-row case proves exact native requests of 100 rows at offset 17 and 50 rows at offset 117, retained filters/actor/RPC, complete ordered rows and source immutability. The one-page request-count assertion and invalid-contract refusal tests remain. This corrects a false baseline expectation without removing the product invariants or changing native Bank code.

Independently ran the actual-auth-transport and Office-presentation suites after these corrections: **14 tests passed; 0 failed, skipped or cancelled; Node exit 0.** Other larger/dedicated batches are integrator evidence, not newly claimed as run by this lane.

## Composition, source order and security review

The repository's managed SQL parser read all sixteen complete proposed definitions. An independent check reconstructed each existing definition with `git merge-file -p(current Bank, original predecessor, proposed PP)` and required exact complete-body equality with the saved successor; the new helper required its exact original proposal definition hash. **16/16 matched.** Every changed permission/read-view hunk was inspected. This proves composition equality, not PostgreSQL compilation or dynamic-patch execution.

The complete current C1 completion body retains SP-12's conditional NEXT admission before row locks. The complete Local completion body retains its native same-transaction query refresh and exact manual-resolution/publication/receipt sequence. The complete manual-review open body retains its native query refresh only for a new review. The PP changes neither replace nor omit those Banking additions. Financial DML, replay, signature, receipt, rate-policy, family locking, frozen publication and ordinary native delegation remain in the merged bodies.

Current source/include scan: **389 migrations, 1,019 repeatable roots.** All sixteen PP signatures resolve last to `14102026_0044_weekly_source_protected_pay_authority_v2.sql`. The original WP0 ledger resolves all **123 rows: 28 direct shared owners and 95 Banking consumer dependencies, zero missing**. Eleven ledger owners now have the bounded PP final root; the other 112 final physical source files compare canonical-byte equal to the exact Bank commit. This refresh is static source evidence. It is not a fresh installed catalogue/ACL census and does not certify dynamic transformations.

The SQL helper is active AND (admin OR authoriser OR golden key), stable SECURITY DEFINER, qualified to the current `public.tms_users` row and pinned to `pg_catalog,pg_temp`. Its service grant is added to the independent service inventory rather than the separate owner-only private list. Both NEW and UPGRADE profiles register the PP verifier. The new root reasserts owner mapping, PUBLIC/anon/authenticated revokes and service_role execution on all sixteen functions itself, then reloads PostgREST schema. It therefore does not rely on an unchanged older ACL root running again during UPGRADE. Final installed browser isolation and effective grants remain mandatory.

Worker route admission uses current-row actor fields, binds browser actor IDs to the authenticated actor, and restricts non-admin PAY access before dispatch. Upload, recheck, finalise, recovery, settings, communications, invoice and later-change commands remain unavailable to that reduced view. The real current-root Source probe precedes correction/lifecycle delegation; ordinary/non-applicable/Daily mutations remain denied for a non-admin, and unknown/PGRST202 routing remains a retryable 503. Existing session/tenant checks, original native lifecycle owners and current Banking imports/hooks remain. Source hints require the authenticated matching actor and preserve explicit false state/lock hints. The helper chooses permission only; it does not choose money, Draft freshness, provider actions or NEXT readiness.

No Policy X drift was identified in this bounded delta: no new economic-key ladder, TS_DAY derivation, live post-Draft finance fallback, Draft/provider/settlement/remittance behavior or freshness bypass was introduced.

## Retained boundary and gates

Bank's final `weekly_exceptional_pay_next_publication_status_v1` and `weekly_exceptional_pay_publish_next_v1` still contain the older active-plus-payment-flags predicate, so an active admin with neither flag remains excluded there. This is retained Bank source and a NEXT consumer/activation boundary; Install 1 is features OFF/LEGACY, and Banking HR-06 consumes only the later genuine compatible helper receipt. This lane does not broaden Install 1 or authorise edits to those Bank owners. Do not claim future NEXT admin parity from this PP receipt alone.

Before installation the integrator must still produce the exact reviewed committed/pushed source and PostgreSQL 17 contract, current generated canonical/security evidence including dynamic final-definition transformations, and the complete exact-candidate rollback rehearsal under the retained shared turn. The protected database release and ordered runtime publication, installed definition/owner/ACL identity, genuine authenticated API actor/reduced-view/ordinary/PGRST202 probes and PUBLISHED_HELPERS receipt remain unproved by this review. Empty reduced-read native fixtures and mocked transport positives are not populated production-view or first-use API acceptance.

No live hosted before/after fingerprint, SQL execution, contract generation, deployment, browser, feature activation, payment/provider/settlement/email action, secret read/disclosure or commit/push was performed by this critic. The report is the sole artifact created by this lane.
