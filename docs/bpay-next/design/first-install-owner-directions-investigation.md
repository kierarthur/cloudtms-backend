# First TEST install owner directions — read-only investigation

13 October 2026. Located and read original backend `codex_outputs/claude-stage2-continuation-20260925/SEND_4_TO_BANKING_PAY_FIRST_INSTALL_FIXES.md` and its later `SEND_4B_TO_BANKING_PAY_TIMING.md`. Applicable workspace/repository rules and current BUILD-BE `docs/AUTOMATIC_TEST_DEPLOYMENT.md` / `docs/DATABASE_RELEASE_BIBLE.md` were read first. Root alone implements. No product/matrix edit, network, secret, gate/test, SQL/RPC, installation or Git mutation was performed.

## Required order

SEND_4B supplies the latest timing and overrides SEND_4's former wait-until-all-phase3 wording:

1. Finish current WB-34 Seq146; do not abandon it mid-card.
2. Perform the first-install fixes and integration **before XD-01/XD-02/XD-03**. Stored-credit cards are not prerequisites for that candidate.
3. Commit and push the integrated candidate to Root's own backend and Office GitHub branches; do not yet promote to `test`/Office`main` or install. Record both exact40-character SHAs plus a one-line content description in `docs/bpay-next/FIRST_INSTALL_CANDIDATE.md`, and append BUILD_LOG.
4. Continue XD-01…03 while the independent owner review checks that exact candidate. New problems require a new candidate commit and updated identity record, preserving history.
5. **Install only when the owner says the re-check has passed.** Then promote through the protected route, install disabled-first, run only the approved safe smoke checks and build F-17 next. Work finished meanwhile may join the reviewed candidate or next rolling release; do not silently mutate the already reviewed candidate identity.

The later specific candidate-branch push instruction authorises that bounded backup; it does not authorise release-branch promotion, installation, activation, LIVE or another task's publication.

## Exact first-install fixes

- Legacy-fence test `tests/bpay-next-legacy-fence.test.js:188/209`: reconcile with reviewed EX-01 reauthentication exemptions, retaining handler-level negative proof that legacy actions cannot use the exemption.
- Shared-calculator source classifier: narrowly admit the genuine trusted quote/ratio/metadata return in `12102026_1646…case_channel_resolution_v1.sql:182–193`; add its positive regression and retain hostile-formula negatives. No economics change or blanket formula allowance.
- Retained-source pins in `scripts/bpay-next-source-gates.mjs:2313,2318`: re-establish exact approved predecessors by canonical LF hashing; change only justified pins. No blanket repin or gate weakening.
- Backend integration: include every then-current TEST-only commit (the review found nine through`e0a02bb5`, which is historical evidence); union migration-lock entries without changing any existing hash, including`10102026_1622_candidate_notification_management.sql`.
- Final owner collision: compose **one reviewed final** `candidate_submission_finalize_atomic_v1` preserving NEXT query admission from`10102026_0931…outer_entry_admission_v1.sql` and TEST imported-expense QR approval from`10102026_1347_import_expense_qr_approval_v1.sql`. Update exact final-definer registry/closures; never choose whichever old body sorts last.
- Contract: clean PostgreSQL17 replay of integrated source in the existing proof container; export a new generated contract, commit source+contract together, run coupling and all applicable gates. Neither conflicted contract is authority for the merged code. Use the existing release proof machinery/canonical ordering, not manual installation or ledger repair.
- Office: preserve all current assets/NEXT script order in index; combine main's contact-policy eligibility/review flow with NEXT's busy/original exact-request retry behavior in`js/weekly-source/import-workspace.js`. Eventual coordinator uses the reviewed clean integrated Office worktree.
- Include every runtime/release input before candidate publication, including actual completed WB-34 repeatables0926/0932 and`js/bpay-next-resolution-adoption.js` where still current. Exclude secrets/unrelated files; a dirty local runtime is not a complete release candidate.

These are reported failures at committed backend497fb059 / Officee4fa16a, not fresh gate results. This investigator ran no checks and claims no fixes implemented. The named readiness report and safe smoke-check document have now been read; exact qualification is below.

## Disabled-first and later transition

SEND_4 corrects the old assumption that a transition switch already exists. F-17 Seq276 and F-21 Seq281 remain missing and installation seeds LEGACY. First install adds NEXT definitions/runtime **without switching it on**. Candidate/communications/autonomous-feature disabled-first rules remain independently binding.

After the owner-approved install, bring F-17 forward immediately, plus only F-21 parts it actually needs. Build the reviewed target/permission/audit/current owner+epoch/readiness transition and use that owner to switch normal TEST to NEXT. Direct UPDATE is forbidden. SEND_4 explicitly supersedes F-17's old normal-TEST-pre-release-owner acceptance and requires no NEXT→LEGACY reversal for the unused legacy Banking journey. No transition is authorised as a shortcut during candidate preparation or first install.

At eventual release: permanent inputs on GitHub, fresh agent-created TEST connection/trigger evidence,`npm run release:test`, database verification first, then normal backend → real private → synthetic private → public broker, Office last. Record installed commit/migrations in`docs/bpay-next/TEST_INSTALLS.md`; safe read-only/disabled-first smoke evidence is not payment or full joined acceptance.

The early HealthRoster group HR-06/01/03/04/07 plus five reopen children remains **BLOCKED-ON-HEALTHROSTER** pending its published shared pay-permission helper. Record the deferral/schedule; do not let it block WB-34 or first install and never create a second producer. Policy X/OR-44 and every other owner rule remain unchanged. No real payments/provider/bank submissions, real emails, secret changes, hand-editing the database or LIVE operations are authorised.

## Input identities

| Owner instruction | SHA-256 |
|---|---|
| SEND_4 | `D5FE8CB6EF9C07A44A3EAFDA1806705F89219BE2E735FB8907B474A2D94D7990` |
| SEND_4B | `132AB4097DD3A091326453416DD934EDD9F84387ECB9E6BF0BE06AF4A5881528` |

## Readiness report detail and safe smoke boundary

`BANKING_INSTALL_READINESS_13102026/REPORT.md` reports28 source tests:25pass/3fail at the pinned archive. Its status is BLOCKED for that checkpoint; it is not proof that the installed normal TEST is broken or that Root's later fixes fail. The exact early-fence retained source is`11102026_0904_banking_pay_next_early_candidate_financial_fence_v1.sql`; the reported committed LF SHA is`0c217e3af301c23f4bc85aee011a3ce6bc14056359a1b4e22dccf30000781c25`. That mismatch is not explained only by CRLF. The0814 discard pin is a line-ending error. Audit both finite exception sets and their approved predecessors before correcting canonical hashes.

The actual colliding signature is `public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)`. Current saved registry still selects0931; TEST1347 defines that signature later in canonical order. Preserve both complete native behaviors, ownership/configuration/ACLs and all correct predecessor clauses; update final root/physical/allowed-predecessor/closure evidence to the composed successor. A mechanical union or a registry entry merely blessing1347 cannot establish preservation.

The review's initial eight TEST-only commits became nine at closing`e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92`; Office main was`887e7af7cd15d9f4034ad73806b44344f3eb0791`. Those are historical pins, not current remote-state assertions. The ninth backend commit adds/reasserts Candidate daily copy-readiness/current booked Rota owners and an exact contract-neutral declaration. Preserve its actual definitions and current operation policy/notification/availability changes; do not reuse a neutral attestation to avoid the changed Banking contract. No remote refs were inspected by this reviewer.

The smoke plan is **AUTHORED/NOT_RUN**, not a standalone grant for fixture/data/action work. Safe disabled-first observations include served identity, ordinary login/navigation, owner status, existing Timesheet/Invoice/Source pages and existing MyTMS weekly/DAILY view, within existing approved sessions/targets. Stop if login needs an unapproved email/SMS challenge. Do not create a Banking Draft or trigger reauthentication challenge merely for smoke coverage.

Ordinary and managed Source Authorise/Unauthorise are separate write paths. Invoice Batch Generate is a write, even when stopping at DRAFT. New-file upload preview stages/records attempts and is also a write. Each requires prior exact fixture scope and demonstrated queue/cron/auto-authorise/auto-invoice/communication safety; absent that, use existing reads and report those write checks SKIPPED. No select-all, paid/invoiced starting fixture, Issue, email, payment/provider, arbitrary cleanup, flags or queue draining. Any unknown outcome retains/reconciles its original request, rather than issuing a new mutation. MyTMS login/view does not authorise edits/submission/withdrawal/invitations/availability/notification mutations.

Fresh coordinator proof is obtained only at release time, with exact integrated commits/current trigger+active-build identities and the normal15-minute proof lifetime. A deployment-branch SHA or unchanged Worker config is not deployed-version/secret-presence proof. Release failures retain original immutable ledgers; no manual VERIFIED repair, omitted mandatory verifier, blind timeout increase or duplicate remote release job. Database verification gates runtime/Office publication. Final smoke PASS/FAIL/SKIPPED must stay distinct from Banking readiness and full HealthRoster/correction/business acceptance.

| Additional read input | SHA-256 |
|---|---|
| Readiness REPORT.md | `A8383837CA92E16AFC8174B566E5AAEF33C555023174FDF6B3C465C64F06D4E2` |
| SMOKE_CHECK_SCRIPT.md | `01659B5974D68E01D7493E26B18075A1773880C434EFFF6802D9843F38F5D7AC` |
| Current BUILD-BE automatic deployment contract | `9BCAD58F6C0C1D7B015BCDC881770A45D16AD2FF6F69628D4EF3CC9C59F118FE` |
| Current BUILD-BE release Bible | `968AB48D5B88181E6EB12F067309DD41162D3A2D816B47FB076E2BB4B87A7F5B` |

Investigation complete. Earlier HealthRoster insertion recommendations do not override SEND_4/4B's explicit deferral and first-install-before-XD timing. This document records authority/order only; it does not authorise this read-only reviewer to execute implementation, tests, branch pushes or release steps.
