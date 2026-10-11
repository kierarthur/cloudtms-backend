# EX-01 independent Worker source review

11 October 2026. **FINAL WORKER SOURCE PASS — W01/W02/W03 CLOSED; no essential Worker finding remains in the reviewed bytes below.** Read-only review of current saved, uncommitted EX-01 Worker changes against BD110, the complete EX-01 card and fourteen deliverables. This is a source-only verdict, not an item, installed-definition, screenshot or runtime acceptance PASS.

Re-read of actual repairs: NEXT wrong-code uses a maximum10-iteration compare-and-set with fresh native counts and safely closes unresolved contention. Correct-code claim requires the exact prior count and configured limit, alongside unused/owner/purpose/expiry. Returned UUID compares canonical lowercase. The financial8s timer now uses the actual attempted flag. START requires the displayed expected_scope_sha256, compares it with an independently computed native hash and stores/signs only the computed hash; changed scope refuses instead of silently authorising another amount. BD110's latest concrete supplement also explicitly records protected tokenless historical mutation recovery as refused, with exact issued-CSV retrieval separately exempt.

**W03 parser closure independently re-read:** readBpayNextReauthJson now catches only its bounded parser failures, retains their safe code/status/retryable values and reconstructs the fault with outcomeUnknown=false. Thus malformed/oversized/invalid-UTF8 proof responses cannot claim an unattempted financial outcome is unknown. The direct financial response still calls the original limitedJson after attempted=true and retains its true uncertainty. The8s timer separately uses attempted. Original findings and pre-repair snapshot below are retained as history; all are now closed. No runtime request was made.

No product edits, tests, fixtures, database/hosted/auth requests, email, payment, installation, commit or push were performed. Only this review file was written. All joined/security/app scenarios remain **NOT_RUN**.

## Final reviewed source bytes

W01/W02 closures, W03 timer/parser closure and START display-concurrency changes were independently re-read at these saved bytes. Other named Worker files retain their previously reviewed hashes. No product changes were written by the reviewer.

| File | SHA-256 |
| --- | --- |
| broker/src/index.js | bd4116b59f2c01383ae1a047da233731111e8ac2b15a82656e32eee221635b64 |
| broker/src/bpay-next-command.js | 3edf996af78ae7d6f5c6b775eccf1add0cce92f3c072281b3f77137e969e3ed0 |
| broker/src/bpay-next-command-rpc.js | d692d96b5cb667c31c4d08ba1d789c9aa74351965c319c6cf594c12f5f3bdf6a |
| broker/src/bpay-next-legacy-fence.js | 9572aaee4287ca1594cd72a3323b6cb805c133e8b4e09f31f860922922033e9d |
| docs/bpay-next/design/EX-01-design.md | 6653da2775aa4df7fec86812e833ac96458865939a5cc3c3ce2e6234fa398653 |

## Original findings saved immediately (historical)

### W01 — NEXT conditional challenge claim does not retain the attempt-limit condition

`broker/src/index.js:138625–138665`. VERIFY reads attempt_count and refuses at the configured limit, then awaits code hashing. The new used=false PATCH checks user/purpose/unused/expiry but not attempt_count. A correct request that read max−1 can therefore mint after a concurrent wrong attempt has raised the challenge to max.

Include the native attempt-limit/current-attempt condition in the atomic NEXT claim. Preserve the existing wrong-code refusal and bounded attempt policy; do not treat used=false alone as complete challenge currentness. The current absolute attempts+1 wrong-code update also has a pre-existing lost-update race under concurrent wrong requests; any narrow NEXT adjustment must avoid advertising a stronger attempt-count guarantee while retaining that race. No such concurrent execution was run here.

### W02 — accepted uppercase UUID consumes its code then returns a refusal

`broker/src/index.js:138545–138549,138659`. The challenge UUID regex is case-insensitive, but after successful conditional PATCH the code compares claimed[0].id !== challengeId. PostgreSQL returns canonical lowercase UUID text. A valid uppercase UUID request claims the code and then receives invalid_or_expired_code instead of its token.

Compare the UUID semantically (canonical lowercase), retaining the one-row/identity claim checks. Rejecting that input before mutation would also be honest, but the current route explicitly admits it.

### W03 — timeout marks unknown financial outcome before financial dispatch

`broker/src/bpay-next-command.js:363–413`. The 8,000ms timer unconditionally constructs DEPENDENCY_TIMEOUT with outcome_unknown=true. EX-01 now awaits signature verification, audit proof record and possible policy read before setting attempted=true at the financial RPC. A timeout during those stages sets an unknown financial journal even though no financial RPC was sent and the aborted-signal guard prevents one.

Use the actual attempted flag in the timeout fault. Keep financial uncertainty true after dispatch. The idempotent proof audit record may have an unknown audit receipt; that does not make an unattempted financial action's outcome unknown. The existing proof-record retry retains the same token SHA and proof identity.

## Source findings and integration assessment

- **Imports/routes:** index imports the actual preview handler/qualifiers/protected registry and fixed RPC adapter. Only the exact POST preview is added to NEXT Office dispatch; it authenticates current user/admin and uses the existing environment check. Exact POST START/VERIFY fence exemptions are explicit; no family-wide financial exemption was introduced.
- **Purpose/security:** BPAY_NEXT_ACTION is finite and distinct from the three existing purposes. START requires its exact three-key body, uses current password verification, server scope, current admin and SQL NEXT qualification. Its NEXT branch cannot enter any of the four old TEST second-factor bypass branches. Genuine other purposes retain their existing behavior.
- **Real policy:** settings.importConfig.auth_effective.tfa_enabled must be a boolean. Configured false issues a normal signed password proof; unknown fails closed. For an unconsumed false-factor proof, the command rereads current policy and refuses if code is now required. Consumed exact replay does not pretend to be a new first use.
- **Challenge provenance:** START writes scope/action obtained from the finite server preview. VERIFY reads challenge action/hash and signs those stored facts, not values from the request. The NEXT body excludes action/hash overrides. It retains subject/IP/code/expiry/attempt checks and uses a conditional unused-row PATCH so two ordinary concurrent correct claims cannot both mint. W01/W02 still prevent final approval of that claim.
- **Signature/closed schema:** verifier requires the native two-part HMAC token, restricted base64url syntax, existing signing secret, exact eight claims, reauth type/NEXT purpose, current actor/action, lowercase SHA-256, boolean factor provenance and safe integer times. Lifetime is at most300s and future issue tolerance5s. Extra claims/old bypass tokens cannot become NEXT action proofs. Expiry enforcement is deliberately delegated to SQL's unused-versus-consumed receipt qualification; expiry is not globally waived.
- **Proof record:** Worker hashes the exact signed token and passes only seven verified metadata fields to the fixed service record RPC. No password/code/raw token is sent to SQL. The two-field record result is strictly checked, and a consumed identity must match the actual command/instruction UUID. The read-only integration check of1302 confirms unique-token idempotent record behavior, exact metadata equality and stale unused-proof refusal.
- **Financial intake:** only proof UUID is appended to typed native args. Integration reading1306 confirms the six actual protected branches call the locked finite native resolver/consume before original acceptance; missing proof refuses except exact already-issued CSV readback. The browser never selects a proof UUID or arbitrary RPC.
- **Exact replay:** returned command identity is now exact for protected CSV_RETURN; unrelated CSV_SETTLEMENT retains native receipt-based compatibility. Consumed token replay keeps the same recorded proof and native command/instruction; SQL/native receipts qualify actor/action/operands/scope after legitimate state changes. Proof record alone does not authorise a new UUID. Whole SQL/money-owner approval belongs to its separate source reviewer.
- **Bounds:** actual financial and preview bodies retain2,048 encoded bytes before parsing; command response/record/preview RPC bodies use128KiB bounds; command and preview each retain8,000ms timers/abort signals. Token is additionally limited to1,536 bytes and must fit the whole command envelope. Fixed RPC wrappers use original actor, POST, service headers, no redirects and explicit native RPC names. No caller-selected SQL, command kind, drain or provider step.
- **Permissions/wake:** current requireUser rereads active user/session version; Office routes remain admin-only. SQL record/preview/intake retain service/NEXT/admin gates and native authoriser flags. Accepted command wake remains exactly original ENROLL; accepted jobs do not authenticate per page.
- **Error mapping:** exact REAUTH_REQUIRED and REAUTH_STALE map403 before generic42501/55000 handling. Invalid signature/shape and missing protected token cannot reach financial acceptance.
- **Future registry:** fifteen names are registered, but current command/preview schemas remain restricted to actually implemented actions. Later owners must add their real scope/caller/acceptance wiring; enum membership is not proof that those actions exist.

Historical tokenless protected NEXT requests are readable as old journal records but current Worker/SQL require a token for every protected action except issued-CSV retrieval. Thus this source does not provide tokenless original cancellation/reissue receipt recovery; it must not be reported as such. No actual historical row/journal was observed or assumed, and this review does not request an old Banking compatibility engine.

## Reviewed byte identities

Snapshot before root repairs W01–W03; saved disk is authoritative. Hashes identify inspected source, not deployment.

| File | SHA-256 |
| --- | --- |
| broker/src/index.js | 3427adcbd5cdc17208a5e1390506294a664f9b70a574c70da2a7c6a5e8debe8e |
| broker/src/bpay-next-command.js | b0a5be51444d6189a6d53ea0743283b17501546e99e87c1769f0409411b52ac5 |
| broker/src/bpay-next-command-rpc.js | d692d96b5cb667c31c4d08ba1d789c9aa74351965c319c6cf594c12f5f3bdf6a |
| broker/src/bpay-next-legacy-fence.js | 9572aaee4287ca1594cd72a3323b6cb805c133e8b4e09f31f860922922033e9d |
| supabase/repeatable/11102026_1302_banking_pay_next_reauth_proof_v1.sql (integration only) | 4e01bfb59bd7d414a79e12822649a06412aee0d3313199539fef9fce944ffce8 |
| supabase/repeatable/11102026_1306_banking_pay_next_reauth_intake_v1.sql (integration only) | 6c94fe2563f53edbfc77f61529ff8172215844c07f9ad3ca812abfe7623de3d5 |

No A4 or installed/joined acceptance is implied by this final source PASS. Actual saved repairs and current hashes are recorded at the top; future joined/security/app acceptance remains NOT_RUN.
