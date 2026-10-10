# SP-06 independent Source/Worker review

Review date: 10 October 2026. Scope: SP-06 D01–D03, the six existing protected actions, final Source-local/NEXT receipt ownership, full retained preview, and the narrow 0821 WAIT permission / protected-route adapter changes. Read-only source review; no SQL, tests, runtime, business data, hosted calls, fixtures or secret access. D04/D05 UI review belongs to the separate caller review. All joined/runtime acceptance remains NOT_RUN.

## Verdict

**Final D01/D02/D03 source PASS. Both finite error-adapter findings below are closed by the fresh source re-review.** The Source-local/NEXT orchestration and full preview remain preserved. The permission gap in the actual WAIT transaction is closed. This is not an executed release/catalogue verification or a whole SP-06 completion claim.

## Reviewed saved source hashes

| File | SHA-256 |
| --- | --- |
| `supabase/repeatable/10102026_0821_banking_pay_next_protected_wait_permission_v1.sql` | `47f7f1befc74f5e5fc8e891d1e8b5b199ac3467ff047ef4555226be89a96f6a0` |
| `broker/src/weekly-source/routes.js` | `3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11` |
| `broker/src/weekly-source/protected-action-orchestrator.mjs` | `5352ef8bcc66e87b090d5a1d4f40ba872cf7a542d38f60aaf690ec1eea80f3c3` |
| `broker/src/weekly-source/protected-next-preview.mjs` | `49ed6b5e24c1ebf6afd9dbada7de0d9d321c8297584491610353c1ee2a2bfc4a` |
| `docs/bpay-next/final-definer-closures.json` | `88c03a24ed35aef5539614543446c876f6ca514a98d83cc50a64a77d68efb28d` |

## Initial findings and final closures

### SP06-R01 — Bind definite refusal to its actual structured database signature — CLOSED

`routes.js:966–974` currently finds a known token in either `error.code` or `error.json.message`, without requiring the corresponding SQLSTATE or an actual error HTTP status. An error carrying `{json:{code:'OTHER',message:'WEEKLY_PROTECTED_ACTION_STALE'}}` is therefore labelled definite `retryable:false` / HTTP 409. A malformed or otherwise unknown transport result must not manufacture the definite-stale outcome that permits the caller to abandon the accepted-key recovery path.

Use only the existing actual structured database pairs, with an integer HTTP error status (400–599), a non-array object `error.json`, and exact comparisons: `PT409 / WEEKLY_PROTECTED_ACTION_STALE`, `55000 / BPAY_NEXT_PROTECTED_LOCAL_OWNER_REQUIRED`, and `42501 / WEEKLY_SOURCE_OFFICE_ACTOR_INACTIVE`, `WEEKLY_SOURCE_OFFICE_ADMIN_REQUIRED`, or `WEEKLY_SOURCE_PAYMENT_AUTHORISER_REQUIRED`. `LOCAL_OWNER_REQUIRED` is also an actual `WeeklyProtectedActionError`; classify that code through the class-bound branch, not an arbitrary object's free-standing `code`. Preserve the generic fallback for unrecognised transport outcomes. No generic money-RPC adapter change is needed.

Evidence: original action SQL `15092026_1534_weekly_source_protected_action_orchestration_v1.sql:209`; current NEXT SQL `04102026_0345_banking_pay_next_protected_source_v1.sql:552,615`; actual Office authority `15092026_1534_00_weekly_source_private_classifiers_v1.sql:166,171,181`; `broker/src/bpay-busy.js:36–47` already demonstrates the finite structured/status recogniser used for its own two old busy refusals.

### SP06-R02 — Preserve the actual fresh Source-admission busy refusal — CLOSED

Fresh WAIT calls `private.weekly_source_pay_query_admit_v2()` at `0821:83`. That existing helper raises exactly `55P03 / WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` (`05102026_0044_bpay_next_source_pay_query_admission_v2.sql:18–19`). The adapter calls `bpayBusyEnvelope`, whose intentional finite allowlist contains only `BPAY_SOURCE_FAMILY_BUSY` and `BPAY_APPROVAL_EVIDENCE_BUSY`. This real Source-admission refusal consequently falls through to the generic route error, losing the required explicit code/manual-retryability at the named protected seam.

Add recognition of this one exact structured status/SQLSTATE/message tuple inside the protected-only adapter. Return its actual code with `retryable:true`, `automatic_retry:false`, and the existing honest distinction between the rolled-back failed RPC and any earlier separately committed preparation. Do not broaden the common busy recogniser to arbitrary `55P03`, and do not claim that the entire multi-request action was rolled back.

## D01 — Existing database owner and WAIT closure

The 0821 full `public.weekly_exceptional_pay_wait_atomic_v1(jsonb)` CREATE body equals the original 1509 definition after removal of the sole new authority comment/call. The call at line 68 uses the genuine two-argument `weekly_source_office_authority_v1(v_actor,'APPROVE_PROTECTED_PAY')`, whose optional scope arguments default to NULL. It rechecks actual active/admin/payment-authoriser authority for fresh work and completed replay after typed request parsing, before admission/business locks or any audit insertion. No synthetic scope or browser authority is introduced.

Original fresh admission, family/run lock order, immutable completed no-write replay, actual expected version/vector checks, retained approval/generation/event identities, WAIT event/state changes, query retargeting, and the final reply are preserved. The fresh WAIT receipt has six keys (`ok`, `outcome`, `idempotent_replay`, `family_id`, `family_bound_version`, `target_vector_sha256`); the original completed replay has the same first five and intentionally omits the hash. WAIT still returns before the financial calculator and neither fabricates a zero payment nor prepares a financial root.

The registry adds only the actual WAIT chain: original 1509 predecessor, final 0821 physical/root/closure. Owner `postgres`, SECURITY DEFINER, original fixed search path, PUBLIC/anon/authenticated revoke and service-role grant are retained. This replaces an existing routine; it adds no public signature or security-definer membership. The review does not execute or substitute the root's protected membership verifier.

The other five action paths continue to use their genuine current owner authorities: action prepare/context, mapped NEXT publication/replay, and Source-local completion/status. No current floor, Rule-B cut, Source producer, local HEAD publisher, or caller-supplied financial facts were changed.

## D02/D03 — Preserved orchestration and preview

The full six-action orchestration keeps exact request allowlists and server actor assignment; nested browser financial facts are refused in `routes.js:573`. The actual Source-local completed/pending receipt is returned as its flat owner result. Its published flag describes actual mapping; it does not claim PositionsApply/Book completion. Genuine unauthorised TSFIN save preserves PENDING_AUTH and first-authorisation requirements. A NULL generation never proves the NEXT route.

The existing four-key SQL owner-status envelope is decoded for status/routing only. Direct NEXT validates the complete fourteen-key native publication receipt and retained sealed revision/command/work identity. A saved local result is validated before routing, and local ownership refusal is preserved. The historical C1 route remains Source-local; there is no external C1 call/retry gate. Unknown receipt/context loss remains eligible for literal same-key recovery through the existing `WeeklyProtectedActionError` branch.

The retained complete target preserves original component identity, WITHDRAW/ACCEPT_SOURCE lineage, actual schedules/breaks/expenses, retained Source vectors and exact decimal money. Preview is capped and typed; it does not reprice imported Source money, generate fake components or substitute request hashes for genuine Source identities. WAIT bypasses preview/financial calculation at the original early return. The raw result is returned directly, without a second Worker protocol or financial summary.

Detailed exact seams and receipt key inventories are in `design/SP-06-worker-census.md`. The card's reuse assumptions are qualified only by the demonstrated WAIT authority gap and the finite protected error transport correction; no unnamed producer rewiring or new protocol is proposed.

## Final fresh-source re-review

Final `routes.js` SHA-256 is `3641498765ed87d9d2c450e07cfc3dca05a9601725976d2e82e315f40dde4b11`. At lines 958–972 the definite-refusal table binds the five actual database messages to their exact SQLSTATE, and `LOCAL_OWNER_REQUIRED` is recognised only on the genuine `WeeklyProtectedActionError` class. The final input fence at 965–967 also requires an integer HTTP error status in 400–599 and a non-array structured object. Wrong SQLSTATE, arbitrary free-standing code, malformed/non-error transport and unknown message no longer enter that definite branch. At lines 979–984 only the actual `55P03 / WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY` tuple obtains the typed manual-retry response. It explicitly identifies only the failed RPC as rolled back, disables automatic retry, and marks the whole workflow `NOT_ESTABLISHED`; prior preparation is not claimed undone. The two existing Banking Pay busy refusals continue through their original finite recogniser, with manual retryability added only at this protected seam.

The concrete structured SQL error creator in `broker/src/index.js:165012–165027` already runs only under `!res.ok` and preserves the response status and parsed `json` on the error. Re-reading this full call path establishes the existing error-status boundary. The final root correction adds the same finite status/type fence directly to this small helper; it does not introduce a new transport framework.

The only routes diff is the class import, protected-action try/catch, and finite helper. Other Source route error adapters and generic monetary RPC handling are preserved. 0821's hash is unchanged. The final registry diff against HEAD is exactly the seventeen-line WAIT chain addition, with original object order retained and no other metadata change. No open concrete D01–D03 source finding remains. Root-reported normal checks are not executed or independently re-certified by this review; joined/runtime acceptance stays NOT_RUN.
