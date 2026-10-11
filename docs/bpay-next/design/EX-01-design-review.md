# EX-01 independent design review

11 October 2026. Sequence **98**. **DESIGN PASS — no essential design finding remains against BD110 and its concrete binding choices.** This approves the proposed scope and source approach only. Product implementation, installed definitions, security behavior, A4 pixels and connected app acceptance have not been approved by this receipt.

The full card `04_EXECUTION_AND_OUTCOMES.md:102–128`, all fourteen checklist rows `IMPLEMENTATION_CHECKLIST.md:2709–2734`, sequence/dependencies, OR-45 and both source investigations were read. Current intake and the original CSV issue, cash reissue, reissue builder, reissue cancellation and bank-return bodies were independently inspected. Supplied B6 SCR-66 password desktop/phone, SCR-66b wrong-code and SCR-66c scheduled-reason PNGs and their specs were read directly from the ordinary design pack; the pack was unchanged.

No product, checklist or pack edits; no database, test, fixture, render, hosted request, email, payment, installation, commit or push was performed. **All joined/runtime acceptance remains NOT_RUN.** The only new file written by this reviewer is this receipt.

## Complete delivery boundary

| Checklist row | Design assessment |
| --- | --- |
| D01 | Named private proof table and exact challenge CHECK/nullable action+scope additions; unique token SHA and nullable unique consumed native identity, finite actions/hashes, genuine time bounds, actor FK, private ACL and service-owner RLS. No raw credentials/token in SQL. |
| D02 | Service-only proof record; Worker-verified closed metadata; identical record retry recovers the same proof without changing identity, scope or times. |
| D03 | Atomic private consume, first use versus exact consumed original replay; required/stale refusal codes retained. |
| D04 | Exact POST START NEXT branch, active current admin/owner qualification, original password and genuine policy, server-computed scope and stored challenge action/hash. |
| D05 | Exact POST VERIFY NEXT branch; stored challenge facts supply claims, retained attempt/IP/expiry checks and conditional unused-challenge claim. |
| D06 | Existing typed transport retained: 2,048 request bytes, 128 KiB result and 8,000 ms deadline. No extra command/job kind. |
| D07 | Strict protected token/proof binding, original typed arguments, fixed dependency/error mapping, native receipt identity and current authoriser checks. |
| D08 | Native Office password/code helper and progress variant, server intro, current Cancel and first CSV issue callers, original parent restoration and same-byte pending journal. Actual A4 remains pending. |
| D09 | Existing START handler's narrow NEXT branch; genuine shared login/security purposes remain intact. OR-45 does not require operating the old Banking engine. |
| D10 | Existing VERIFY handler's narrow NEXT branch, with no TEST bypass or browser-authoritative action/scope. |
| D11 | Exact existing POST START exemption only; no route-family exemption. |
| D12 | Exact existing POST VERIFY exemption only; no route-family exemption. |
| D13 | Current command module/adapter verifier, record dependency and proof UUID only in SQL args; original bounds and admin guard. |
| D14 | Full fixed protected registry plus actual consume wiring for all six currently installed native money branches. Later action owners must add their real native scope/caller/acceptance seam; strings alone do not complete those future actions. |

The thin service-only preview RPC and bounded admin/NEXT POST are necessary support for D08/R4: no existing NEXT reader owns this finite action preview. One shared private resolver must supply both preview/START scope and locked intake qualification. This provides the card's actual action, Candidate identity/count and exact native amount without a browser money/hash authority. It does not add an execution subsystem, financial command, new calculator, scope storage, queue, trigger or generic proof framework. New public-definer catalogue/contract/ACL evidence belongs to the implementation, not to this design receipt.

The complete protected set is fixed by the card. The six current branches are WRITE_OFF, CANCEL_REQUEST, CASH_REISSUE, REISSUE_CANCEL, CSV_ISSUE and CSV_RETURN. Future execution/approval/file/disposition/whole-selection/release/return/outcome-review and DR-10 override owners must consume this seam in their own chain order. Outcome-only settlement/refused/unknown/internal recording retains its native authoriser gates without proof. Preparation removal/discard is outside this set.

## Native amount, locking and replay decisions

**First use:** validate the closed typed intent, acquire each action's existing command/root/Candidate/worker/Case/transfer/cash/bank locks in its native order, then recompute the shared finite scope and lock/consume the proof before unchanged native acceptance. Preview is a bounded read, not a writer. A scope SELECT before the conflicting native locks would leave a race and would not satisfy this design. Proof record is an independent audit-only transaction and does not wait on business locks; first-use intake has business locks before the proof-row lock. Re-entering the unchanged native owner under those same locks must preserve its original order.

**Accepted replay:** qualify the exact original native receipt, actor, action, original UUID and every retained intent operand first. Require the same stored consumed proof/token hash/scope and native identity. Current successful state/balance/bank changes must not force a new scope or authentication. Expiry is ignored only for this exact already-consumed receipt; recorded-but-unconsumed and unused expired proofs cannot first authorise money. Current role/payment-authoriser/module gates still apply. Missing proof is 42501/HTTP403 BPAY_NEXT_REAUTH_REQUIRED; Build's explicit 55000 BPAY_NEXT_REAUTH_STALE remains a distinct HTTP403 code ahead of generic error maps.

| Action | Required native binding and replay boundary |
| --- | --- |
| WRITE_OFF | Existing qualified collection/Case/component, Candidate, revisions and original principal/recovered/written-off/hold basis. Preserve the acceptor's raw amount/reason/scope equality and native ALL whole-liability refusal; no clamp or second W calculator. Native command mutex → Candidate → Case → component order. |
| CANCEL_REQUEST | Existing worker/run/frozen channel, preparation/selection revision and current accepted projection when present. Pending NET/build remains cancellable: intro explicitly labels frozen Draft gross being removed and only an actual projection is bank cash. Never turn NULL cash into zero. Optional run/channel resolves to the same immutable address. Simple run → Candidate → worker; Case branch first retains its native command mutex. Because the native request lacks actor, the consumed EX-01 proof supplies original actor evidence. Pre-EX-01 receipts cannot be fabricated/backfilled as new proof-bound actions. |
| CASH_REISSUE | Native return-cash/original transfer/worker/run/Candidate/beneficiary/currency and O-H-P whole availability, original complete return and pending-request exclusion. Current source lifecycle and unique pending request support the unchanged deferred whole-pool builder; the design explicitly refuses unsupported positive partial-pool scope. Replays use the retained request and consumed scope, since this native request stores no accepted amount/revision. Native accept run → worker → actor → cash, not the job builder's lease/order. |
| REISSUE_CANCEL | Exact existing reissue transfer/member/pool identities and whole immutable cash contribution. Add original actor/kind/epoch qualification to its transfer-only native replay; later apply still owns releasing the captured reservation. Native run → Candidate → worker → transfer → actor. |
| CSV_ISSUE | Instruction UUID is the original identity, not a command-table row: no consumed-ID FK to bpay_next_command. First issue binds actual native Candidate/Umbrella/ONEOFF bank authority/version/hash and immutable cash under the existing locks before destination mutation. Pure preview must not invoke the one-off locking/writing helper. Add original issuer/instruction equality to native transfer-only replay. Same issued instruction retrieves original bytes and skips rebind; a different UUID is not a new authorised issue. Native run → worker → transfer → actor → bank facts. |
| CSV_RETURN | Native immutable transfer cash, original settlement, exact receipt, typed time and original command/actor/worker/Candidate/epoch. Independently confirmed that native0230 deduplicates by receipt and returns an older command without comparing the submitted UUID/actor; the planned intake-only prequalification is essential. Preserve numeric/timestamptz equality and exact receipt string, while the journal retries identical serialized bytes. Native run → Candidate → worker → transfer → actor. |

The whole-pool conclusion is specifically the currently registered native whole-return/reissue/reserve/settle/cancel lifecycle and its source fences, not an assertion about arbitrary manually edited/imported rows satisfying the arithmetic CHECK. No new retained amount column or builder rewrite is needed for that supported scope. Future partial writers must revisit it.

The current Worker permits CSV_RETURN's original different-command receipt compatibility alongside CSV_SETTLEMENT. The EX-01 successor must enforce the protected return's exact original UUID while retaining the unrelated outcome branch; it must not treat this older response allowance as proof qualification.

## Policy, token and journal completeness

BD110 resolves the card's password-only wording consistently with its explicit Rules/R4: genuinely configured code-disabled policy can produce second_factor_used=false after password. Only the later scheduled CANCEL_SELECTION exception can skip a code-enabled policy. Unknown policy fails closed; neither path extends the old TEST CSV bypass. First use must recheck genuine policy so a still-unused password-only token issued before policy becomes code-required cannot authorise a new action. Accepted exact replay remains receipt recovery, with current permission gates, not a fresh password-only authorisation.

VERIFY must atomically claim the same stored challenge; body-supplied action/hash/second-factor provenance is not authoritative. Record is idempotent only for identical signed metadata/token SHA. Token signature/type/purpose/subject/action/scope/times are closed and bounded; stale scope is not repaired by rewriting proof. Both token and optional proof fields are restricted to protected schemas. No token/password/code reaches SQL; no credential enters the pending command journal.

Journal the original typed command UUID/instruction, all operands and token before dispatch. Unknown outcome offers only identical bytes; no new UUID, new token, re-scope or live recalculation. Historical tokenless pending bytes remain readable for known original receipt recovery, never silently become a newly authorised mutation. A token that fits its own limit must also fit the existing complete 2,048-byte command envelope; increasing that envelope is outside this design.

## Required screen comparison and honest remaining acceptance

Supplied B6 designs establish the compact native Confirm it is you dialog, factual subject followed by password or email code, inline field error, Send a new code and action-specific footer. SCR-66c is the later scheduled cancellation password/reason/safe-keep variant; it does not add a reason or scheduled capability to current CANCEL_REQUEST. Current single-Candidate cancel/CSV issue differs from the pictured future 147-payment execution by its actual installed action and server factual scope. No invented batch reference/pay date, company, future execution table, projected bank cash or unnecessary controls may be copied from the design.

For current actual captures inspect password, code, wrong-code/expired/refused, genuine configured no-code, busy, known refusal, unknown identical retry/recovered receipt and progress-variant parent restoration in dark/device-light/390px phone. Record every visible difference against SCR-66/66b and only use SCR-66c where its own later scheduled action is provided. Check close/credential cleanup, real Candidate identity and amount basis, native focus/disabled reason, destructive-left/safe default where applicable and 44px phone controls. All42 policy rows, manual policy.json wording, owner25/readerAdditions26 N/A and each control's payroll necessity must be reported from actual pixels/source. No pixel pass is implied now.

Expired first use, wrong actor/action/scope/UUID/operands, reused token for another action, challenge race, revoked permission, wrong code, protected bank return and lost-reply exact accepted replay are required future joined acceptance, **NOT_RUN**. Source/compile/ACL/gates and local fixed-response captures can close their own implementation evidence only.

## Reviewed bytes

SHA-256 of saved evidence at this review; hashes identify read source, not deployment.

| Path (BE unless PLAN prefix) | SHA-256 |
| --- | --- |
| docs/bpay-next/design/EX-01-design.md | 84281490155a52dec8f71276d1e39d9a6776a0eb364f9e2042685a5df3313175 |
| docs/bpay-next/design/EX-01-current-investigation.md | e913494c558c5005940445e0e1a34a3855071cdef5094575571f9204630e103a |
| docs/bpay-next/design/EX-01-scope-replay-investigation.md | 3dda4a8203bd4a41949b8dfa6b3f7cabccdf9327453b79f6d0ec4d9422009596 |
| docs/bpay-next/final-definer-closures.json | e30ee55274ed2d00a1b33c0892e199d036ef8ad4e3e16b17b0926da4672aa8b0 |
| supabase/repeatable/10102026_2150_banking_pay_next_workbench_retry_v1.sql | 34b1354dbf7e3a142ed597fd25283a6076254c6b28718239edf0bc8ba38ba293 |
| supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql | 98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6 |
| supabase/repeatable/04102026_0230_banking_pay_next_simple_csv_return_v1.sql | 0b4085def3cfcc9c6b8f7d1aae05716dea85578bd6433ed28f70038f98cc145d |
| supabase/repeatable/04102026_0355_banking_pay_next_reissue_cancel_v1.sql | 3f31e48aef1629bac6dea37cd2a40a1e2dd0b5806031971b0c5e8334f4873c55 |
| supabase/repeatable/04102026_0240_banking_pay_next_simple_cash_reissue_v1.sql | 5f8365702daf773fae696a4c0861892f5fd19be701c517a00bbbbee07e50e714 |
| broker/src/bpay-next-command.js | 95f082f6fef61932b0498ea007d47966692ec15094d954fc9cbbf4bd1d7ed059 |
| PLAN/04_EXECUTION_AND_OUTCOMES.md | 61a2a6d0b85f60d3efd450928c3200d4c94e17fc212109f23628d5beb9369f6a |
| PLAN/IMPLEMENTATION_CHECKLIST.md | 81be0f2d348fa5a9ad37c11d35e6e40a4bbb3731ff9e7c7abd7099d25c24722e |
| PLAN/08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md | 64db4b6328c01336213f02da1caca0889db119c0fa06b803abbda0756c30fc14 |
| PLAN/10_OWNER_COMMENTS_THIS_CONVERSATION.md | 1ba82b320169c871e346ac1c5b01c96cf99a19143e42e39aadda31aabe5b5c35 |
