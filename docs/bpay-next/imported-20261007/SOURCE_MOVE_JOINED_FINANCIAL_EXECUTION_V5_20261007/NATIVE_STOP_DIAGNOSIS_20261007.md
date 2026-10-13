# V5 local financial run — observed stop

7 October 2026. Implementation evidence, not a Claude-method review or adoption.

## Observed result

One root-owned run: terminal session 73728, exit 1 (tool 34ee9d). Its immutable driver/ACTUAL_FINANCIAL_EXECUTION.json is SHA-256 23CD4F26EC22D2F52604D8FC679202B65742CE7C3CC1BC55074B2E0F1FB937D4 (root 55f641).

The candidate compiled 53 units and read back 186 routines, overlay B2A9A14155A537F8B1D8C99E718285A8A2F5CB320F798799B0836B750D8CFD66. The financial journey stopped at C2_ACTUAL_QUEUE_REFUSAL. financialResult is null and finalGatesComplete is false. Twelve declared vectors are not twelve completed proofs.

The rollback command was not acknowledged: rollbackSent=false; cleanupFailure=ROLLBACK/P0001/PROCESS_EXIT. This must not be called a clean acknowledged rollback. After connection closure, the driver's fresh full Target-plus-Source snapshot matched BEFORE exactly: 75FF32BBA993F0956C2C4D26D3402FCE40E81BD34CCA0E38B930DBE24C328676. restorationFailure is null. Root b7b710 then confirmed the restricted owner and zero other sessions. No permanent installation, release-ledger write, hosted change, provider call or external effect occurred.

## Exact underlying database error

Root e2d8cb used a read-only, bounded Docker log query (last 180 entries, last 15 minutes, only ERROR/CONTEXT/function lines). The local server recorded at 10:43:01.769 UTC, PID 38894:

    BPAY_NEXT_COMPARISON_LOCAL_NATIVE_BASIS_UNQUALIFIED
    bpay_next_source_comparison_carry_v2(uuid,bigint,uuid,text,uuid), line 46 at RAISE
    bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer), line 269 at PERFORM
    bpay_next_financial_job_v1(text,uuid,text,jsonb), line 358 at assignment

No rejected operands or exact vector were retained. Their values and the number of completed preceding vectors must not be invented. The V5 assertion correction advanced the run beyond the earlier projection-assertion stop, but that is not an award of its full acceptance criterion.

## Source evidence and next work

Root 23aa13/8ec56f/85dc22 inspected the saved authority under SOURCE_MOVE_JOINED_UM_MATERIAL_V1_20261007/managed-inputs/supabase/repeatable:

- 07102026_0241_source_comparison_review_credit_carry_v2.sql: carry captures the local scalar and refuses when updates_local_scalar is true but localp.source_basis_channel differs from op.native_channel (lines 452–455). Its later writes separately update native channel books and local scalar columns.
- 07102026_0157_bpay_next_source_comparison_full_v2.sql: the hold operation gets native_channel from the frozen run_line.source_pay_channel and sets updates_local_scalar=true (lines 454–455). This is source evidence, not proof of the rejected operand values.
- 03102026_1620_banking_pay_next_fresh_positive_hold_v1.sql: qualified captured comparison is checked before the real RESERVE call; run_line records updates_original_source_scalar based on canonical/local identity (lines 259–271). Do not remove the comparison guard to force this run through.

Sol 6.1 Extra High agent source_reservation_basis_diagnosis is investigating the exact native-book versus historical-local-scalar distinction and the smallest safe correction. It has read-only/source-only scope. No retry, new candidate or product edit is authorised by this report alone. A proposed correction must preserve frozen historical payments, shared valuation, receipt replay, Case/allocation arithmetic, locks/currentness/CAS and security, and be independently checked before the next root native attempt.

## Methodology and scores

The agent previously named claude_method_r12_critic had been reused only for the V5 assertion/binding review; it completed that review with seal A0093BD77B8FD421627153809444CF69B6F1FDB71FA5F0C83EC09DA860528470. It was not repeating the Claude-method audit. Claude's methodology response is still awaited; no proposed method was adopted.

No programme task is newly DONE: 96/1,040 mandatory; Stage 2 81/297. S2-MOV17 and the narrow NET/dispatch proofs remain open until their literal acceptance conditions finish. V4 and all older results remain unchanged. The independent unpaid-zero diagnostic packet remains source-only/HELD, not a payment or REMOVED-guard acceptance.
