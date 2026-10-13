# PF-04-D03 cleanup minute-lane source review

10 October 2026. **PASS for the saved Worker cleanup transport and fifth independent NEXT minute-recovery lane. No actionable D03 source defect found.** Independent source-only review; no tests, SQL, runtime, fixtures, network/hosted operations or product edits. This verdict does not clear the separate D01 SQL deletion-eligibility review or claim full PF-04/application completion.

Reviewed the actual PF-04 card/five-row scope, current changes to `bpay-next-runtime-rpc.js` and `bpay-next-background.js`, the existing bounded transport/read/error machinery, the latest ordinary runtime/background units including the added cleanup assertions, the unchanged index minute-entry owner gate, and the exact SQL service signature/role/NEXT/receipt boundary.

## Scope and wiring

- The fixed runtime RPC map adds only CLEANUP → `bpay_next_terminal_cleanup_service_v1`. The exported adapter accepts exactly `{limit}` with integer 1–100 and sends exactly `{p_limit}`. No caller cursor, lane selection, clock, actor, financial payload, command kind or arbitrary endpoint is admitted. Its exact two-argument interface uses the existing injected raw service transport.
- The transport retains service-role header shape validation, fixed POST endpoint, redirect refusal, forwarded AbortSignal, request byte cap and original route class. SQL independently checks actual service JWT role and holds the singleton NEXT ownership check FOR SHARE before its exact private cleanup call. Private direct EXECUTE is revoked; only the public wrapper is granted to service_role. Local JWT parsing remains shape validation rather than authentication.
- `runBpayNextRecoveryTick` now awaits ENROLL, READY, LEASE_EXPIRED, EXPIRY, then one CLEANUP page with limit 100. The existing per-lane catch/result handling lets every later lane run despite an earlier failure. The cleanup lane publishes zero messages, performs no inline financial step or recursive drain, and cannot alter queue ACK/retry or continuation envelopes.
- The actual scheduled index entry still checks existing minute-cron eligibility and obtains fresh module owner status before calling this recovery tick; LEGACY/DISABLED skip it. SQL supplies the second authoritative NEXT fence. No index rewiring, cron/config/binding change, financial owner replacement, old engine compatibility path or Office route is introduced by this batch.

## Closed bounded result and uncertainty

The cleanup decoder permits exactly `lane`, `rows_visited`, `rows_deleted`, `rows_retained`, `busy`. All three counters must be canonical nonnegative decimal strings, within the requested page limit, and deleted + retained must equal visited. A nonbusy result has only SELECTION or JOB lane; busy requires NULL lane and zero visited, with the sum/nonnegative constraints also forcing both other counters to zero. Numeric JSON counters, extra fields, invented lanes or inconsistent counts fail closed. The actual SQL JSON fields/types and service parameter match this contract.

Cleanup reuses the unchanged 1,024-byte request / 32,768-byte response / 8-second deadline machinery: actual streamed bytes are bounded before JSON parsing, invalid UTF-8/duplicate JSON keys/redirect or non-JSON/non-200 success is refused, and abandoned or late bodies are cancelled. Validated receipts are frozen. Definite SQL errors preserve the existing sanitized typed error mapping; lost/malformed/timeout replies preserve unknown outcome and `WAIT_FOR_SWEEP_RECOVERY`. There is one attempted RPC and no automatic repeat. A later existing minute tick revisits durable SQL-owned cleanup state; the Worker neither guesses deletion success nor replaces the cursor.

## Ordinary source units and remaining boundary

Latest unit source covers the fixed fourth endpoint/arguments, one cleanup call with truthful counts, busy no-op, counter mismatch/page overflow/numeric counter/extra money field/unsupported lane/busy contradiction refusal, invalid limit/caller cursor before any call, and lost reply with one-call unknown recovery. Existing background units now assert the exact five-lane sequence and that an earlier failing lane leaves the other four their turn, with existing sends awaited and no queue ACK in the minute tick. The endpoint census and header documentation now correctly say four metadata RPCs. Existing stream/deadline/error/late-response checks remain unchanged. These assertions were read, not executed by this reviewer.

D01 must independently establish safe retained-reference/replay eligibility; the D03 transport does not make a terminal row deletable. D02 health, D04 future joined checklist and D05 history-point gate retain their own reviews. Full Worker/queue/minute/application acceptance remains NOT_RUN by this reviewer, and no new fixture or harness is implied.

## Exact latest reviewed hashes

| File | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-runtime-rpc.js` | `7fd7a0cecb2f27d87f5b78ca7453b1684e9c5ee2b4e956d9e30c1da59262d88b` |
| `broker/src/bpay-next-background.js` | `bf485315e29a6180337ff04ee38d97fa4e9afb4f35dc4c69294f69ccc151cf14` |
| `tests/bpay-next-runtime-rpc.test.js` | `bae1e33e9bafdfcc389c87348d33a92b3848dd783a2a2ce149ba161b9e87e4f5` |
| `tests/bpay-next-background.test.js` | `d0f58e81faab60133e7a6c159e081e4af32f040f4aa587b0b1af53ccd890f212` |
| `broker/src/index.js` | `164abc3f3836abbc95af562493fd25291014f0b6ec335e562b5af006e77f3f22` |

The index was inspected only for the existing minute-entry ownership seam; no unrelated shared index code is approved by this verdict. Hashes bind saved source, not installation or runtime proof.
