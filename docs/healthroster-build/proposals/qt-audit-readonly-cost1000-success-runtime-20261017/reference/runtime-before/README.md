# Fixed QT read-only 1,000-row cost diagnosis

This source-only proposal provides five closed modes, one explicitly selected mode per original short shared turn. It does not change QT assertions or propose another full-table serializer trial. There is no built-in 10,000-row mode, automatic next mode, retry, fixture, temporary table, DO block, DDL or business write.

| Mode | Per-row expression actually consumed |
|---|---|
| SCAN_PROJECT | `pg_column_size(whole named audit row)` |
| JSON_TEXT | UTF-8 byte length of `to_jsonb(whole row)::text` |
| JSON_MD5 | Byte length of the original full-row JSON text MD5 |
| RECORD_BINARY | Byte length of `record_send(whole named audit row)` |
| RECORD_SHA256 | Byte length of SHA256 over that binary record |

Each input has four statements: SET LOCAL read-only, START clock/admission, one consumed numeric aggregate, END clock/admission. Fresh read-only verification returns one FRESH identity/admission frame without repeating the sample. The aggregate uses a MATERIALIZED numeric CTE over the literal `LIMIT 1000`. Count of sampled rows must equal count of evaluated expressions; sums force consumption and no sampled row, identifier, digest, plan or PII is returned. Zero/fewer-than-1,000 rows are honest small samples. LIMIT without ORDER is intentionally nonrepresentative; independent modes can observe different rows/snapshots and cache state. Clock span includes query-boundary overhead, not just CPU.

The copied c1e91 metadata binds all sixteen current audit column identities/order/types and six built-in types. Before any audit sample, fresh catalogue admission requires the same exact descriptor, ordinary heap/owner/RLS/literal policy, no rules/inheritance, six built-in type send/output routines and five named internal top routines. Exact symbol/language/owner/namespace/header conditions refuse unexpected bindings. The existing shared-turn owner separately proves TEST/LEGACY1/no release; each frame additionally proves those identities, read-only/no role switch and actual held A. A failed catalogue/admission condition causes a native SQL error before the sample and continue_on_error is false.

The original approved catalogue-only holder is a complete raw parent. Its original own-A serialized connection/close implementation, shared turn, protected release check, Controls ABORT/EOF/order/stdin pause and 120/60/180-second budgets (120,000/60,000/180,000ms) are retained. The complete holder inverse has eighteen literal changes; secure-owned has one fixed output-prefix change. No browser or auth child is involved. Report source descendants are admitted only with every selected source byte still exact in saved/index/current HEAD/historical Git images and original full holder checks; ancestry alone admits nothing. Each invocation gets new image reads and end-HEAD guards.

Native frames must match the prior actual direct-Kier header shape, the exact rebuilt input/receipt, fresh connection, actual database/OID/held PID, both main clocks and fresh clock <=30 seconds/+1 second future/order, four exact statements, no truncation, canonical counters and bounded consumption. No strict independent-C PID/restoration verdict is claimed by this read-only diagnostic. Fixed warnings/header names come from a safe header-only extraction of the committed R6 HOLD; that historical timeout is preserved and supplies no successful diagnostic/performance evidence.

Original 30-second statement and 2-second lock timeout remain. Sampling limits row count, not individual field size, TOAST work, per-value allocation or total memory; a very large row can still HOLD. No resource setting, provider, hosted cap, collector, job or lock policy changes. Neither mode timings nor byte totals prove a cause, whole-table runtime, parallel execution, serializer equality or QT acceptance. Binary JSONB still formats JSON text. A different binary/SHA assertion would require its own reviewed equivalence argument; this packet makes none.

Safe source replay from this ignored packet: `node --test test.mjs root-tools.test.mjs`, `python pg17-proof.py`, `node verify.mjs`. These tests use synthetic values/memory tool doubles only. Do not run prepare-holder, launch, hold, source admission or root-io during peer review. No actual operation has been run.

## Root invocation after source review, commit and fresh push verification

Root installs only this complete new proposal at I2 `docs/healthroster-build/proposals/qt-audit-readonly-cost1000-20261017`, commits/pushes its exact images, and records that actual source commit. No existing runtime target is overwritten. Source gate requires the real committed image in saved/index/current HEAD/history and actual physical D roots/Git common directories. Root separately confirms the account-specific direct Kier route and healthy TEST identities using its existing approved route; the diagnostic never substitutes another account/provider.

Start one reviewed holder in the original owned PTY with the selected literal mode and actual reviewed commit:

```powershell
& 'C:/Program Files/nodejs/node.exe' 'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026/docs/healthroster-build/proposals/qt-audit-readonly-cost1000-20261017/launch.mjs' --mode SCAN_PROJECT --source-commit ACTUAL_REVIEWED_40_HEX_COMMIT
```

Keep that terminal owned and open. Await its actual newline-terminated COST_INPUT_READY record; an elapsed time, closed phase file or example vector is not readiness. The public record includes the holder's actual start clock so root orchestration cannot restart its budget from a delayed READY. Root may use its already reviewed native-cmd stdout redirection pattern; stdout contains only closed records. Never dispatch a synthetic fixture from tests.

`root-tools.mjs` is a self-contained functions-tools template without imports or Node dependencies. Root reads that exact reviewed committed text through its fixed public file tool, removes line-leading `export ` tokens and evaluates it to obtain `runReadyCost`. Supply the actual READY, same mode, actual reviewed commit and that original tool session ID. It reads/reconstructs only the fixed INPUT/RECEIPT through root-io; invokes only `mcp__cloudtms_miget_operations_kier_codex__miget_db_transaction_rollback` once with the exact generated input; filters closed scalar frames before passing data to root-io; validates and wx saves NATIVE_RESULT; sends RESULT_READY; awaits actual COST_CAPTURED_PENDING_RELEASE; sends original RELEASE; and requires normal own-terminal exit0 plus original post-unlock/close SUMMARY. No arbitrary SQL/path/provider argument or new transport is present.

On HOLD it sends ABORT only to that supplied owned session and waits within the original cleanup/whole bounds. Lost/unconfirmed tool exit, native failure, unexpected wrapper/frame or truncation never becomes success; root retains its actual tool receipt and original explicit A release evidence separately. There is no forced kill, cancellation of another session, copied canonical restoration verdict or successful QT claim. Native diagnostic ABI, runtime, source admission and timings remain NOT_RUN until root's separately reviewed actual call.
