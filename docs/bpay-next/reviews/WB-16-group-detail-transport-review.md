# WB-16 GROUP_LINES transport review

Verdict: **SOURCE PASS for the twelve-field GROUP_LINES transport addition.** No open concrete finding in this reviewed slice. This does not approve the newly authored payment-detail renderer, the whole WB-16 item, or any application/data/timing execution.

Reviewed saved source on 10 October 2026:

| Source | SHA-256 |
| --- | --- |
| `broker/src/bpay-next-workbench.js` | `78917cd474bbd585de010d1b515c8c71a56b87857aafc8f4d7820746636368c1` |
| BUILD-FE `js/bpay-next-workbench.js` | `098a8727a8477b3fc25c5927f4d5810b0151e9d880f797fb5eac758ecedb0944` |
| `supabase/repeatable/10102026_2026_banking_pay_next_current_group_details_v1.sql` | `278bd4d3dccc1e7453a69d1d2e0e4bdcde7075e5fe5181c5fe090c4a9dfc7e8a` |

The Worker ROWS declaration (around line 299) and Office mirror (around line 301) agree exactly. The added fields are the native approved-line ordinal, paired native shift-detail UUID/ordinal, retained job/band, proven original Timesheet UUID/reference, nullable adjustment flag, nullable component-known flag, and three nullable exact decimal-text totals. They correspond to the actual twelve projected keys at SQL lines 392–397. The previous fields retain their declarations.

The corrected totals use `decimal?`, an existing validator in both modules (Worker lines 405–413; Office lines 407–415). It accepts NULL or a signed decimal string, rejects JSON numbers, exponent notation and an undefined type discriminator, and retains the response byte bound. Ordinals remain positive bigint text; UUIDs, text and booleans retain their existing typed validators. No numeric coercion or browser amount calculation was introduced in these mirror additions.

The new GROUP_LINES relation checks agree in both modules (Worker lines 497–500; Office lines 499–502). Case rows cannot carry the new Work factual/context fields; shift UUID/ordinal nullability must agree, an actual shift requires an approved-segment identity, and an original reference requires its original Timesheet UUID. Component-known TRUE requires all three totals; FALSE or NULL requires all three totals NULL. This matches the 2026 SQL's shared complete/current/non-dirty amount qualifier, including genuine known zero and unavailable amount evidence. Optional line/context fields permit actual GROUP_EVIDENCE and unavailable native-detail cases rather than manufacturing facts.

The existing exact-key shape, actor/scope/revision context, native group identity, ordered keyset, uniqueness, page-size and page-receipt validation remains in the response path. The transport addition adds no route, kind, command, accepted-input call, monetary write, producer, trigger or protocol. Existing factual preparation mirrors were reviewed separately in `WB-16-detail-transport-review.md`.

Method: static saved-file reads, textual comparison and SHA-256 only. No SQL, database, tests, gates, runtime, fixtures or external calls were invoked by this reviewer. The FE file also contains a concurrently authored renderer; its source is outside this narrow verdict and requires its own review before approval.
