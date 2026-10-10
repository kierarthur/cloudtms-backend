# PS-08 frozen run capture immutability investigation — 10 October 2026

Read-only source investigation/design. No product source changed, no SQL/runtime/test/app/fixture/hosted/network operation executed. Source conclusions do not establish installation/readback or in-app acceptance.

## Conclusion and smallest required work

F-06's carried `supabase/repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql` already contains the exact four guards required by PS-08. Preserve that one physical DDL owner and the unchanged generic immutable callback. There is no current frozen-row UPDATE/DELETE needing a product SQL successor or a CM-03 rewrite.

The missing PS-08-D02 deliverable is a finite source census in the existing `scripts/bpay-next-source-gates.mjs` + ordinary `tests/bpay-next-source-gates.test.js`, using its current all-source recursive closure inventory. Do not create a new programme, fixture runner, database trigger, immutable-row exception or proof framework.

## Binding card and checklist

Full card: normal plan `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1480–1500`; BUILD-BE checklist `docs/bpay-next/IMPLEMENTATION_CHECKLIST.md:797–820`. Sequence 32, dependency F-06 only, no successor chain/no design approval needed.

| Row | Actual deliverable / ownership | Source finding and remaining work |
| --- | --- | --- |
| PS-08-D01 | Existing 0718 four frozen guards | Present and correct source. Checklist's MIGRATION category is an evidence label; saved physical owner is **supabase/repeatable/08102026_0718...sql**, not a newly required migration. Preserve F-06 ownership and exact bytes. |
| PS-08-D02 | Static UPDATE/DELETE census for run_work/run_line | Missing in existing gate. Add exact protected-target rule across every current NEXT repeatable and recursive physical include, including retained predecessors; no changed-file or latest-winner-only waiver. |
| PS-08-D03 | Future joined acceptance checklist | Author explicit legal preparation INSERT, direct UPDATE/DELETE/TRUNCATE rejection, and confirm/cancel/expiry/outcome progress clauses. Real execution remains NOT_RUN. |
| PS-08-D04 | Shared F-06/F-24 release Bible/readback evidence | Context only, not another schema/release task. Exact trigger/index readback belongs to their approved release workflow. File presence does not prove installed state. |

Worker/Screen: none. CM-03 later reconciles RBC behavior against these exact guards; do not implement a depth/whitelist escape or jump that owner now.

## Exact current guards

| Relation | Trigger | Timing/events/level | Callback / exact trigger type |
| --- | --- | --- | --- |
| private.bpay_next_run_work | bpay_next_run_work_frozen_immutable_v1 | BEFORE UPDATE OR DELETE, EACH ROW | private.bpay_next_effect_immutable_v1(), tgtype 27 |
| private.bpay_next_run_work | bpay_next_run_work_frozen_no_truncate_v1 | BEFORE TRUNCATE, EACH STATEMENT | same callback, tgtype 34 |
| private.bpay_next_run_line | bpay_next_run_line_frozen_immutable_v1 | BEFORE UPDATE OR DELETE, EACH ROW | same callback, tgtype 27 |
| private.bpay_next_run_line | bpay_next_run_line_frozen_no_truncate_v1 | BEFORE TRUNCATE, EACH STATEMENT | same callback, tgtype 34 |

0718:13–30 enumerates those exact names/relations/types and refuses a same-name existing attachment when its callback/type/enabled/internal/args/attribute/qualifier/constraint/deferral shape differs, with SQLSTATE 23514 `BPAY_NEXT_FROZEN_CAPTURE_TRIGGER_CONFLICT`. The four subsequent DROP/CREATE pairs install only their exact matching guards; no INSERT event, WHEN, alternate callback, trigger-depth exception, owner/role whitelist or bypass is added. Original generic callback in `03102026_1342_banking_pay_next_guards_v1.sql:188–199` unconditionally raises SQLSTATE **23514 BPAY_NEXT_FINANCIAL_EFFECT_IMMUTABLE** for every tg_op other than INSERT, before RETURN NEW. Both TRUNCATE statement calls therefore raise as required.

0718 also carries the existing full non-unique cancellation-history index preflight/creation; PS-09 owns its verification. Do not duplicate that DDL or modify its pending-worker admission index in PS-08.

## Current static write census

A source-only whole-tree case-insensitive text census under `supabase/repeatable/**/*.sql` found **zero** literal UPDATE/DELETE/MERGE/TRUNCATE target sites for the exact relations run_work/run_line. This is a present-source observation, not execution of the proposed source gate or runtime trigger proof. At capture, physical tree contained 816 SQL sources and 112 with NEXT references; these incidental counts are not proposed acceptance criteria.

Four legal INSERT sites remain:

| Physical saved file | Source line | Role |
| --- | --- | --- |
| 03102026_1600_banking_pay_next_prepare_pin_v1.sql | 441 | Retained historical run_work insertion |
| 03102026_1620_banking_pay_next_fresh_positive_hold_v1.sql | 193 | Retained historical run_line insertion |
| 09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql | 1366 | Current fresh hold capture inserts complete immutable line then creates legal positive hold |
| 10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql | 523 | Current Rule-B pin inserts exact chosen captured revision into run_work |

Rule-B 0151:523–532 inserts complete run_work and its separate retained query-pin row. Its progress updates at 535–542 target **run_worker** and **job**, not frozen run_work. 2018:1366–1380 inserts complete frozen line; the subsequent positive reservation writes **hold**. Preserve these legal INSERTs and all progress owners; do not rewrite their authority or reopen F-25. Zero-value immutable captured lines are legal INSERTs and are not zero-value holds.

## Existing gate integration seams

`assertSqlWriteBoundaries` at source-gates.mjs:630–645 already iterates `executableParts` and enforces finite module/test-marker/provider/admission boundaries. It does **not** currently prohibit UPDATE/DELETE on run_work/run_line. `assertFrozenAuthority` at 907–952 protects frozen authority and admits the four exact additive 0718 attachments at 925–931; this trigger-DDL rule is not a frozen data-write census.

`verifyBpayNextSourceGates` at 987 builds `sources` from **all** `inventory()` migrations/repeatables and each repeatable's `paths`, then unconditionally calls `assertSqlWriteBoundaries` at 1025–1026. Only special-construct/competing-DDL checks are changed-file gated. Place the new exact two-target DML check in the unconditional write-boundary path so old unchanged definitions and included files remain covered. Never put this rule only inside changed.has(file) or only over the current final definer.

`cloudtms-db-release-lib.mjs:466–492` recursively resolves each repeatable's \ir includes, rejects escapes/cycles/missing children, and returns every physical `paths` member. Its `inventory()` at 495–498 uses those recursive closures. Thus the existing source Set already scans included helpers without a new crawler. F-23 units are wired into normal npm test and protected source CI (`.github/workflows/database-release.yml`, `supabase-migrate.yml`). Extend these existing units/wiring, rather than add another runner.

## Finite rule and ordinary source units

The smallest rule can reuse `executableParts(source,file)` and `scanManagedSql(part.text).statements`:

1. Normalize comments/simple quoted identifiers with the existing `sqlWithoutComments` handling used by executableParts; tokenization then masks diagnostic string literals as `<QUOTED>` and dollar metadata as `<BODY>`.
2. Match actual token sequences **UPDATE [ONLY] [private .] BPAY_NEXT_RUN_WORK/BPAY_NEXT_RUN_LINE** and **DELETE FROM [ONLY] [private .] exact relation** anywhere in actual routine/DO executable statements. Handle whitespace/newlines and quoted identifiers. Exact relation equality prevents matching run_worker, run_work_query_snapshot, run_work_query_pin_v2, run_line-derived helpers or harmless variable names.
3. Refuse with a finite source error naming the physical file/target. No baseline-body waiver: the card says no NEXT repeatable may issue these writes.
4. INSERTs and updates to run_worker/hold/request/outcome/job remain permitted. SELECT FOR UPDATE is a lock clause with no protected-target UPDATE statement and remains permitted; trigger `BEFORE UPDATE OR DELETE ON ...` is DDL, not a data writer.

A similarly finite MERGE INTO/TRUNCATE target check is compatible with the four guards if desired, but the explicit new source-census requirement is UPDATE/DELETE. No generic callgraph or dynamic-SQL interpreter is needed. Existing special-construct rules reject unclassified executable dynamic SQL; any new dynamic mutation of these protected targets must fail those current rules, not gain a source-census exception. Do not reject a quoted COMMENT/diagnostic sentence merely containing forbidden text. If supporting literal executed SQL, apply the exact target check only to an actual EXECUTE literal at the existing classified seam, never to arbitrary catalogue metadata.

Add ordinary units in the existing test file covering direct routine/DO UPDATE and DELETE for both exact targets, ONLY, newlines, simple quoted schema/table identifiers, and a child physical include being reached by the existing all-source inventory path. Positive cases: comment/diagnostic string text, the exact0718 trigger DDL, SELECT FOR UPDATE, complete INSERTs, updates to run_worker/job/hold/outcome, and similarly prefixed retained query-pin/snapshot table identities. These source units validate the required rule; no database fixture or new app scenario harness is warranted.

## Future connected acceptance

After owner-authorised installation, the joined checklist/readback must cover all four exact active attachments and callback shape, INSERT unchanged, direct UPDATE/DELETE/TRUNCATE each rejected, and real preparation/confirm/cancel/expiry/outcome progress completing only on their mutable workflow facts. Installation and app boxes remain NOT_RUN in this investigation. Preserve one F-06/F-24 release/readback owner and existing immutable baseline. Do not infer installation from current0718 file presence or replace the real trigger readback with source counts/hashes.

## Fresh raw source hashes

| BUILD-BE relative path | SHA256 |
| --- | --- |
| `supabase/repeatable/08102026_0718_banking_pay_next_frozen_run_read_safety_v1.sql` | `0a884c8cb2568ca568ac51069fcf8fac945159a7f3f4d688d57b1731c295feae` |
| `supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql` | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| `scripts/bpay-next-source-gates.mjs` | `65a6ff58a8919cc443518da1a4bb24c895b3a41a5245b988f581333696b4afb8` |
| `tests/bpay-next-source-gates.test.js` | `3a8608c6d714eb28489c5b165b36f3f559bc6da2cd4bb52b63948d0151ec069b` |
| `scripts/cloudtms-managed-release-sql.mjs` | `ff391fccf7f690d4d476e935870d740667e4dfd92b972bfd5d7d80b1b3bb912b` |
| `scripts/cloudtms-db-release-lib.mjs` | `a5c79ae9d95b5e835e0c84fd876cad45bd8f5b31f8c1bafa95c76d6578afb4bf` |
| `supabase/repeatable/10102026_0151_banking_pay_next_rule_b_selection_consumers_v1.sql` | `2698331479177a246d2ee581f71aa1514a1479f05bf6258c1ceb1f4f490104cf` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
